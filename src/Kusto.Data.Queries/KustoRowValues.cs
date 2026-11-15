using System.Buffers.Text;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace Kusto.Data.Queries;

internal static class KustoRowValues
{
    public static object?[] ToValues(ReadOnlySpan<byte> row, ReadOnlySpan<KustoColumn> columns)
    {
        var values = new object?[columns.Length];
        ToValues(row, columns, values);
        return values;
    }

    public static void ToValues(ReadOnlySpan<byte> row, ReadOnlySpan<KustoColumn> columns, Span<object?> destination)
    {
        if (destination.Length < columns.Length)
        {
            throw new ArgumentException($"Destination has {destination.Length} elements but the table has {columns.Length} columns.", nameof(destination));
        }

        var reader = new Utf8JsonReader(row);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartArray)
        {
            throw new JsonException("Row is not a JSON array.");
        }

        for (var i = 0; i < columns.Length; i++)
        {
            if (!reader.Read() || reader.TokenType == JsonTokenType.EndArray)
            {
                throw TooFewValues(i, columns.Length);
            }

            try
            {
                destination[i] = ReadValue(ref reader, columns[i]);
            }
            catch (InvalidOperationException e)
            {
                throw InvalidText(columns[i], e);
            }
        }

        if (!reader.Read() || reader.TokenType != JsonTokenType.EndArray)
        {
            throw new JsonException($"Row has more values than the table's {columns.Length} columns.");
        }
    }

    public static JsonException TooFewValues(int count, int columns) => new($"Row has {count} values but the table has {columns} columns.");

    public static T? ReadValue<T>(ref Utf8JsonReader reader, KustoColumn column, T? defaultValue) where T : struct
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            return defaultValue;
        }

        try
        {
            return ReadNonNullValue(ref reader, column) is T value ? value : throw WrongType<T>(column);
        }
        catch (InvalidOperationException e)
        {
            throw InvalidText(column, e);
        }
    }

    public static T? ReadValue<T>(ref Utf8JsonReader reader, KustoColumn column, T? defaultValue) where T : class
    {
        try
        {
            object? value = ReadValue(ref reader, column);
            return value is null ? defaultValue : value as T ?? throw WrongType<T>(column);
        }
        catch (InvalidOperationException e)
        {
            throw InvalidText(column, e);
        }
    }

    // Rows are not validated as they are read, so this is where the reader finds invalid UTF-8 or a lone surrogate escape.
    private static JsonException InvalidText(KustoColumn column, InvalidOperationException e) =>
        new($"Column '{column.Name}' ({column.Type}) has a string value that is not valid text.", e);

    private static InvalidCastException WrongType<T>(KustoColumn column) =>
        new($"Column '{column.Name}' ({column.Type}) cannot be read as {typeof(T)}.");

    private static object? ReadValue(ref Utf8JsonReader reader, KustoColumn column) =>
        reader.TokenType == JsonTokenType.Null ? null : ReadNonNullValue(ref reader, column);

    // Inlined so that a typed read unboxes in the method that boxed, which lets the JIT keep the box off the heap.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static object ReadNonNullValue(ref Utf8JsonReader reader, KustoColumn column)
    {
        var token = reader.TokenType;
        return column.Type switch
        {
            KustoColumnType.Bool when token is JsonTokenType.True or JsonTokenType.False => token == JsonTokenType.True,
            KustoColumnType.Int when token == JsonTokenType.Number && reader.TryGetInt32(out var i) => i,
            KustoColumnType.Long when token == JsonTokenType.Number && reader.TryGetInt64(out var l) => l,
            KustoColumnType.Real when token == JsonTokenType.Number && reader.TryGetDouble(out var d) && double.IsFinite(d) => d,
            // The service writes the non-finite values, which JSON numbers cannot express, as strings.
            KustoColumnType.Real when token == JsonTokenType.String && TryParse(ref reader, TryParseNonFinite, out var special) => special,
            KustoColumnType.Decimal when token == JsonTokenType.Number && reader.TryGetDecimal(out var m) => m,
            KustoColumnType.Decimal when token == JsonTokenType.String && TryParse(ref reader, TryParseDecimal, out var dec) => dec,
            KustoColumnType.DateTime when token == JsonTokenType.String && reader.TryGetDateTime(out var dt) => dt.Kind switch
            {
                DateTimeKind.Utc => dt,
                DateTimeKind.Local => dt.ToUniversalTime(),
                _ => DateTime.SpecifyKind(dt, DateTimeKind.Utc),
            },
            KustoColumnType.TimeSpan when token == JsonTokenType.String && TryParse(ref reader, TryParseTimeSpan, out var ts) => ts,
            KustoColumnType.Guid when token == JsonTokenType.String && reader.TryGetGuid(out var g) => g,
            KustoColumnType.String when token == JsonTokenType.String => reader.GetString()!,
            KustoColumnType.Dynamic => JsonElement.ParseValue(ref reader),
            _ => throw new JsonException($"Column '{column.Name}' ({column.Type}) cannot hold a JSON {token} value or the value is invalid or out of range."),
        };
    }

    private delegate bool Utf8TryParse<T>(ReadOnlySpan<byte> text, out T value);

    // Only the service's exact spellings: double.TryParse would also turn overflowing numbers into infinities.
    private static readonly Utf8TryParse<double> TryParseNonFinite = static (text, out value) =>
    {
        if (text.SequenceEqual("NaN"u8))
        {
            value = double.NaN;
        }
        else if (text.SequenceEqual("Infinity"u8))
        {
            value = double.PositiveInfinity;
        }
        else if (text.SequenceEqual("-Infinity"u8))
        {
            value = double.NegativeInfinity;
        }
        else
        {
            value = default;
            return false;
        }

        return true;
    };

    private static readonly Utf8TryParse<decimal> TryParseDecimal = static (text, out value) =>
        decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value);

    private static readonly Utf8TryParse<TimeSpan> TryParseTimeSpan = static (text, out value) =>
        Utf8Parser.TryParse(text, out value, out int consumed, 'c') && consumed == text.Length;

    // The string may be JSON-escaped, so it is parsed from an unescaped copy; no valid value is long, but the
    // buffer is compared to the escaped length, which a fully escaped value (6 bytes per character) can reach.
    private static bool TryParse<T>(ref Utf8JsonReader reader, Utf8TryParse<T> parser, out T value)
    {
        Span<byte> buffer = stackalloc byte[192];
        value = default!;
        return reader.ValueSpan.Length <= buffer.Length && parser(buffer[..reader.CopyString(buffer)], out value);
    }
}

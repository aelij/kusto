using System.Buffers;
using System.Text.Json;

namespace Kusto.Data.Queries;

/// <summary>
/// A row of a <see cref="KustoTable"/>. The row is a view over the reader's buffer: it is valid only until the
/// row enumerator advances or is disposed, after which every member throws <see cref="ObjectDisposedException"/>.
/// </summary>
public readonly struct KustoRow
{
    private readonly KustoTable? _table;
    private readonly long _version;

    internal KustoRow(KustoTable table, long version)
    {
        _table = table;
        _version = version;
    }

    /// <summary>The table the row belongs to.</summary>
    /// <exception cref="ObjectDisposedException">The row is <see langword="default"/>.</exception>
    public KustoTable Table => _table ?? throw new ObjectDisposedException(nameof(KustoRow));

    /// <summary>Invokes <paramref name="action"/> with the row's UTF-8 JSON array; the span must not be used after it returns.</summary>
    /// <remarks>
    /// The array is well-formed JSON, but its strings are not validated: they may hold invalid UTF-8 or escape lone
    /// surrogates. <see cref="ToValues"/> rejects such strings with a <see cref="JsonException"/>.
    /// </remarks>
    public void UseData<TState>(ReadOnlySpanAction<byte, TState> action, TState state)
    {
        ArgumentNullException.ThrowIfNull(action);
        UseData(static (data, args) => { args.action(data, args.state); return 0; }, (action, state));
    }

    /// <summary>Invokes <paramref name="func"/> with the row's UTF-8 JSON array; the span must not be used after it returns.</summary>
    /// <remarks>
    /// The array is well-formed JSON, but its strings are not validated: they may hold invalid UTF-8 or escape lone
    /// surrogates. <see cref="ToValues"/> rejects such strings with a <see cref="JsonException"/>.
    /// </remarks>
    public TResult UseData<TState, TResult>(Func<ReadOnlySpan<byte>, TState, TResult> func, TState state)
    {
        ArgumentNullException.ThrowIfNull(func);
        KustoTableReader reader = Table.Reader;
        ReadOnlySpan<byte> data = reader.EnterRow(_version);
        try
        {
            return func(data, state);
        }
        finally
        {
            reader.Exit();
        }
    }

    /// <summary>Converts the row's values to .NET objects according to the table's column types.</summary>
    public object?[] ToValues() => UseData(static (data, table) => KustoRowValues.ToValues(data, table.ColumnSpan), Table);

    /// <summary>
    /// Converts the value of the column at <paramref name="columnIndex"/> to <typeparamref name="T"/>, which must be
    /// the column type's .NET type (see <see cref="KustoColumnType"/>); a null value gives <paramref name="defaultValue"/>.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="columnIndex"/> is not a column of the row's table.</exception>
    /// <exception cref="InvalidCastException"><typeparamref name="T"/> does not match the column's type.</exception>
    public T? GetValue<T>(int columnIndex, T? defaultValue = default) where T : struct => UseData(
        static (data, args) =>
        {
            Utf8JsonReader reader = args.row.ReadToken(data, args.columnIndex, out KustoColumn column);
            return KustoRowValues.ReadValue(ref reader, column, args.defaultValue);
        },
        (row: this, columnIndex, defaultValue));

    /// <inheritdoc cref="GetValue{T}(int, T?)"/>
    public T? GetValue<T>(int columnIndex, T? defaultValue = default) where T : class => UseData(
        static (data, args) =>
        {
            Utf8JsonReader reader = args.row.ReadToken(data, args.columnIndex, out KustoColumn column);
            return KustoRowValues.ReadValue(ref reader, column, args.defaultValue);
        },
        (row: this, columnIndex, defaultValue));

    /// <summary>Converts the value of the column named <paramref name="columnName"/>; see <see cref="GetValue{T}(int, T?)"/>.</summary>
    /// <remarks>The column is looked up on every call; to read many rows, get its index once from <see cref="KustoTable.GetColumnIndex"/>.</remarks>
    /// <exception cref="ArgumentException">The row's table has no column named <paramref name="columnName"/>.</exception>
    public T? GetValue<T>(string columnName, T? defaultValue = default) where T : struct => UseData(
        static (data, args) =>
        {
            Utf8JsonReader reader = args.row.ReadToken(data, args.row.Table.GetColumnIndex(args.columnName), out KustoColumn column);
            return KustoRowValues.ReadValue(ref reader, column, args.defaultValue);
        },
        (row: this, columnName, defaultValue));

    /// <inheritdoc cref="GetValue{T}(string, T?)"/>
    public T? GetValue<T>(string columnName, T? defaultValue = default) where T : class => UseData(
        static (data, args) =>
        {
            Utf8JsonReader reader = args.row.ReadToken(data, args.row.Table.GetColumnIndex(args.columnName), out KustoColumn column);
            return KustoRowValues.ReadValue(ref reader, column, args.defaultValue);
        },
        (row: this, columnName, defaultValue));

    // Called with the row's data in use, so that an expired row is reported before a column that does not exist.
    private Utf8JsonReader ReadToken(ReadOnlySpan<byte> data, int columnIndex, out KustoColumn column)
    {
        KustoTable table = Table;
        ReadOnlySpan<KustoColumn> columns = table.ColumnSpan;
        ArgumentOutOfRangeException.ThrowIfNegative(columnIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(columnIndex, columns.Length);
        column = columns[columnIndex];
        Utf8JsonReader reader = new(data[table.Reader.GetValueOffset(columnIndex)..]);
        reader.Read();
        return reader;
    }

    /// <summary>Copies the row into a standalone <see cref="JsonDocument"/> whose root is the row's array.</summary>
    public JsonDocument ToDocument() => UseData(
        static (data, _) =>
        {
            Utf8JsonReader reader = new(data);
            return JsonDocument.ParseValue(ref reader);
        },
        0);
}

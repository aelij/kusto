using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Azure.Core;

namespace Kusto.Data.Queries;

/// <summary>The JSON body of the REST API requests.</summary>
internal sealed record KustoRequestBody(
    [property: JsonPropertyName("db")] string? Database,
    [property: JsonPropertyName("csl")] string Csl,
    [property: JsonPropertyName("properties")] KustoRequestBody.RequestProperties? Properties)
{
    private const int InitialBufferSize = 1024;

    /// <summary>Serializes the body into a pooled buffer, which the content returns to the pool when disposed.</summary>
    public static RequestContent Create(string? database, string csl, KustoRequestProperties? properties)
    {
        KustoRequestBody body = new(database, csl, properties is null ? null : new(properties, properties.ParametersOrNull is { Count: > 0 } p ? p : null));
        PooledByteBufferWriter buffer = new(InitialBufferSize);
        try
        {
            using (Utf8JsonWriter writer = new(buffer))
            {
                JsonSerializer.Serialize(writer, body, KustoJsonContext.Default.KustoRequestBody);
            }

            return new PooledContent(buffer);
        }
        catch
        {
            buffer.Dispose();
            throw;
        }
    }

    public static string CancelQueryCommand(string clientRequestId, string reason) =>
        $".cancel query {StringLiteral(clientRequestId)} with (reason={StringLiteral(reason)})";

    /// <summary>Rejects the parameter values that <see cref="Create"/> cannot serialize, without serializing the body.</summary>
    public static void ValidateParameters(KustoRequestProperties properties)
    {
        if (properties.ParametersOrNull is not { } parameters)
        {
            return;
        }

        foreach ((string name, object? value) in parameters)
        {
            // The types that WriteParameter and ToLiteral support.
            if (value is not (string or long or bool or int or double or decimal or DateTime or DateTimeOffset or TimeSpan or Guid or JsonElement or JsonNode))
            {
                throw UnsupportedParameter(name, value);
            }
        }
    }

    private sealed class PooledContent(PooledByteBufferWriter buffer) : RequestContent
    {
        public override bool TryComputeLength(out long length)
        {
            length = buffer.WrittenMemory.Length;
            return true;
        }

        public override void WriteTo(Stream stream, CancellationToken cancellation) => stream.Write(buffer.WrittenMemory.Span);

        public override async Task WriteToAsync(Stream stream, CancellationToken cancellation) =>
            await stream.WriteAsync(buffer.WrittenMemory, cancellation).ConfigureAwait(false);

        public override void Dispose() => buffer.Dispose();
    }

    internal sealed record RequestProperties(
        [property: JsonPropertyName("Options")] KustoRequestProperties Options,
        [property: JsonPropertyName("Parameters"), JsonConverter(typeof(ParametersConverter))] IReadOnlyDictionary<string, object>? Parameters);

    internal sealed class ParametersConverter : JsonConverter<IReadOnlyDictionary<string, object>>
    {
        public override IReadOnlyDictionary<string, object> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            throw new NotSupportedException();

        public override void Write(Utf8JsonWriter writer, IReadOnlyDictionary<string, object> value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            foreach ((string name, object parameter) in value)
            {
                WriteParameter(writer, name, parameter);
            }

            writer.WriteEndObject();
        }
    }

    // The service takes strings and longs as JSON values, and every other type as a KQL literal.
    private static void WriteParameter(Utf8JsonWriter writer, string name, object value)
    {
        switch (value)
        {
            case string s:
                writer.WriteString(name, s);
                break;

            case long l:
                writer.WriteNumber(name, l);
                break;

            default:
                writer.WriteString(name, ToLiteral(name, value));
                break;
        }
    }

    private static string ToLiteral(string name, object? value) => value switch
    {
        bool b => b ? "true" : "false",
        int i => Invariant($"int({i})"),
        double d => Invariant($"real({RealValue(d)})"),
        decimal m => Invariant($"decimal({m})"),
        DateTime dt => Invariant($"datetime({ToUtc(dt):yyyy-MM-ddTHH:mm:ss.fffffffZ})"),
        DateTimeOffset dto => Invariant($"datetime({dto.UtcDateTime:yyyy-MM-ddTHH:mm:ss.fffffffZ})"),
        TimeSpan ts => Invariant($"time({ts:c})"),
        Guid g => Invariant($"guid({g:D})"),
        JsonElement e => $"dynamic({e.GetRawText()})",
        JsonNode n => $"dynamic({n.ToJsonString()})",
        _ => throw UnsupportedParameter(name, value),
    };

    private static ArgumentException UnsupportedParameter(string name, object? value) => value is null
        ? new($"The value of the query parameter '{name}' is null. Omit the parameter to use its declared default.")
        : new($"The value of the query parameter '{name}' has the unsupported type '{value.GetType()}'.");

    private static string RealValue(double d) => d switch
    {
        double.PositiveInfinity => "+inf",
        double.NegativeInfinity => "-inf",
        _ when double.IsNaN(d) => "nan",
        _ => d.ToString("R", CultureInfo.InvariantCulture),
    };

    private static DateTime ToUtc(DateTime value) => value.Kind switch
    {
        DateTimeKind.Local => value.ToUniversalTime(),
        DateTimeKind.Unspecified => DateTime.SpecifyKind(value, DateTimeKind.Utc),
        _ => value,
    };

    private static string Invariant(FormattableString value) => value.ToString(CultureInfo.InvariantCulture);

    private static string StringLiteral(string value)
    {
        StringBuilder builder = new(value.Length + 2);
        builder.Append('"');
        foreach (char c in value)
        {
            _ = c switch
            {
                '"' => builder.Append("\\\""),
                '\\' => builder.Append("\\\\"),
                '\n' => builder.Append("\\n"),
                '\r' => builder.Append("\\r"),
                '\t' => builder.Append("\\t"),
                _ => builder.Append(c),
            };
        }

        return builder.Append('"').ToString();
    }
}

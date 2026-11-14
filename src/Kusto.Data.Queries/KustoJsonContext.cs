using System.Text.Json.Serialization;

namespace Kusto.Data.Queries;

[JsonSourceGenerationOptions(
    RespectNullableAnnotations = true,
    RespectRequiredConstructorParameters = true,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(KustoColumn[]))]
[JsonSerializable(typeof(ErrorPayload[]))]
[JsonSerializable(typeof(KustoRequestBody))]
internal sealed partial class KustoJsonContext : JsonSerializerContext;

/// <summary>
/// The shapes in which the service reports errors: <c>{"error": {...}}</c> as a whole response or as an
/// element of <c>OneApiErrors</c>, and <c>{"OneApiErrors": [...]}</c> in place of a row.
/// </summary>
internal sealed class ErrorPayload
{
    [JsonPropertyName("error")]
    public KustoError? Error { get; init; }

    [JsonPropertyName("OneApiErrors")]
    public ErrorPayload?[]? OneApiErrors { get; init; }

    public static List<KustoError> Flatten(ReadOnlySpan<ErrorPayload?> payloads)
    {
        List<KustoError> errors = [];
        foreach (ErrorPayload? payload in payloads)
        {
            if (payload?.Error is { } error)
            {
                errors.Add(error);
            }

            errors.AddRange(Flatten(payload?.OneApiErrors));
        }

        return errors;
    }
}

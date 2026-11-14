using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kusto.Data.Queries;

/// <summary>An error reported by the Kusto service (the OneApi error contract).</summary>
public sealed class KustoError
{
    /// <summary>The code of the error, such as <c>General_BadRequest</c>.</summary>
    [JsonPropertyName("code")]
    public string? Code { get; init; }

    /// <summary>The general message of the error; <see cref="Description"/> is usually more specific.</summary>
    [JsonPropertyName("message")]
    public string? Message { get; init; }

    /// <summary>The service-side type of the error, such as <c>Kusto.Data.Exceptions.KustoBadRequestException</c>.</summary>
    [JsonPropertyName("@type")]
    public string? Type { get; init; }

    /// <summary>The detailed message, which is usually more specific than <see cref="Message"/>.</summary>
    [JsonPropertyName("@message")]
    public string? Description { get; init; }

    /// <summary>Whether retrying the request is expected to fail again.</summary>
    [JsonPropertyName("@permanent")]
    public bool IsPermanent { get; init; }

    /// <summary>Identifies the request the error was raised for.</summary>
    [JsonPropertyName("@context")]
    public KustoErrorContext? Context { get; init; }

    /// <summary>The error that caused this one, if any.</summary>
    [JsonPropertyName("innererror")]
    public KustoError? InnerError { get; init; }

    /// <summary>The members of the error that have no dedicated property.</summary>
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? AdditionalProperties { get; init; }

    /// <summary>Returns the most specific message the error has.</summary>
    public override string ToString() => Description ?? Message ?? Code ?? "The Kusto service reported an error.";
}

/// <summary>Identifies where and when a <see cref="KustoError"/> was raised.</summary>
public sealed class KustoErrorContext
{
    /// <summary>The client request id of the request (<see cref="KustoRequestProperties.ClientRequestId"/>).</summary>
    [JsonPropertyName("clientRequestId")]
    public string? ClientRequestId { get; init; }

    /// <summary>The service-side id of the request's activity, for service diagnostics.</summary>
    [JsonPropertyName("activityId")]
    public string? ActivityId { get; init; }

    /// <summary>The members of the context that have no dedicated property.</summary>
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? AdditionalProperties { get; init; }
}

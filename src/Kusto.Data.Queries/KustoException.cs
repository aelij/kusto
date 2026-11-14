using Azure;

namespace Kusto.Data.Queries;

/// <summary>Thrown when the Kusto service reports that a request failed, fully or partially.</summary>
/// <remarks>
/// <see cref="RequestFailedException.Status"/> is the HTTP status when the service rejected the request, and 0 when
/// the error was reported within the results. <see cref="RequestFailedException.ErrorCode"/> is the code of the first error.
/// </remarks>
public sealed class KustoException : RequestFailedException
{
    internal KustoException(IReadOnlyList<KustoError> errors, string? fallbackMessage = null)
        : this(0, errors, fallbackMessage)
    {
    }

    internal KustoException(int status, IReadOnlyList<KustoError> errors, string? fallbackMessage = null)
        : base(
            status,
            errors.Count > 0 ? errors[0].ToString() : fallbackMessage ?? "The Kusto service reported an error.",
            errors.Count > 0 ? errors[0].Code : null,
            innerException: null)
        => Errors = errors;

    /// <summary>The errors reported by the service; empty when it reported a failure without details.</summary>
    public IReadOnlyList<KustoError> Errors { get; }
}

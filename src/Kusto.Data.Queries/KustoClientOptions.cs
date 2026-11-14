using Azure.Core;

namespace Kusto.Data.Queries;

/// <summary>Options for <see cref="KustoClient"/>.</summary>
public sealed class KustoClientOptions : ClientOptions
{
    /// <summary>Creates options with the defaults of <see cref="ClientOptions"/>, also logging the Kusto diagnostic headers.</summary>
    public KustoClientOptions()
    {
        Diagnostics.LoggedHeaderNames.Add("x-ms-activity-id");
        Diagnostics.LoggedHeaderNames.Add("x-ms-app");
        Diagnostics.LoggedHeaderNames.Add("x-ms-client-version");
    }

    /// <summary>
    /// The audience of the access tokens, such as <c>https://kusto.kusto.windows.net</c>. Defaults to the scheme
    /// and host of the endpoint.
    /// </summary>
    public Uri? Audience { get; set; }

    /// <summary>
    /// The application name the service reports the requests under (the <c>x-ms-app</c> header). Defaults to
    /// <see cref="DiagnosticsOptions.ApplicationId"/>.
    /// </summary>
    public string? Application { get; set; }

    /// <summary>The user name the service reports the requests under (the <c>x-ms-user-id</c> header).</summary>
    public string? User { get; set; }
}

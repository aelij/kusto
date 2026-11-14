using Azure.Core.Extensions;
using Kusto.Data.Queries;

namespace Microsoft.Extensions.Azure;

/// <summary>Extensions for registering <see cref="KustoClient"/> with an Azure client factory.</summary>
public static class KustoClientBuilderExtensions
{
    /// <summary>Registers a <see cref="KustoClient"/> for the cluster at <paramref name="endpoint"/>.</summary>
    /// <typeparam name="TBuilder">The Azure client factory builder type.</typeparam>
    /// <param name="builder">The builder to register the client with.</param>
    /// <param name="endpoint">The cluster endpoint, such as <c>https://help.kusto.windows.net</c>.</param>
    /// <returns>A builder for configuring the client options, credential and registration name.</returns>
    public static IAzureClientBuilder<KustoClient, KustoClientOptions> AddKustoClient<TBuilder>(
        this TBuilder builder,
        Uri endpoint)
        where TBuilder : IAzureClientFactoryBuilderWithCredential
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(endpoint);
        return builder.RegisterClientFactory<KustoClient, KustoClientOptions>(
            (options, credential) => new KustoClient(endpoint, credential, options));
    }
}

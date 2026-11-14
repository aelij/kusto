using System.Diagnostics;

namespace Kusto.Data.Queries;

/// <summary>The names the client traces under.</summary>
internal static class KustoDiagnostics
{
    /// <summary>The name of the <see cref="System.Diagnostics.ActivitySource"/> of the client's activities.</summary>
    public const string ActivitySourceName = "Kusto.Data.Queries";

    internal const string QueryActivityName = "KustoClient.Query";

    internal const string CommandActivityName = "KustoClient.ExecuteCommand";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName, typeof(KustoDiagnostics).Assembly.GetName().Version?.ToString());

    internal static class Tags
    {
        public const string AzureNamespace = "az.namespace";
        public const string ClientRequestId = "az.client_request_id";
        public const string ServiceRequestId = "az.service_request_id";
        public const string DatabaseNamespace = "db.namespace";
        public const string DatabaseOperationName = "db.operation.name";
        public const string ServerAddress = "server.address";
        public const string ServerPort = "server.port";
        public const string ErrorType = "error.type";
    }
}

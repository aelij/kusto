using System.Diagnostics;
using System.IO.Compression;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Azure;
using Azure.Core;
using Azure.Core.Pipeline;

namespace Kusto.Data.Queries;

/// <summary>A client of the Kusto query REST API (v2, non-progressive).</summary>
public class KustoClient
{
    private const string QueryPath = "/v2/rest/query";
    private const string ManagementPath = "/v1/rest/mgmt";

    private static readonly TimeSpan DefaultServerTimeout = TimeSpan.FromMinutes(4);
    private static readonly TimeSpan DefaultCommandServerTimeout = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan MaxServerTimeout = TimeSpan.FromHours(1);

    // The client gives the service this long past its own timeout to report it.
    private static readonly TimeSpan ClientTimeoutGrace = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan CancelTimeout = TimeSpan.FromSeconds(30);

    private static readonly string ClientVersion = "Kusto.Data.Queries:" +
        typeof(KustoClient).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;

    private readonly HttpPipeline _pipeline;
    private readonly bool _tracingEnabled;
    private readonly string? _application;
    private readonly string? _user;

    /// <summary>Creates a client of the cluster at <paramref name="endpoint"/>, such as <c>https://help.kusto.windows.net</c>.</summary>
    public KustoClient(Uri endpoint, TokenCredential credential, KustoClientOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(credential);
        options ??= new();
        Endpoint = endpoint;
        _application = options.Application ?? options.Diagnostics.ApplicationId;
        _user = options.User;
        _tracingEnabled = options.Diagnostics.IsDistributedTracingEnabled;
        string audience = (options.Audience ?? new Uri(endpoint.GetLeftPart(UriPartial.Authority))).AbsoluteUri.TrimEnd('/');
        _pipeline = HttpPipelineBuilder.Build(new HttpPipelineOptions(options)
        {
            PerRetryPolicies = { new BearerTokenAuthenticationPolicy(credential, audience + "/.default") },
            ResponseClassifier = new KustoResponseClassifier(),
        });
    }

    /// <summary>For mocking.</summary>
    protected KustoClient()
    {
        Endpoint = null!;
        _pipeline = null!;
    }

    /// <summary>The cluster endpoint, such as <c>https://help.kusto.windows.net</c>.</summary>
    public Uri Endpoint { get; }

    /// <summary>
    /// Runs a query and streams the tables of its results. The request is sent when the enumeration starts; the
    /// tables and their rows can be enumerated once, as described by <see cref="KustoTableReader.ReadTablesAsync"/>.
    /// </summary>
    /// <remarks>
    /// When <paramref name="cancellationToken"/> is canceled while the query runs, the client also asks the service to
    /// cancel it, by its <see cref="KustoRequestProperties.ClientRequestId"/>.
    /// The query is traced by a <c>KustoClient.Query</c> activity of the <see cref="KustoDiagnostics.ActivitySourceName"/>
    /// source, which lasts until the enumeration of the tables ends, and is the parent of the HTTP request activities.
    /// </remarks>
    /// <exception cref="KustoException">The service reported an error, either instead of or within the results.</exception>
    /// <exception cref="RequestFailedException">The request failed without a Kusto error.</exception>
    /// <exception cref="InvalidOperationException">The results are enumerated more than once.</exception>
    public virtual IAsyncEnumerable<KustoTable> QueryAsync(
        string database,
        string query,
        KustoRequestProperties? properties = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(database);
        ArgumentException.ThrowIfNullOrEmpty(query);
        properties ??= KustoRequestProperties.Default;
        KustoRequestBody.ValidateParameters(properties);
        return new QueryResults(this, database, query, properties, cancellationToken);
    }

    private async IAsyncEnumerable<KustoTable> QueryCoreAsync(
        string database,
        string query,
        KustoRequestProperties properties,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // The body is serialized into a pooled buffer only now, and the message returns it when disposed.
        using HttpMessage message = CreateMessage(QueryPath, KustoRequestBody.Create(database, query, properties), properties.ClientRequestId ?? NewClientRequestId("Query"));
        message.BufferResponse = false;
        message.NetworkTimeout = GetNetworkTimeout(properties, DefaultServerTimeout);

        string clientRequestId = message.Request.ClientRequestId;
        using Activity? activity = StartActivity(KustoDiagnostics.QueryActivityName, "query", database, clientRequestId);
        using CancellationTokenRegistration registration = cancellationToken.Register(() => CancelQuery(database, clientRequestId));

        Stream stream = await SendAsync(message, activity, cancellationToken).ConfigureAwait(false);
        KustoTableReader reader = new(stream);
        try
        {
            await using (stream.ConfigureAwait(false))
            {
                await foreach (KustoTable table in reader.ReadTablesAsync(cancellationToken).ConfigureAwait(false))
                {
                    yield return table;
                }
            }
        }
        finally
        {
            // Errors within the results are also thrown to the caller while it enumerates rows, outside this iterator.
            if (reader.Error is { } error)
            {
                SetError(activity, error);
            }
        }
    }

    /// <summary>Runs a management command and returns its response (the v1 format, <c>{"Tables":[...]}</c>).</summary>
    /// <param name="database">The database the command runs in the context of, or null for commands that need none.</param>
    /// <param name="command">The command, such as <c>.show tables</c>.</param>
    /// <param name="properties">The request properties, or null to send none.</param>
    /// <param name="cancellationToken">A token to cancel waiting for the response; the command itself isn't cancelled.</param>
    /// <remarks>
    /// Since commands may have side effects, the request is only retried when it was throttled. The command is traced
    /// by a <c>KustoClient.ExecuteCommand</c> activity of the <see cref="KustoDiagnostics.ActivitySourceName"/> source.
    /// </remarks>
    /// <exception cref="KustoException">The service reported an error.</exception>
    /// <exception cref="RequestFailedException">The request failed without a Kusto error.</exception>
    /// <exception cref="JsonException">The response is not valid JSON.</exception>
    public virtual async Task<JsonDocument> ExecuteCommandAsync(
        string? database,
        string command,
        KustoRequestProperties? properties = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(command);
        if (properties is not null)
        {
            KustoRequestBody.ValidateParameters(properties);
        }

        using HttpMessage message = CreateMessage(ManagementPath, KustoRequestBody.Create(database, command, properties), properties?.ClientRequestId ?? NewClientRequestId("Command"));
        message.BufferResponse = false;
        message.NetworkTimeout = GetNetworkTimeout(properties, DefaultCommandServerTimeout);
        message.ResponseClassifier = CommandResponseClassifier.Instance;

        using Activity? activity = StartActivity(KustoDiagnostics.CommandActivityName, "command", database, message.Request.ClientRequestId);
        Stream stream = await SendAsync(message, activity, cancellationToken).ConfigureAwait(false);
        await using (stream.ConfigureAwait(false))
        {
            try
            {
                return await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                SetError(activity, e);
                throw;
            }
        }
    }

    private static TimeSpan GetNetworkTimeout(KustoRequestProperties? properties, TimeSpan defaultServerTimeout) =>
        (properties?.NoRequestTimeout == true ? MaxServerTimeout : properties?.ServerTimeout ?? defaultServerTimeout) + ClientTimeoutGrace;

    private Activity? StartActivity(string name, string operation, string? database, string clientRequestId)
    {
        if (!_tracingEnabled || KustoDiagnostics.ActivitySource.StartActivity(name, ActivityKind.Client) is not { } activity)
        {
            return null;
        }

        if (activity.IsAllDataRequested)
        {
            activity.SetTag(KustoDiagnostics.Tags.AzureNamespace, "Microsoft.Kusto");
            activity.SetTag(KustoDiagnostics.Tags.ClientRequestId, clientRequestId);
            activity.SetTag(KustoDiagnostics.Tags.DatabaseNamespace, database);
            activity.SetTag(KustoDiagnostics.Tags.DatabaseOperationName, operation);
            activity.SetTag(KustoDiagnostics.Tags.ServerAddress, Endpoint.Host);
            activity.SetTag(KustoDiagnostics.Tags.ServerPort, Endpoint.Port);
        }

        return activity;
    }

    private async ValueTask<Stream> SendAsync(HttpMessage message, Activity? activity, CancellationToken cancellationToken)
    {
        try
        {
            await _pipeline.SendAsync(message, cancellationToken).ConfigureAwait(false);
            if (activity is { IsAllDataRequested: true } && message.Response.Headers.TryGetValue("x-ms-activity-id", out string? activityId))
            {
                activity.SetTag(KustoDiagnostics.Tags.ServiceRequestId, activityId);
            }

            return await GetContentStreamAsync(message.Response, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            SetError(activity, e);
            throw;
        }
    }

    private static void SetError(Activity? activity, Exception error)
    {
        activity?.SetStatus(ActivityStatusCode.Error, error.Message);
        activity?.SetTag(KustoDiagnostics.Tags.ErrorType, error is RequestFailedException { ErrorCode: { } code } ? code : error.GetType().FullName);
    }

    private void CancelQuery(string database, string clientRequestId) => _ = Task.Run(async () =>
    {
        RequestContent body = KustoRequestBody.Create(database, KustoRequestBody.CancelQueryCommand(clientRequestId, "Request cancelled by user"), null);
        using HttpMessage message = CreateMessage(ManagementPath, body, NewClientRequestId("CancelQuery"));
        message.NetworkTimeout = CancelTimeout;
        try
        {
            await _pipeline.SendAsync(message, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e) when (e is RequestFailedException or OperationCanceledException or IOException)
        {
            // Best effort: the service also stops the query when its connection is closed or it times out.
        }
    });

    private HttpMessage CreateMessage(string path, RequestContent body, string clientRequestId)
    {
        HttpMessage message = _pipeline.CreateMessage();
        Request request = message.Request;
        request.Method = RequestMethod.Post;
        request.Uri.Reset(Endpoint);
        request.Uri.AppendPath(path, escape: false);
        request.ClientRequestId = clientRequestId;
        request.Headers.Add(HttpHeader.Names.Accept, "application/json");
        request.Headers.Add(HttpHeader.Names.ContentType, "application/json; charset=utf-8");
        request.Headers.Add("Accept-Encoding", "gzip");
        request.Headers.Add("x-ms-client-version", ClientVersion);
        if (_application is not null)
        {
            request.Headers.Add("x-ms-app", _application);
        }

        if (_user is not null)
        {
            request.Headers.Add("x-ms-user-id", _user);
        }

        request.Content = body;
        return message;
    }

    private static string NewClientRequestId(string operation) => $"Kusto.Data.Queries.{operation};{Guid.NewGuid()}";

    private static async ValueTask<Stream> GetContentStreamAsync(Response response, CancellationToken cancellationToken)
    {
        Stream stream = response.ContentStream ?? Stream.Null;
        if (response.Headers.TryGetValue("Content-Encoding", out string? encoding) && encoding.Equals("gzip", StringComparison.OrdinalIgnoreCase))
        {
            stream = new GZipStream(stream, CompressionMode.Decompress);
        }

        if (!response.IsError)
        {
            return stream;
        }

        MemoryStream content = new();
        await using (stream.ConfigureAwait(false))
        {
            await stream.CopyToAsync(content, cancellationToken).ConfigureAwait(false);
        }

        content.Position = 0;
        response.ContentStream = content;
        throw CreateException(response, content.GetBuffer().AsSpan(0, (int)content.Length));
    }

    private static RequestFailedException CreateException(Response response, ReadOnlySpan<byte> content)
    {
        try
        {
            ErrorPayload? payload = JsonSerializer.Deserialize(content, KustoJsonContext.Default.ErrorPayload);
            List<KustoError> errors = ErrorPayload.Flatten([payload]);
            if (errors.Count > 0)
            {
                return new KustoException(response.Status, errors);
            }
        }
        catch (JsonException)
        {
        }

        return new RequestFailedException(response);
    }

    // Like the tables they are read as, the results are enumerable once; otherwise each enumeration would send the query again.
    private sealed class QueryResults(KustoClient client, string database, string query, KustoRequestProperties properties, CancellationToken cancellationToken)
        : IAsyncEnumerable<KustoTable>
    {
        private bool _started;

        public IAsyncEnumerator<KustoTable> GetAsyncEnumerator(CancellationToken enumeratorCancellationToken = default) =>
            Interlocked.Exchange(ref _started, true)
                ? throw new InvalidOperationException("The results of a query can only be enumerated once.")
                : client.QueryCoreAsync(database, query, properties, cancellationToken).GetAsyncEnumerator(enumeratorCancellationToken);
    }

    private sealed class KustoResponseClassifier : ResponseClassifier
    {
        // A client-side timeout outlasts the service's own timeout, so retrying it would only run the query again.
        public override bool IsRetriable(HttpMessage message, Exception exception) => IsRetriableException(exception);
    }

    // A command may have run, at least partially, unless it was throttled.
    private sealed class CommandResponseClassifier : ResponseClassifier
    {
        public static readonly CommandResponseClassifier Instance = new();

        public override bool IsRetriableResponse(HttpMessage message) => message.Response.Status == 429;

        public override bool IsRetriable(HttpMessage message, Exception exception) => false;
    }
}

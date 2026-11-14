using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Compression;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using AwesomeAssertions;
using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;

namespace Kusto.Data.Queries.Tests;

[TestClass]
public class KustoClientTests
{
    public TestContext TestContext { get; set; } = null!;

    [TestMethod]
    public async Task AddKustoClient_UsesTheSharedCredentialAndConfiguredOptions()
    {
        Handler handler = new((_, _, _) => Task.FromResult(Gzip("{}"u8.ToArray())));
        Credential credential = new();
        ServiceCollection services = new();
        services.AddAzureClients(builder =>
        {
            builder.UseCredential(credential);
            builder.ConfigureDefaults(options =>
            {
                options.Transport = new HttpClientTransport(new HttpClient(handler));
                options.Retry.MaxRetries = 0;
            });
            builder.AddKustoClient(new Uri("https://help.kusto.windows.net"))
                .ConfigureOptions(options => options.Application = "registered-app");
        });
        using ServiceProvider provider = services.BuildServiceProvider();
        KustoClient client = provider.GetRequiredService<KustoClient>();

        using JsonDocument response = await client.ExecuteCommandAsync("Samples", ".show tables", cancellationToken: TestContext.CancellationToken);

        credential.Scopes.Should().Equal("https://help.kusto.windows.net/.default");
        HttpRequestMessage request = handler.Requests.Single().Request;
        request.RequestUri.Should().Be(new Uri("https://help.kusto.windows.net/v1/rest/mgmt"));
        request.Headers.GetValues("x-ms-app").Should().Equal("registered-app");
    }

    [TestMethod]
    public async Task AddKustoClient_NamedClientsHaveSeparateEndpointsCredentialsAndOptions()
    {
        Handler handler = new((_, _, _) => Task.FromResult(Gzip("{}"u8.ToArray())));
        Credential sharedCredential = new();
        Credential otherCredential = new();
        ServiceCollection services = new();
        services.AddAzureClients(builder =>
        {
            builder.UseCredential(sharedCredential);
            builder.ConfigureDefaults(options =>
            {
                options.Transport = new HttpClientTransport(new HttpClient(handler));
                options.Retry.MaxRetries = 0;
                options.Diagnostics.ApplicationId = "shared-app";
            });
            builder.AddKustoClient(new Uri("https://help.kusto.windows.net")).WithName("help");
            builder.AddKustoClient(new Uri("https://other.kusto.windows.net"))
                .WithName("other")
                .WithCredential(otherCredential)
                .ConfigureOptions(options => options.Application = "other-app");
        });
        using ServiceProvider provider = services.BuildServiceProvider();
        IAzureClientFactory<KustoClient> factory = provider.GetRequiredService<IAzureClientFactory<KustoClient>>();

        using JsonDocument helpResponse = await factory.CreateClient("help")
            .ExecuteCommandAsync("Samples", ".show tables", cancellationToken: TestContext.CancellationToken);
        using JsonDocument otherResponse = await factory.CreateClient("other")
            .ExecuteCommandAsync("Samples", ".show tables", cancellationToken: TestContext.CancellationToken);

        sharedCredential.Scopes.Should().Equal("https://help.kusto.windows.net/.default");
        otherCredential.Scopes.Should().Equal("https://other.kusto.windows.net/.default");
        handler.Requests.Select(r => r.Request.RequestUri).Should().Equal(
            new Uri("https://help.kusto.windows.net/v1/rest/mgmt"),
            new Uri("https://other.kusto.windows.net/v1/rest/mgmt"));
        handler.Requests.First().Request.Headers.GetValues("x-ms-app").Should().Equal("shared-app");
        handler.Requests.Last().Request.Headers.GetValues("x-ms-app").Should().Equal("other-app");
    }

    [TestMethod]
    public async Task QueryAsync_SendsTheQueryAndStreamsTheResults()
    {
        byte[] payload = File.ReadAllBytes(Path.Combine(AppContext.BaseDirectory, "testdata", "StormEvents.100.json"));
        Handler handler = new((_, _, _) => Task.FromResult(Gzip(payload)));
        Credential credential = new();
        KustoClient client = CreateClient(handler, credential);
        KustoRequestProperties properties = new()
        {
            ClientRequestId = "crid",
            ServerTimeout = TimeSpan.FromSeconds(90),
            QueryConsistency = KustoQueryConsistency.WeakByQuery,
            QueryNow = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc),
            QueryCursorScopedTables = ["StormEvents"],
            MaterializedViewShuffleQuery = [new("V1", ["K1"]), new("V2")],
            AdditionalOptions = new() { ["custom_option"] = 1 },
            Parameters =
            {
                ["s"] = "a \"q\" \\",
                ["l"] = -5L,
                ["i"] = 7,
                ["r"] = double.NegativeInfinity,
                ["d"] = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc).AddTicks(1234567),
                ["t"] = -new TimeSpan(1, 2, 3, 4, 500),
                ["g"] = Guid.Parse("5f9cba8e-9a4d-42a1-840e-49208fc247e3"),
                ["b"] = true,
                ["dy"] = JsonNode.Parse("""{"a":[1,"x"]}""")!,
                ["dec"] = 1.25m,
            },
        };
        using CancellationTokenSource cts = new();

        List<(string Name, int Rows)> tables = await ReadAllAsync(client.QueryAsync("Samples", "StormEvents | take 100", properties, cts.Token));

        tables.Should().Equal(("@ExtendedProperties", 1), ("PrimaryResult", 100), ("QueryCompletionInformation", 4));
        credential.Scopes.Should().Equal("https://help.kusto.windows.net/.default");
        (HttpRequestMessage request, string body) = handler.Requests.Single();
        request.Method.Should().Be(HttpMethod.Post);
        request.RequestUri.Should().Be(new Uri("https://help.kusto.windows.net/v2/rest/query"));
        request.Headers.Authorization!.ToString().Should().Be("Bearer token");
        request.Headers.GetValues("x-ms-client-request-id").Should().Equal("crid");
        request.Headers.GetValues("x-ms-app").Should().Equal("app");
        request.Headers.AcceptEncoding.ToString().Should().Be("gzip");
        JsonNode.DeepEquals(JsonNode.Parse(body), JsonNode.Parse(
            """
            {"db":"Samples","csl":"StormEvents | take 100","properties":{
              "Options":{"results_progressive_enabled":false,"servertimeout":"00:01:30","queryconsistency":"weakconsistency_by_query",
                "query_now":"2024-01-02T03:04:05Z","query_cursor_scoped_tables":["StormEvents"],
                "materialized_view_shuffle_query":[{"Name":"V1","Keys":["K1"]},{"Name":"V2"}],"custom_option":1},
              "Parameters":{"s":"a \"q\" \\","l":-5,"i":"int(7)","r":"real(-inf)","d":"datetime(2024-01-02T03:04:05.1234567Z)",
                "t":"time(-1.02:03:04.5000000)","g":"guid(5f9cba8e-9a4d-42a1-840e-49208fc247e3)","b":"true",
                "dy":"dynamic({\"a\":[1,\"x\"]})","dec":"decimal(1.25)"}}}
            """)).Should().BeTrue(body);

        // The query has completed, so cancelling now must not cancel it on the service.
        await cts.CancelAsync();
        await Task.Delay(100, TestContext.CancellationToken);
        handler.Requests.Should().HaveCount(1);
    }

    [TestMethod]
    public async Task QueryAsync_ThrowsKustoExceptionWhenTheServiceRejectsTheQuery()
    {
        byte[] payload = File.ReadAllBytes(Path.Combine(AppContext.BaseDirectory, "testdata", "Error.BadRequest.json"));
        KustoClient client = CreateClient(new((_, _, _) => Task.FromResult(Gzip(payload, HttpStatusCode.BadRequest))));

        KustoException e = (await client.Invoking(c => ReadAllAsync(c.QueryAsync("Samples", "bad"))).Should().ThrowAsync<KustoException>()).Which;

        e.Status.Should().Be(400);
        e.ErrorCode.Should().Be("General_BadRequest");
        e.Message.Should().StartWith("Request is invalid and cannot be processed: Syntax error");
    }

    [TestMethod]
    public async Task QueryAsync_ThrowsRequestFailedExceptionWhenTheErrorIsNotFromKusto()
    {
        KustoClient client = CreateClient(new((_, _, _) => Task.FromResult(
            new HttpResponseMessage(HttpStatusCode.BadGateway) { Content = new StringContent("<html>Bad gateway</html>") })));

        RequestFailedException e = (await client.Invoking(c => ReadAllAsync(c.QueryAsync("Samples", "print 1"))).Should().ThrowAsync<RequestFailedException>()).Which;

        e.Should().NotBeOfType<KustoException>();
        e.Status.Should().Be(502);
    }

    [TestMethod]
    public async Task QueryAsync_CancelsTheQueryOnTheServiceWhenCancelled()
    {
        TaskCompletionSource querySent = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<(HttpRequestMessage Request, string Body)> cancelSent = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Handler handler = new(async (request, body, cancellationToken) =>
        {
            if (request.RequestUri!.AbsolutePath == "/v1/rest/mgmt")
            {
                cancelSent.SetResult((request, body));
                return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{}") };
            }

            querySent.SetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken);
            throw new UnreachableException();
        });
        KustoClient client = CreateClient(handler);
        using CancellationTokenSource cts = new();

        Task read = ReadAllAsync(client.QueryAsync("Samples", "print 1", new() { ClientRequestId = "my \"id\"" }, cts.Token));
        await querySent.Task.WaitAsync(TestContext.CancellationToken);
        await cts.CancelAsync();

        await read.Invoking(t => t).Should().ThrowAsync<OperationCanceledException>();
        (HttpRequestMessage request, string body) = await cancelSent.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.CancellationToken);
        request.RequestUri.Should().Be(new Uri("https://help.kusto.windows.net/v1/rest/mgmt"));
        request.Headers.Authorization!.ToString().Should().Be("Bearer token");
        JsonNode.DeepEquals(JsonNode.Parse(body), JsonNode.Parse(
            """{"db":"Samples","csl":".cancel query \"my \\\"id\\\"\" with (reason=\"Request cancelled by user\")"}"""))
            .Should().BeTrue(body);
    }

    [TestMethod]
    public async Task QueryAsync_TracesTheQueryUntilItsResultsAreRead()
    {
        byte[] payload = File.ReadAllBytes(Path.Combine(AppContext.BaseDirectory, "testdata", "StormEvents.100.json"));
        Handler handler = new((_, _, _) =>
        {
            HttpResponseMessage response = Gzip(payload);
            response.Headers.Add("x-ms-activity-id", "service-activity");
            return Task.FromResult(response);
        });
        KustoClient client = CreateClient(handler);
        string clientRequestId = Guid.NewGuid().ToString();
        using ActivityCollector collector = new();
        using Activity parent = new Activity("Test").Start();

        IAsyncEnumerator<KustoTable> tables = client.QueryAsync("Samples", "print 1", new() { ClientRequestId = clientRequestId })
            .GetAsyncEnumerator(TestContext.CancellationToken);
        (await tables.MoveNextAsync()).Should().BeTrue();
        Activity query = collector.Single(clientRequestId);
        query.IsStopped.Should().BeFalse("the results are still being read");
        while (await tables.MoveNextAsync())
        {
        }

        await tables.DisposeAsync();

        query.IsStopped.Should().BeTrue();
        query.OperationName.Should().Be("KustoClient.Query");
        query.Kind.Should().Be(ActivityKind.Client);
        query.ParentId.Should().Be(parent.Id);
        query.Status.Should().Be(ActivityStatusCode.Unset);
        query.TagObjects.Should().BeEquivalentTo(new Dictionary<string, object?>
        {
            ["az.namespace"] = "Microsoft.Kusto",
            ["az.client_request_id"] = clientRequestId,
            ["az.service_request_id"] = "service-activity",
            ["db.namespace"] = "Samples",
            ["db.operation.name"] = "query",
            ["server.address"] = "help.kusto.windows.net",
            ["server.port"] = 443,
        });
        handler.Requests.Single().Request.Headers.GetValues("traceparent").Should().Equal(query.Id);
    }

    [TestMethod]
    [DataRow("Error.BadRequest.json", HttpStatusCode.BadRequest, "General_BadRequest", DisplayName = "rejected")]
    [DataRow("Error.ResultTruncated.json", HttpStatusCode.OK, "LimitsExceeded", DisplayName = "error within the results")]
    public async Task QueryAsync_MarksTheTraceOfAFailedQuery(string file, HttpStatusCode status, string errorType)
    {
        byte[] payload = File.ReadAllBytes(Path.Combine(AppContext.BaseDirectory, "testdata", file));
        KustoClient client = CreateClient(new((_, _, _) => Task.FromResult(Gzip(payload, status))));
        string clientRequestId = Guid.NewGuid().ToString();
        using ActivityCollector collector = new();

        await client.Invoking(c => ReadAllAsync(c.QueryAsync("Samples", "print 1", new() { ClientRequestId = clientRequestId })))
            .Should().ThrowAsync<KustoException>();

        Activity query = collector.Single(clientRequestId);
        query.IsStopped.Should().BeTrue();
        query.Status.Should().Be(ActivityStatusCode.Error);
        query.GetTagItem("error.type").Should().Be(errorType);
    }

    [TestMethod]
    [DataRow("Samples", DisplayName = "with database")]
    [DataRow(null, DisplayName = "without database")]
    public async Task ExecuteCommandAsync_SendsTheCommandAndReturnsTheResponse(string? database)
    {
        Handler handler = new((_, _, _) => Task.FromResult(Gzip(
            """{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"TableName","DataType":"String","ColumnType":"string"}],"Rows":[["StormEvents"]]}]}"""u8.ToArray())));
        KustoClient client = CreateClient(handler);

        using JsonDocument response = await client.ExecuteCommandAsync(database, ".show tables", cancellationToken: TestContext.CancellationToken);

        response.RootElement.GetProperty("Tables")[0].GetProperty("Rows")[0][0].GetString().Should().Be("StormEvents");
        (HttpRequestMessage request, string body) = handler.Requests.Single();
        request.RequestUri.Should().Be(new Uri("https://help.kusto.windows.net/v1/rest/mgmt"));
        request.Headers.Authorization!.ToString().Should().Be("Bearer token");
        JsonObject expected = new() { ["csl"] = ".show tables" };
        if (database is not null)
        {
            expected["db"] = database;
        }

        JsonNode.DeepEquals(JsonNode.Parse(body), expected).Should().BeTrue(body);
    }

    [TestMethod]
    public async Task ExecuteCommandAsync_SendsTheRequestProperties()
    {
        Handler handler = new((_, _, _) => Task.FromResult(Gzip("""{"Tables":[]}"""u8.ToArray())));
        KustoClient client = CreateClient(handler);
        KustoRequestProperties properties = new()
        {
            ClientRequestId = "crid",
            ServerTimeout = TimeSpan.FromMinutes(30),
            RequestDescription = "desc",
            Parameters = { ["n"] = 5L },
        };

        using JsonDocument response = await client.ExecuteCommandAsync("Samples", ".show tables", properties, TestContext.CancellationToken);

        (HttpRequestMessage request, string body) = handler.Requests.Single();
        request.Headers.GetValues("x-ms-client-request-id").Should().Equal("crid");
        JsonNode.DeepEquals(JsonNode.Parse(body), JsonNode.Parse(
            """
            {"db":"Samples","csl":".show tables","properties":{
              "Options":{"results_progressive_enabled":false,"request_description":"desc","servertimeout":"00:30:00"},
              "Parameters":{"n":5}}}
            """)).Should().BeTrue(body);
    }

    [TestMethod]
    public async Task ExecuteCommandAsync_RejectsUnsupportedParameterValuesBeforeSending()
    {
        Handler handler = new((_, _, _) => throw new UnreachableException());
        KustoClient client = CreateClient(handler);

        await client.Invoking(c => c.ExecuteCommandAsync("Samples", ".show tables", new() { Parameters = { ["x"] = new object() } }, TestContext.CancellationToken))
            .Should().ThrowAsync<ArgumentException>().WithMessage("*'x'*");
        handler.Requests.Should().BeEmpty();
    }

    [TestMethod]
    public async Task ExecuteCommandAsync_OnlyRetriesWhenThrottled()
    {
        Queue<HttpStatusCode> statuses = new([HttpStatusCode.TooManyRequests, HttpStatusCode.ServiceUnavailable, HttpStatusCode.OK]);
        Handler handler = new((_, _, _) => Task.FromResult(new HttpResponseMessage(statuses.Dequeue()) { Content = new StringContent("{}") }));
        KustoClient client = CreateClient(handler, maxRetries: 3);

        RequestFailedException e = (await client.Invoking(c => c.ExecuteCommandAsync("Samples", ".drop table T", cancellationToken: TestContext.CancellationToken))
            .Should().ThrowAsync<RequestFailedException>()).Which;

        e.Status.Should().Be(503);
        handler.Requests.Should().HaveCount(2);
    }

    [TestMethod]
    public async Task QueryAsync_SendsQueriesLargerThanTheInitialBuffer()
    {
        Handler handler = new((_, _, _) => Task.FromResult(Gzip(Harness.Utf8(Payloads.Response()))));
        KustoClient client = CreateClient(handler);
        string query = "print '" + string.Concat(Enumerable.Repeat("é☃😀x", 50_000)) + "'";

        await ReadAllAsync(client.QueryAsync("Samples", query, cancellationToken: TestContext.CancellationToken));

        JsonNode.Parse(handler.Requests.Single().Body)!["csl"]!.GetValue<string>().Should().Be(query);
    }

    [TestMethod]
    public void QueryAsync_RejectsUnsupportedParameterValuesBeforeSending()
    {
        Handler handler = new((_, _, _) => throw new UnreachableException());
        KustoClient client = CreateClient(handler);

        client.Invoking(c => c.QueryAsync("Samples", "print 1", new() { Parameters = { ["x"] = new object() } }))
            .Should().Throw<ArgumentException>().WithMessage("*'x'*");
        handler.Requests.Should().BeEmpty();
    }

    [TestMethod]
    public async Task QueryAsync_ResultsCanOnlyBeEnumeratedOnce()
    {
        Handler handler = new((_, _, _) => Task.FromResult(Gzip(Harness.Utf8(Payloads.Response(Payloads.Table(1, """[["a"]]""", name: "T"))))));
        KustoClient client = CreateClient(handler);
        IAsyncEnumerable<KustoTable> results = client.QueryAsync("Samples", "print 1", cancellationToken: TestContext.CancellationToken);

        (await ReadAllAsync(results)).Should().Equal(("T", 1));

        (await client.Invoking(_ => ReadAllAsync(results)).Should().ThrowExactlyAsync<InvalidOperationException>())
            .WithMessage("*only be enumerated once*");
        handler.Requests.Should().HaveCount(1);
    }

    private static KustoClient CreateClient(Handler handler, Credential? credential = null, int maxRetries = 0) => new(
        new Uri("https://help.kusto.windows.net"),
        credential ?? new Credential(),
        new KustoClientOptions
        {
            Application = "app",
            Retry = { MaxRetries = maxRetries, Mode = RetryMode.Fixed, Delay = TimeSpan.FromMilliseconds(1) },
            Transport = new HttpClientTransport(new HttpClient(handler)),
        });

    private static async Task<List<(string Name, int Rows)>> ReadAllAsync(IAsyncEnumerable<KustoTable> tables)
    {
        List<(string, int)> result = [];
        await foreach (KustoTable table in tables)
        {
            int rows = 0;
            await foreach (KustoRow _ in table.GetRowsAsync())
            {
                rows++;
            }

            result.Add((table.Name, rows));
        }

        return result;
    }

    private static HttpResponseMessage Gzip(byte[] payload, HttpStatusCode status = HttpStatusCode.OK)
    {
        MemoryStream compressed = new();
        using (GZipStream gzip = new(compressed, CompressionLevel.Fastest, leaveOpen: true))
        {
            gzip.Write(payload);
        }

        ByteArrayContent content = new(compressed.ToArray());
        content.Headers.ContentType = new("application/json");
        content.Headers.ContentEncoding.Add("gzip");
        return new HttpResponseMessage(status) { Content = content };
    }

    /// <summary>Collects the client's activities; tests in parallel tell theirs apart by client request id.</summary>
    private sealed class ActivityCollector : IDisposable
    {
        private readonly ConcurrentQueue<Activity> _started = new();
        private readonly ActivityListener _listener;

        public ActivityCollector()
        {
            _listener = new()
            {
                ShouldListenTo = source => source.Name == KustoDiagnostics.ActivitySourceName,
                Sample = (ref _) => ActivitySamplingResult.AllData,
                ActivityStarted = _started.Enqueue,
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public Activity Single(string clientRequestId) =>
            _started.Single(activity => (string?)activity.GetTagItem("az.client_request_id") == clientRequestId);

        public void Dispose() => _listener.Dispose();
    }

    private sealed class Handler(Func<HttpRequestMessage, string, CancellationToken, Task<HttpResponseMessage>> respond) : HttpMessageHandler
    {
        public ConcurrentQueue<(HttpRequestMessage Request, string Body)> Requests { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            string body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(cancellationToken);
            Requests.Enqueue((request, body));
            return await respond(request, body, cancellationToken);
        }
    }

    private sealed class Credential : TokenCredential
    {
        public ConcurrentQueue<string> Scopes { get; } = new();

        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            foreach (string scope in requestContext.Scopes)
            {
                Scopes.Enqueue(scope);
            }

            return new("token", DateTimeOffset.UtcNow.AddHours(1));
        }

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new(GetToken(requestContext, cancellationToken));
    }
}

# Kusto.Data.Queries

A modern, near-zero-allocation .NET client for the Kusto query REST API (Azure Data Explorer, Microsoft Fabric
Eventhouse). It asynchronously streams query results straight from the HTTP response: rows are not materialized into a
`DataTable` or `object[]`, but exposed as views over a pooled buffer, so you only pay for the values you actually read.

- **Async streaming:** tables and rows are `IAsyncEnumerable`s that read the
  [v2 (non-progressive)](https://learn.microsoft.com/kusto/api/rest/response-v2) response frame by frame as it arrives,
  never blocking a thread on I/O, with memory bounded by the largest single row, not the response.
- **Near-zero allocation:** rows are UTF-8 JSON slices of a pooled buffer, so reading a response allocates a constant
  few kilobytes whatever its size.
  Converting rows to objects or documents costs only the values themselves, and requests are serialized into pooled
  buffers too.
- **Azure.Core:** built on `HttpPipeline` with `TokenCredential` authentication, retries, logging and
  `ClientOptions`, like the other Azure SDK clients.
- **Distributed tracing:** a `KustoClient.Query` activity that lasts until the results are read, and W3C trace
  context propagation.
- **Native AOT and trimming compatible:** System.Text.Json source generation throughout, no reflection.

## Getting started

```csharp
using Kusto.Data.Queries;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.DependencyInjection;

ServiceCollection services = new();
services.AddAzureClients(builder =>
    builder.AddKustoClient(new Uri("https://help.kusto.windows.net")));
using var provider = services.BuildServiceProvider();
var client = provider.GetRequiredService<KustoClient>();

KustoRequestProperties properties = new()
{
    ServerTimeout = TimeSpan.FromMinutes(10),
    Parameters = { ["state"] = "TEXAS" },
};

await foreach (KustoTable table in client.QueryAsync(
    "Samples",
    "declare query_parameters(state:string); StormEvents | where State == state | take 10",
    properties))
{
    if (table.Kind != KustoTableKind.PrimaryResult)
        continue;

    await foreach (KustoRow row in table.GetRowsAsync())
    {
        Console.WriteLine(string.Join(" | ", row.ToValues()));
    }
}
```

## API

| API | Description |
| --- | --- |
| `KustoClient.QueryAsync()` | Runs a query and streams its tables as an `IAsyncEnumerable<KustoTable>`. |
| `KustoClient.ExecuteCommandAsync()` | Runs a management command and returns its response as a `JsonDocument`. |
| `KustoClientOptions` | Azure.Core's `ClientOptions` (retries, transport, diagnostics), plus `Audience`, `Application` and `User`. |
| `KustoRequestProperties` | A typed version of the [request properties](https://learn.microsoft.com/kusto/api/rest/request-properties), plus `ClientRequestId`, query `Parameters` and `AdditionalOptions`. |
| `KustoTableReader.ReadTablesAsync()` | Streams the tables of a Kusto v2 (non-progressive) response from any `Stream`, such as a saved response. |
| `KustoTable.GetRowsAsync()` | Streams the table's rows as an `IAsyncEnumerable<KustoRow>`. A row is only valid until its enumerator advances. |
| `KustoTable.GetColumnIndex()` | Returns the index of a column by name. |
| `KustoRow.ToValues()` | Converts the row's values to .NET objects. |
| `KustoRow.GetValue<T>()` | Converts one value, by column index or name, to `T`. |
| `KustoRow.ToDocument()` | Copies the row into a disposable `JsonDocument`. |
| `KustoRow.UseData()` | Passes the row's raw UTF-8 JSON array to a callback, without copying. |
| `AddKustoClient()` extension | Registers a client with `Microsoft.Extensions.Azure`'s `AddAzureClients`. |

Canceling a query also sends `.cancel query` to the service, so it stops running there.

## Benchmarks

Reading a whole response from memory (parsing only, no network), compared with the reader of the
[official SDK](https://www.nuget.org/packages/Microsoft.Azure.Kusto.Data):

**StormEvents.10000.json** (11.3 MB, 10,000 rows of 22 columns, including `dynamic`):

| Benchmark | Mean | Ratio | Gen0 | Gen1 | Allocated | Alloc ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `Sdk` (baseline) - `IDataReader.GetValues` for every row. | 26.37 ms | 1.00 | 6843.75 | 93.75 | 55.01 MB | 1.000 |
| `ToValues` for every row. | 14.31 ms | 0.54 | 3359.38 | 62.50 | 26.89 MB | 0.489 |
| `ToDocument` for every row, disposing each document. | 15.33 ms | 0.58 | 78.13 | - | 715.3 KB | 0.013 |
| `RawRows` - `KustoRow.UseData` for every row, reading the raw UTF-8 JSON: the cost of the reader itself. | 4.40 ms | 0.17 | - | - | 11.8 KB | 0.000 |
| `TokenizeOnly` (floor) - A `Utf8JsonReader` over the whole response. | 4.13 ms | 0.16 | - | - | - | - |

# Sample data

Real responses from `https://help.kusto.windows.net/v2/rest/query` (database `Samples`), requested with `results_progressive_enabled=false`. Regenerate from this directory with `dotnet run fetch.cs [name...]` (requires the .NET SDK and a signed-in Azure CLI; authenticates with `AzureCliCredential` from Azure.Core); omit names to fetch every query. Queries live in `queries/<name>.kql`. Timestamps and activity ids differ per run.

| File | Query | HTTP | Contents |
|---|---|---|---|
| `AllTypes.json` | `queries/AllTypes.kql` | 200 | One row per scalar value across every column type |
| `StormEvents.100.json` | `queries/StormEvents.100.kql` | 200 | 100 StormEvents rows, including the `dynamic` column `StormSummary` |
| `StormEvents.10000.json` | `queries/StormEvents.10000.kql` | 200 | Same, 10000 rows (~11.9 MB) |
| `Numbers.100000.json` | `queries/Numbers.100000.kql` | 200 | 100000 small rows (long, real, string, datetime) |
| `Error.BadRequest.json` | `queries/Error.BadRequest.kql` | 400 | Not a frame array: a single `{"error":{"code":"General_BadRequest",...}}` object (syntax error) |
| `Error.ResultTruncated.json` | `queries/Error.ResultTruncated.kql` | 200 | `PrimaryResult` has 10 rows plus an 11th "row" that is the object `{"OneApiErrors":[{"error":{"code":"LimitsExceeded",...}}]}`; the final `DataSetCompletion` has `"HasErrors":true` and the same `OneApiErrors` |
| `Error.MemoryExceeded.json` | `queries/Error.MemoryExceeded.kql` | 200 | `PrimaryResult` has no real rows, only the single "row" `{"OneApiErrors":[...]}` (E_RUNAWAY_QUERY); the final `DataSetCompletion` has `"HasErrors":true` and `OneApiErrors`. The header carries `"ErrorReportingPlacement":"InData"` |

All 200 responses start with a `DataSetHeader` frame with `"IsProgressive":false` and use `DataTable` frames only.
`assert()` is rejected up front with a 400 (SEM0040), so a memory-budget failure is used for the mid-execution error.

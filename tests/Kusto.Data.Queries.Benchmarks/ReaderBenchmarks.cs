using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Kusto.Data.Queries;
using Kusto.Data.Data;
using Kusto.Data.Net.Client;
using Kusto.Data.Results;

[MemoryDiagnoser]
[MarkdownExporterAttribute.GitHub]
public class ReaderBenchmarks
{
    byte[] _bytes = [];

    [Params("StormEvents.10000.json", "Numbers.100000.json")]
    public string File { get; set; } = "";

    [GlobalSetup]
    public async Task Setup()
    {
        _bytes = await System.IO.File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "testdata", File));
        var sdk = Sdk();
        var raw = await KustoTableReader_RawRows();
        var rows = await RowCount();
        if (sdk != rows)
        {
            throw new InvalidOperationException($"{File}: SDK saw {sdk} rows, KustoTableReader saw {rows}.");
        }

        if (raw <= 0)
        {
            throw new InvalidOperationException($"{File}: no raw bytes read.");
        }
    }

    [Benchmark(Baseline = true)]
    public long Sdk()
    {
        long rows = 0;
        using var frames = KustoJsonDataStream.CreateProgressiveReader(new MemoryStream(_bytes), Encoding.UTF8, new KustoClientRequestDescriptor("", "", ""));
        while (frames.MoveNext())
        {
            if (frames.Current is not ProgressiveDataSetDataTableFrame frame)
            {
                continue;
            }

            using var reader = frame.TableData;
            var values = new object[reader.FieldCount];
            while (reader.Read())
            {
                reader.GetValues(values);
                rows++;
            }
        }
        return rows;
    }

    [Benchmark]
    public async Task<long> KustoTableReader_ToValues()
    {
        long values = 0;
        await foreach (var table in new KustoTableReader(new MemoryStream(_bytes)).ReadTablesAsync())
        {
            await foreach (var row in table.GetRowsAsync())
            {
                values += row.ToValues().Length;
            }
        }

        return values;
    }

    [Benchmark]
    public async Task<long> KustoTableReader_ToDocument()
    {
        long values = 0;
        await foreach (var table in new KustoTableReader(new MemoryStream(_bytes)).ReadTablesAsync())
        {
            await foreach (var row in table.GetRowsAsync())
            {
                using JsonDocument document = row.ToDocument();
                values += document.RootElement.GetArrayLength();
            }
        }

        return values;
    }

    [Benchmark]
    public async Task<long> KustoTableReader_RawRows()
    {
        long bytes = 0;
        await foreach (var table in new KustoTableReader(new MemoryStream(_bytes)).ReadTablesAsync())
        {
            await foreach (var row in table.GetRowsAsync())
            {
                bytes += row.UseData(static (span, _) => span.Length, 0);
            }
        }

        return bytes;
    }

    // The floor for any reader built on Utf8JsonReader: one tokenizing pass, with no framing and no copy from a stream.
    [Benchmark]
    public long Utf8JsonReader_TokenizeOnly()
    {
        long tokens = 0;
        Utf8JsonReader reader = new(_bytes);
        while (reader.Read())
        {
            tokens++;
        }

        return tokens;
    }

    async Task<long> RowCount()
    {
        long rows = 0;
        await foreach (var table in new KustoTableReader(new MemoryStream(_bytes)).ReadTablesAsync())
        {
            await foreach (var row in table.GetRowsAsync())
            {
                rows++;
            }
        }

        return rows;
    }
}

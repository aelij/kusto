using System.Text.Json;
using AwesomeAssertions;

namespace Kusto.Data.Queries.Tests;

[TestClass]
public class KustoRowGetValueTests
{
    private const string LongColumns = """[{"ColumnName":"a","ColumnType":"long"},{"ColumnName":"b","ColumnType":"long"},{"ColumnName":"c","ColumnType":"long"}]""";

    public static IEnumerable<object[]> BufferSizesAndCompletions =>
        from size in (int[])[1, 2, 3, 7, 16, 64] from completion in Enum.GetValues<Completion>() select new object[] { size, completion };

    private static object? Get(KustoRow row, int column, bool byName)
    {
        string name = row.Table.Columns[column].Name;
        return row.Table.Columns[column].Type switch
        {
            KustoColumnType.Long => byName ? row.GetValue<long>(name) : row.GetValue<long>(column),
            KustoColumnType.Real => byName ? row.GetValue<double>(name) : row.GetValue<double>(column),
            KustoColumnType.String => byName ? row.GetValue<string>(name) : row.GetValue<string>(column),
            KustoColumnType.Dynamic => (byName ? row.GetValue<JsonElement>(name) : row.GetValue<JsonElement>(column))?.GetRawText(),
            var type => throw new NotSupportedException(type.ToString()),
        };
    }

    /// <summary>Reads every value of the representative payload with GetValue and checks it against ToValues, which reads a row in one pass.</summary>
    private static async Task VerifyAsync(TestStream stream, int bufferSize, string context = "")
    {
        TrackingArrayPool pool = new();
        int rows = 0;
        await foreach (KustoTable table in new KustoTableReader(stream, pool, bufferSize).ReadTablesAsync())
        {
            await foreach (KustoRow row in table.GetRowsAsync())
            {
                object?[] expected = row.ToValues();

                // Rows alternate between reading the columns backwards, which indexes the row at once, and forwards, which extends the index for each column.
                for (int n = 0; n < expected.Length; n++)
                {
                    int column = rows % 2 == 0 ? expected.Length - 1 - n : n;
                    object? value = expected[column] is JsonElement element ? element.GetRawText() : expected[column];
                    Get(row, column, byName: false).Should().Be(value, $"{context} row {rows}, column {column}");
                    Get(row, column, byName: true).Should().Be(value, $"{context} row {rows}, column {column} by name");
                }

                rows++;
            }
        }

        pool.AssertAllReturned(context);
        rows.Should().Be(6, context);
    }

    [TestMethod]
    [DynamicData(nameof(BufferSizesAndCompletions))]
    public Task OneByteAtATime(int bufferSize, Completion completion) =>
        VerifyAsync(ChunkedStream.OneByte(Payloads.Representative, completion), bufferSize);

    [TestMethod]
    [DataRow(Completion.Sync)]
    [DataRow(Completion.Async)]
    public async Task SplitAtEveryIndex(Completion completion)
    {
        foreach (int bufferSize in (int[])[7, 64, 4096])
        {
            for (int split = 0; split <= Payloads.Representative.Length; split++)
            {
                await VerifyAsync(ChunkedStream.SplitAt(Payloads.Representative, completion, split), bufferSize, $"buffer {bufferSize}, split {split}");
            }
        }
    }

    [TestMethod]
    public async Task RowOfTheWrongLength_FailsOnlyItsMissingValues_AndLeavesTheReaderUsable()
    {
        TrackingArrayPool pool = new();
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, "[[1],[],[1,2,3,4],[5,6,7]]", LongColumns)));
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.OneByte(payload), pool, 1).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();

        (await rows.MoveNextAsync()).Should().BeTrue();
        ((Action)(() => rows.Current.GetValue<long>(2))).Should().ThrowExactly<JsonException>().WithMessage("Row has 1 values but the table has 3 columns.");
        rows.Current.GetValue<long>(0).Should().Be(1);
        ((Action)(() => rows.Current.GetValue<long>("b"))).Should().ThrowExactly<JsonException>().WithMessage("Row has 1 values but the table has 3 columns.");
        ((Action)(() => rows.Current.GetValue<object>(1))).Should().ThrowExactly<JsonException>();
        ((Action)(() => rows.Current.GetValue<object>("c"))).Should().ThrowExactly<JsonException>();
        ((Action)(() => rows.Current.ToValues())).Should().ThrowExactly<JsonException>().WithMessage("Row has 1 values but the table has 3 columns.");
        rows.Current.GetValue<long>("a").Should().Be(1);
        KustoTableReaderLifecycleTests.Text(rows.Current).Should().Be("[1]");

        (await rows.MoveNextAsync()).Should().BeTrue();
        ((Action)(() => rows.Current.GetValue<long>(0))).Should().ThrowExactly<JsonException>().WithMessage("Row has 0 values but the table has 3 columns.");

        (await rows.MoveNextAsync()).Should().BeTrue();
        rows.Current.GetValue<long>(2).Should().Be(3);
        rows.Current.GetValue<long>(0).Should().Be(1);
        rows.Current.GetValue<object>(1).Should().Be(2L);
        ((Action)(() => rows.Current.ToValues())).Should().ThrowExactly<JsonException>();

        (await rows.MoveNextAsync()).Should().BeTrue();
        rows.Current.GetValue<long>(1).Should().Be(6);
        rows.Current.ToValues().Should().Equal(5L, 6L, 7L);

        (await rows.MoveNextAsync()).Should().BeFalse();
        (await tables.MoveNextAsync()).Should().BeFalse();
        pool.AssertAllReturned();
    }

    [TestMethod]
    public async Task ConversionFailure_LeavesTheReaderUsable()
    {
        TrackingArrayPool pool = new();
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, """[["x",2,3],[4,5,6]]""", LongColumns)));
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Whole(payload), pool, 16).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();

        ((Action)(() => rows.Current.GetValue<long>(0))).Should().ThrowExactly<JsonException>().WithMessage("*'a'*");
        ((Action)(() => rows.Current.GetValue<object>("a"))).Should().ThrowExactly<JsonException>().WithMessage("*'a'*");
        ((Action)(() => rows.Current.GetValue<int>(1))).Should().ThrowExactly<InvalidCastException>().WithMessage("Column 'b' (Long) cannot be read as System.Int32.");
        ((Action)(() => rows.Current.GetValue<string>("c"))).Should().ThrowExactly<InvalidCastException>().WithMessage("Column 'c' (Long) cannot be read as System.String.");

        rows.Current.GetValue<long>(1).Should().Be(2);
        KustoTableReaderLifecycleTests.Text(rows.Current).Should().Be("""["x",2,3]""");
        (await rows.MoveNextAsync()).Should().BeTrue();
        rows.Current.GetValue<long>("a").Should().Be(4);

        (await rows.MoveNextAsync()).Should().BeFalse();
        (await tables.MoveNextAsync()).Should().BeFalse();
        pool.AssertAllReturned();
    }

    [TestMethod]
    public async Task ExpiredRow_IsReportedBeforeAColumnThatDoesNotExist()
    {
        TrackingArrayPool pool = new();
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, "[[1,2,3],[4,5,6]]", LongColumns)));
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.OneByte(payload), pool, 1).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();
        KustoRow expired = rows.Current;
        expired.GetValue<long>(1).Should().Be(2);
        (await rows.MoveNextAsync()).Should().BeTrue();

        foreach (KustoRow row in (KustoRow[])[expired, default])
        {
            ((Action)(() => row.GetValue<long>(1))).Should().ThrowExactly<ObjectDisposedException>();
            ((Action)(() => row.GetValue<long>(3))).Should().ThrowExactly<ObjectDisposedException>();
            ((Action)(() => row.GetValue<object>(-1))).Should().ThrowExactly<ObjectDisposedException>();
            ((Action)(() => row.GetValue<long>("nope"))).Should().ThrowExactly<ObjectDisposedException>();
            ((Action)(() => row.GetValue<object>("nope"))).Should().ThrowExactly<ObjectDisposedException>();
            ((Action)(() => row.GetValue<long>((string)null!))).Should().ThrowExactly<ObjectDisposedException>();
        }

        ((Action)(() => rows.Current.GetValue<long>((string)null!))).Should().ThrowExactly<ArgumentNullException>();
        ((Action)(() => rows.Current.GetValue<object>((string)null!))).Should().ThrowExactly<ArgumentNullException>();
        rows.Current.GetValue<long>(1).Should().Be(5);

        (await rows.MoveNextAsync()).Should().BeFalse();
        (await tables.MoveNextAsync()).Should().BeFalse();
        pool.AssertAllReturned();
    }
}

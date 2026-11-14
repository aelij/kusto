using AwesomeAssertions;
using System.Text;

namespace Kusto.Data.Queries.Tests;

[TestClass]
public class KustoTableReaderInterruptionTests
{
    private static readonly int[] BufferSizes = [1, 2, 3, 7, 16, 64];
    private static readonly Completion[] Completions = Enum.GetValues<Completion>();
    private static readonly List<ExpectedEvent> RepresentativeEvents = ResponseOracle.Expect(Payloads.Representative);
    private static readonly List<ExpectedEvent> SmallEvents = ResponseOracle.Expect(Payloads.Small);

    public static IEnumerable<object[]> BufferSizesAndCompletions =>
        from size in BufferSizes from completion in Completions select new object[] { size, completion };

    [TestMethod]
    public void Oracle_DescribesTheRepresentativePayload()
    {
        RepresentativeEvents.Texts().Should().Equal(
            "table 0 '@ExtendedProperties' QueryProperties (Key:String, Value:Dynamic)",
            """row ["a",{"x":[1,{"y":null}],"z":"]}"}]""",
            """row [ "b" , "[]{}\"\\" ]""",
            """row ["cé",[[[]],{}]]""",
            "table 1 'Empty' PrimaryResult (n:Long)",
            "table 2 'T2' PrimaryResult (i:Long, r:Real, s:String, d:Dynamic)",
            """row [1,-2.5e3,"é☃😀\t",[[[1]],{"k":[]}]]""",
            "row [null,null,null,null]",
            """row [9223372036854775807, 1E+300, "x]y}z{w[\"", {"k":"v\n","n":{"m":[true,false]}}]""");
        SmallEvents.Should().HaveCount(6);
    }

    [TestMethod]
    [DynamicData(nameof(BufferSizesAndCompletions))]
    public Task OneByteAtATime(int bufferSize, Completion completion) =>
        Harness.VerifyAsync(ChunkedStream.OneByte(Payloads.Representative, completion), bufferSize, RepresentativeEvents);

    [TestMethod]
    [DynamicData(nameof(BufferSizesAndCompletions))]
    public Task OneByteAtATime_WithBom(int bufferSize, Completion completion) =>
        Harness.VerifyAsync(ChunkedStream.OneByte([0xEF, 0xBB, 0xBF, .. Payloads.Representative], completion), bufferSize, RepresentativeEvents);

    [TestMethod]
    [DynamicData(nameof(BufferSizesAndCompletions))]
    public async Task RandomChunks(int bufferSize, Completion completion)
    {
        for (int seed = 0; seed < 20; seed++)
        {
            await Harness.VerifyAsync(ChunkedStream.Random(Payloads.Representative, seed, 1 + seed, completion), bufferSize, RepresentativeEvents, $"seed {seed}");
        }
    }

    [TestMethod]
    [DataRow(Completion.Sync)]
    [DataRow(Completion.Async)]
    public async Task SplitAtEveryIndex(Completion completion)
    {
        foreach (int bufferSize in (int[])[7, 64, 4096])
        {
            for (int split = 0; split <= Payloads.Representative.Length; split++)
            {
                await Harness.VerifyAsync(ChunkedStream.SplitAt(Payloads.Representative, completion, split), bufferSize, RepresentativeEvents, $"buffer {bufferSize}, split {split}");
            }
        }
    }

    [TestMethod]
    [DataRow(16)]
    [DataRow(4096)]
    public async Task SplitAtEveryPairOfIndices(int bufferSize)
    {
        byte[] payload = Payloads.Small;
        for (int first = 1; first < payload.Length; first++)
        {
            for (int second = first + 1; second < payload.Length; second++)
            {
                Completion completion = (first + second) % 2 == 0 ? Completion.Sync : Completion.Async;
                await Harness.VerifyAsync(ChunkedStream.SplitAt(payload, completion, first, second), bufferSize, SmallEvents, $"splits {first}, {second}");
            }
        }
    }

    [TestMethod]
    public async Task TruncatedAtEveryIndex_ThrowsJsonExceptionAfterTheCompleteItems()
    {
        byte[] payload = Payloads.Representative;
        for (int length = 0; length < payload.Length; length++)
        {
            byte[] prefix = payload[..length];
            List<string> expected = RepresentativeEvents.Prefix(length, Events.Throws<System.Text.Json.JsonException>());
            string context = $"truncated at {length}";
            await Harness.VerifyAsync(ChunkedStream.Whole(prefix), 4096, expected, context);
            await Harness.VerifyAsync(ChunkedStream.Random(prefix, length, 8, Completions[length % 3]), BufferSizes[length % BufferSizes.Length], expected, context);
        }
    }

    [TestMethod]
    public async Task TruncatedSmallPayloadAtEveryIndex_OneByteAtATime()
    {
        for (int length = 0; length < Payloads.Small.Length; length++)
        {
            await Harness.VerifyAsync(
                ChunkedStream.OneByte(Payloads.Small[..length], Completions[length % 3]),
                1 + (length % 4),
                SmallEvents.Prefix(length, Events.Throws<System.Text.Json.JsonException>()),
                $"truncated at {length}");
        }
    }

    [TestMethod]
    [DataRow(new byte[] { })]
    [DataRow(new byte[] { 0xEF })]
    [DataRow(new byte[] { 0xEF, 0xBB })]
    [DataRow(new byte[] { 0xEF, 0xBB, 0xBF })]
    [DataRow(new byte[] { 0xEF, 0xBB, 0xBF, (byte)'[' })]
    [DataRow(new byte[] { (byte)' ', (byte)'\n' })]
    public async Task TruncatedAroundTheBom_ThrowsJsonException(byte[] payload)
    {
        await Harness.VerifyAsync(ChunkedStream.Whole(payload), 64, [Events.Throws<System.Text.Json.JsonException>()]);
        await Harness.VerifyAsync(ChunkedStream.OneByte(payload, Completion.Async), 1, [Events.Throws<System.Text.Json.JsonException>()]);
    }

    [TestMethod]
    public async Task StreamFailingAfterEveryIndex_PropagatesTheIOException()
    {
        byte[] payload = Payloads.Representative;
        for (int failAt = 0; failAt <= payload.Length; failAt++)
        {
            List<string> expected = failAt == payload.Length ? RepresentativeEvents.Texts() : RepresentativeEvents.Prefix(failAt, Events.Throws<IOException>());
            await Harness.VerifyAsync(
                ChunkedStream.FailAfter(payload, failAt, _ => 1 + (failAt % 5), Completions[failAt % 3]),
                BufferSizes[failAt % BufferSizes.Length],
                expected,
                $"failing at {failAt}");
        }
    }

    [TestMethod]
    [DataRow(20 * 1024)]
    [DataRow(300 * 1024)]
    public async Task LargeStrings(int length)
    {
        StringBuilder value = new(length + 16);
        for (int i = 0; value.Length < length; i++)
        {
            value.Append(i % 97 == 0 ? "\\\"[]{}\\u00e9\\\\" : i % 89 == 0 ? "é☃😀" : "abcdefgh");
        }

        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, $"""[["{value}"],["after"]]""")));
        List<ExpectedEvent> expected = ResponseOracle.Expect(payload);
        await Harness.VerifyAsync(ChunkedStream.OneByte(payload), 1, expected, "one byte, sync");
        await Harness.VerifyAsync(ChunkedStream.Random(payload, 1, 7, Completion.Async), 16, expected, "small chunks, async");
        await Harness.VerifyAsync(ChunkedStream.Random(payload, 2, 3000, Completion.Mixed), 3, expected, "medium chunks, mixed");
        await Harness.VerifyAsync(ChunkedStream.Whole(payload), 64, expected, "whole");
    }

    [TestMethod]
    public async Task LargeNestedDynamicValue()
    {
        StringBuilder value = new();
        value.Append('[');
        for (int i = 0; i < 3000; i++)
        {
            value.Append(i == 0 ? "" : ",").Append("""{"i":""").Append(i).Append(""","a":[[[{"s":"v""").Append(i).Append("""\"]"}]],null,true,-1.5e-3],"o":{"p":{"q":{}}}}""");
        }

        value.Append(']');
        string columns = """[{"ColumnName":"d","ColumnType":"dynamic"},{"ColumnName":"s","ColumnType":"string"}]""";
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, $"[[{value},\"x\"],[{value}, \"y\"]]", columns)));
        List<ExpectedEvent> expected = ResponseOracle.Expect(payload);
        await Harness.VerifyAsync(ChunkedStream.OneByte(payload), 1, expected, "one byte, sync");
        await Harness.VerifyAsync(ChunkedStream.OneByte(payload, Completion.Async), 2, expected, "one byte, async");
        await Harness.VerifyAsync(ChunkedStream.Random(payload, 3, 50, Completion.Mixed), 7, expected, "small chunks, mixed");
    }

    [TestMethod]
    public async Task BufferGrowsByDoublingAndReturnsEveryBuffer()
    {
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, $"""[["{new string('x', 5000)}"]]""")));
        TrackingArrayPool pool = new();
        List<string> events = await Harness.ReadEventsAsync(new KustoTableReader(ChunkedStream.OneByte(payload), pool, 1));

        events.Should().Equal(ResponseOracle.Expect(payload).Texts());
        pool.RentedSizes.Should().Equal(Enumerable.Range(0, 15).Select(i => 1 << i), "the buffer doubles until the 5 KB row fits in its first half");
        pool.AssertAllReturned();
    }

    [TestMethod]
    public async Task HugeIncompleteToken_WaitsForTheUnscannedDataToDouble()
    {
        string prefix = $$"""[{{Payloads.Header}},{"FrameType":"DataTable","Columns":{{Payloads.StringColumn}},"Rows":[""";
        string token = new('x', 6000);
        byte[] payload = Harness.Utf8($"{prefix}[\"{token}\"],[\"next\"]]}}]");
        GatedStream stream = new(payload);
        TrackingArrayPool pool = new();
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(stream, pool, 64 * 1024).ReadTablesAsync().GetAsyncEnumerator();

        ValueTask<bool> moveNext = tables.MoveNextAsync();
        stream.Release(Encoding.UTF8.GetByteCount(prefix));
        (await moveNext).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();

        ValueTask<bool> row = rows.MoveNextAsync();
        stream.Release(5000);
        stream.Release(10);

        int rest = Encoding.UTF8.GetByteCount($"{token}\"],") - 5010 + 1;
        stream.Release(rest);
        row.IsCompleted.Should().BeFalse("the row is complete, but the reader waits until the unscanned data has doubled");
        stream.IsPending.Should().BeTrue();

        (await stream.CompleteAsync(row)).Should().BeTrue();
        rows.Current.UseData(static (data, _) => Encoding.UTF8.GetString(data), 0).Should().Be($"[\"{token}\"]");
        (await rows.MoveNextAsync()).Should().BeTrue();
        rows.Current.UseData(static (data, _) => Encoding.UTF8.GetString(data), 0).Should().Be("[\"next\"]");
        (await rows.MoveNextAsync()).Should().BeFalse();
        (await tables.MoveNextAsync()).Should().BeFalse();
        pool.AssertAllReturned();
    }

    [TestMethod]
    public async Task IncompleteTokenUnder4KB_IsCompletedAsSoonAsItsDataArrives()
    {
        string prefix = $$"""[{{Payloads.Header}},{"FrameType":"DataTable","Columns":{{Payloads.StringColumn}},"Rows":[""";
        string token = new('x', 3000);
        byte[] payload = Harness.Utf8($"{prefix}[\"{token}\"],[\"next\"]]}}]");
        GatedStream stream = new(payload);
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(stream, new TrackingArrayPool(), 64 * 1024).ReadTablesAsync().GetAsyncEnumerator();

        ValueTask<bool> moveNext = tables.MoveNextAsync();
        stream.Release(Encoding.UTF8.GetByteCount(prefix));
        (await moveNext).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();

        ValueTask<bool> row = rows.MoveNextAsync();
        stream.Release(2000);
        stream.Release(10);
        stream.Release(token.Length - 2010 + 4);
        row.IsCompleted.Should().BeTrue();
        (await row).Should().BeTrue();
    }

    public static IEnumerable<object[]> SampleFiles =>
        Directory.EnumerateFiles(Path.Combine(AppContext.BaseDirectory, "testdata"), "*.json").Select(f => Path.GetFileName(f)).Order().Select(f => new object[] { f });

    [TestMethod]
    [DynamicData(nameof(SampleFiles))]
    public async Task Samples_MatchTheOracle(string file)
    {
        byte[] payload = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "testdata", file));
        List<ExpectedEvent> expected = ResponseOracle.Expect(payload);
        if (file.StartsWith("Error.", StringComparison.Ordinal))
        {
            expected[^1].Text.Should().Be(Events.Throws<KustoException>());
        }
        else
        {
            expected.Should().Contain(e => e.Text.StartsWith("row ", StringComparison.Ordinal));
        }

        await Harness.VerifyAsync(ChunkedStream.Whole(payload), 64 * 1024, expected, "whole");
        if (payload.Length > 1024 * 1024)
        {
            await Harness.VerifyAsync(ChunkedStream.Random(payload, 1, 16 * 1024, Completion.Mixed), 4096, expected, "random chunks");
            await Harness.VerifyAsync(ChunkedStream.Fixed(payload, 4093, Completion.Async), 1024, expected, "fixed chunks");
        }
        else
        {
            await Harness.VerifyAsync(ChunkedStream.OneByte(payload), 1, expected, "one byte, sync");
            await Harness.VerifyAsync(ChunkedStream.OneByte(payload, Completion.Async), 16, expected, "one byte, async");
            await Harness.VerifyAsync(ChunkedStream.Random(payload, 1, 64, Completion.Mixed), 7, expected, "random chunks");
        }

        (await Harness.ReadEventsAsync(new KustoTableReader(ChunkedStream.Whole(payload)))).Should().Equal(expected.Texts(), "the public constructor");
    }

    [TestMethod]
    [DynamicData(nameof(SampleFiles))]
    public async Task Samples_ErrorFilesReportServiceErrors(string file)
    {
        byte[] payload = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "testdata", file));
        TrackingArrayPool pool = new();
        Func<Task> read = async () =>
        {
            await foreach (KustoTable table in new KustoTableReader(ChunkedStream.Random(payload, 5, 1000), pool, 64).ReadTablesAsync())
            {
                await foreach (KustoRow row in table.GetRowsAsync())
                {
                    row.ToValues().Should().HaveCount(table.Columns.Count);
                }
            }
        };

        if (file.StartsWith("Error.", StringComparison.Ordinal))
        {
            KustoException e = (await read.Should().ThrowExactlyAsync<KustoException>()).Which;
            e.Errors.Should().NotBeEmpty();
            e.Errors.Should().AllSatisfy(error => error.Code.Should().NotBeNullOrEmpty());
            e.Message.Should().Be(e.Errors[0].Description);
        }
        else
        {
            await read.Should().NotThrowAsync();
        }

        pool.AssertAllReturned();
    }
}

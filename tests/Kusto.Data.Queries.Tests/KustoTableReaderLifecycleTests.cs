using System.Text;
using System.Text.Json;
using AwesomeAssertions;

namespace Kusto.Data.Queries.Tests;

[TestClass]
public class KustoTableReaderLifecycleTests
{
    internal static readonly byte[] TwoTables = Harness.Utf8(Payloads.Response(
        Payloads.Table(1, """[["a"],["b"],["c"]]""", name: "T1"),
        Payloads.Table(2, """[["d"],["e"]]""", name: "T2")));

    private static readonly List<string> TwoTablesEvents = ResponseOracle.Expect(TwoTables).Texts();

    internal static string Text(KustoRow row) => row.UseData(static (data, _) => Encoding.UTF8.GetString(data), 0);

    /// <summary>The offset just after the <c>[</c> that opens the rows of the <paramref name="n"/>th table.</summary>
    internal static int RowsOffset(byte[] payload, int n = 1)
    {
        string text = Encoding.UTF8.GetString(payload);
        int index = -1;
        for (int i = 0; i < n; i++)
        {
            index = text.IndexOf("\"Rows\":[", index + 1, StringComparison.Ordinal);
        }

        return Encoding.UTF8.GetByteCount(text[..index]) + "\"Rows\":[".Length;
    }

    /// <summary>Reads the remaining rows of the current table (through <paramref name="rows"/> if given) and all later tables.</summary>
    internal static async Task<List<string>> RestAsync(IAsyncEnumerator<KustoTable> tables, IAsyncEnumerator<KustoRow>? rows = null)
    {
        List<string> events = [];
        rows ??= tables.Current.GetRowsAsync().GetAsyncEnumerator();
        while (true)
        {
            while (await rows.MoveNextAsync())
            {
                events.Add(rows.Current.UseData(static (data, _) => Events.Row(data), 0));
            }

            await rows.DisposeAsync();
            if (!await tables.MoveNextAsync())
            {
                return events;
            }

            KustoTable table = tables.Current;
            events.Add(Events.Table(table.Id, table.Name, table.Kind, table.Columns));
            rows = table.GetRowsAsync().GetAsyncEnumerator();
        }
    }

    internal static void ShouldBeDisposed(KustoRow row)
    {
        ((Action)(() => row.UseData(static (_, _) => 0, 0))).Should().ThrowExactly<ObjectDisposedException>();
        ((Action)(() => row.UseData(static (_, _) => { }, 0))).Should().ThrowExactly<ObjectDisposedException>();
        ((Action)(() => row.ToValues())).Should().ThrowExactly<ObjectDisposedException>();
        ((Action)(() => row.ToDocument())).Should().ThrowExactly<ObjectDisposedException>();
    }

    internal static void ShouldNotBePositioned(IAsyncEnumerator<KustoRow> rows) =>
        ((Func<KustoTable>)(() => rows.Current.Table)).Should().ThrowExactly<ObjectDisposedException>();

    [TestMethod]
    public void Constructor_NullStream_ThrowsArgumentNullException()
    {
        ((Action)(() => _ = new KustoTableReader(null!))).Should().ThrowExactly<ArgumentNullException>().WithParameterName("stream");
        ((Action)(() => _ = new KustoTableReader(null!, new TrackingArrayPool(), 16))).Should().ThrowExactly<ArgumentNullException>().WithParameterName("stream");
    }

    [TestMethod]
    public async Task Reading_UsesOnlyReadAsyncAndLeavesTheStreamOpen()
    {
        ChunkedStream stream = ChunkedStream.Random(TwoTables, 1, 9);
        (await Harness.ReadEventsAsync(new KustoTableReader(stream))).Should().Equal(TwoTablesEvents);
        stream.IsDisposed.Should().BeFalse();
        stream.ReadCount.Should().BePositive();
    }

    [TestMethod]
    public async Task Buffer_IsRentedOnTheFirstRead()
    {
        TrackingArrayPool pool = new();
        KustoTableReader reader = new(ChunkedStream.Whole(TwoTables), pool, 4096);
        await using IAsyncEnumerator<KustoTable> tables = reader.ReadTablesAsync().GetAsyncEnumerator();
        pool.RentedSizes.Should().BeEmpty();

        (await tables.MoveNextAsync()).Should().BeTrue();
        pool.RentedSizes.Should().Equal(4096);
        pool.Outstanding.Should().Be(1);
    }

    [TestMethod]
    public async Task Buffer_IsNeverRentedWhenDisposedBeforeReading()
    {
        TrackingArrayPool pool = new();
        IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Whole(TwoTables), pool, 16).ReadTablesAsync().GetAsyncEnumerator();
        await tables.DisposeAsync();
        (await tables.MoveNextAsync()).Should().BeFalse();
        await tables.DisposeAsync();
        pool.RentedSizes.Should().BeEmpty();
    }

    [TestMethod]
    public async Task Buffer_IsReturnedWhenTheTablesEnd_BeforeDisposal()
    {
        TrackingArrayPool pool = new();
        IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Random(TwoTables, 2, 5), pool, 2).ReadTablesAsync().GetAsyncEnumerator();
        (await RestAsync(tables, AsyncEnumerable.Empty<KustoRow>().GetAsyncEnumerator())).Should().Equal(TwoTablesEvents);
        pool.AssertAllReturned();

        (await tables.MoveNextAsync()).Should().BeFalse();
        await tables.DisposeAsync();
        (await tables.MoveNextAsync()).Should().BeFalse();
        pool.AssertAllReturned();
    }

    [TestMethod]
    public async Task Buffer_IsReturnedWhenTheTableEnumeratorIsDisposed()
    {
        TrackingArrayPool pool = new();
        IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.OneByte(TwoTables), pool, 4).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();
        KustoRow row = rows.Current;

        await tables.DisposeAsync();

        pool.AssertAllReturned();
        ShouldBeDisposed(row);
        ShouldNotBePositioned(rows);
        (await rows.MoveNextAsync()).Should().BeFalse();
        (await tables.MoveNextAsync()).Should().BeFalse();
        await rows.DisposeAsync();
        await tables.DisposeAsync();
        pool.AssertAllReturned();
    }

    [TestMethod]
    public async Task BreakingOutOfTheTablesLoop_ReturnsTheBuffer()
    {
        TrackingArrayPool pool = new();
        await foreach (KustoTable table in new KustoTableReader(ChunkedStream.OneByte(TwoTables), pool, 4).ReadTablesAsync())
        {
            await foreach (KustoRow row in table.GetRowsAsync())
            {
                Text(row).Should().Be("[\"a\"]");
                break;
            }

            break;
        }

        pool.AssertAllReturned();
    }

    [TestMethod]
    public async Task BreakingOutOfTheRowsLoop_SkipsTheRestOfTheTable()
    {
        List<string> rows = [];
        await foreach (KustoTable table in new KustoTableReader(ChunkedStream.OneByte(TwoTables, Completion.Mixed), new TrackingArrayPool(), 1).ReadTablesAsync())
        {
            await foreach (KustoRow row in table.GetRowsAsync())
            {
                rows.Add(Text(row));
                break;
            }
        }

        rows.Should().Equal("[\"a\"]", "[\"d\"]");
    }

    [TestMethod]
    public async Task ReadTablesAsync_CanBeEnumeratedOnce()
    {
        KustoTableReader reader = new(ChunkedStream.Whole(TwoTables), new TrackingArrayPool(), 16);
        IAsyncEnumerable<KustoTable> tables = reader.ReadTablesAsync();
        await using IAsyncEnumerator<KustoTable> enumerator = tables.GetAsyncEnumerator();

        ((Action)(() => tables.GetAsyncEnumerator())).Should().ThrowExactly<InvalidOperationException>();
        ((Action)(() => reader.ReadTablesAsync().GetAsyncEnumerator())).Should().ThrowExactly<InvalidOperationException>();
        ((Action)(() => reader.ReadTablesAsync(new CancellationTokenSource().Token).GetAsyncEnumerator(new CancellationTokenSource().Token)))
            .Should().ThrowExactly<InvalidOperationException>();

        (await RestAsync(enumerator, AsyncEnumerable.Empty<KustoRow>().GetAsyncEnumerator())).Should().Equal(TwoTablesEvents);
        ((Action)(() => reader.ReadTablesAsync().GetAsyncEnumerator())).Should().ThrowExactly<InvalidOperationException>();
    }

    [TestMethod]
    public async Task Tables_ArriveInOrderWithTheirMetadata()
    {
        string columns = """
            [{"ColumnName":"b","ColumnType":"bool"},{"ColumnName":"dt","ColumnType":"datetime"},{"ColumnName":"dyn","ColumnType":"dynamic"},
             {"ColumnName":"g","ColumnType":"guid"},{"ColumnName":"i","ColumnType":"int"},{"ColumnName":"l","ColumnType":"long"},
             {"ColumnName":"r","ColumnType":"real"},{"ColumnName":"s","ColumnType":"string"},{"ColumnName":"ts","ColumnType":"timespan"},
             {"ColumnName":"dec","ColumnType":"decimal"},{"ColumnName":"x","ColumnType":"TimeSpan"}]
            """;
        byte[] payload = Harness.Utf8(Payloads.Response(
            Payloads.Table(7, "[]", columns, "QueryCompletionInformation", "Info"),
            """{"FrameType":"DataTable","Columns":[],"Rows":[]}""",
            Payloads.Table(-3, "[]", "[]", "Bogus", "")));
        List<KustoTable> tables = [];
        await foreach (KustoTable table in new KustoTableReader(ChunkedStream.OneByte(payload), new TrackingArrayPool(), 3).ReadTablesAsync())
        {
            tables.Add(table);
        }

        tables.Select(t => (t.Id, t.Name, t.Kind, t.Columns.Count)).Should().Equal(
            (7, "Info", KustoTableKind.QueryCompletionInformation, 11),
            (0, "", KustoTableKind.Unknown, 0),
            (-3, "", KustoTableKind.Unknown, 0));
        tables[0].Columns.Should().Equal(
            new KustoColumn("b", KustoColumnType.Bool),
            new KustoColumn("dt", KustoColumnType.DateTime),
            new KustoColumn("dyn", KustoColumnType.Dynamic),
            new KustoColumn("g", KustoColumnType.Guid),
            new KustoColumn("i", KustoColumnType.Int),
            new KustoColumn("l", KustoColumnType.Long),
            new KustoColumn("r", KustoColumnType.Real),
            new KustoColumn("s", KustoColumnType.String),
            new KustoColumn("ts", KustoColumnType.TimeSpan),
            new KustoColumn("dec", KustoColumnType.Decimal),
            new KustoColumn("x", KustoColumnType.TimeSpan));
    }

    public static IEnumerable<object[]> KnownTableKinds => Enum.GetValues<KustoTableKind>().Select(kind => new object[] { kind });

    [TestMethod]
    [DynamicData(nameof(KnownTableKinds))]
    public async Task TableKind_IsParsed(KustoTableKind kind)
    {
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, "[]", kind: kind.ToString())));
        await foreach (KustoTable table in new KustoTableReader(ChunkedStream.Whole(payload)).ReadTablesAsync())
        {
            table.Kind.Should().Be(kind);
        }
    }

    [TestMethod]
    [DataRow("\"Bogus\"")]
    [DataRow("\"\"")]
    [DataRow("null")]
    [DataRow("\"primaryresult\"")]
    [DataRow("\"42\"", DisplayName = "numeric TableKind")]
    [DataRow("\"1\"", DisplayName = "numeric TableKind of a defined value")]
    [DataRow("\"PrimaryResult,QueryPlan\"", DisplayName = "comma-separated TableKind")]
    public async Task TableKind_Unrecognised_IsUnknown(string json)
    {
        byte[] payload = Harness.Utf8($$"""[{"FrameType":"DataTable","TableKind":{{json}},"Columns":[],"Rows":[]}]""");
        int count = 0;
        await foreach (KustoTable table in new KustoTableReader(ChunkedStream.Whole(payload)).ReadTablesAsync())
        {
            table.Kind.Should().Be(KustoTableKind.Unknown);
            count++;
        }

        count.Should().Be(1);
    }

    [TestMethod]
    public async Task GetRowsAsync_CanBeEnumeratedOnce()
    {
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Whole(TwoTables), new TrackingArrayPool(), 16).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        IAsyncEnumerable<KustoRow> rows = tables.Current.GetRowsAsync();
        await using IAsyncEnumerator<KustoRow> enumerator = rows.GetAsyncEnumerator();

        ((Action)(() => rows.GetAsyncEnumerator())).Should().ThrowExactly<InvalidOperationException>();
        ((Action)(() => tables.Current.GetRowsAsync().GetAsyncEnumerator())).Should().ThrowExactly<InvalidOperationException>();
        (await enumerator.MoveNextAsync()).Should().BeTrue();
        Text(enumerator.Current).Should().Be("[\"a\"]");
    }

    [TestMethod]
    public async Task GetRowsAsync_AfterTheReaderMovedPastTheTable_Throws()
    {
        List<KustoTable> tables = [];
        await foreach (KustoTable table in new KustoTableReader(ChunkedStream.Whole(TwoTables), new TrackingArrayPool(), 16).ReadTablesAsync())
        {
            tables.Add(table);
        }

        tables.Should().HaveCount(2);
        foreach (KustoTable table in tables)
        {
            ((Action)(() => table.GetRowsAsync().GetAsyncEnumerator())).Should().ThrowExactly<InvalidOperationException>();
        }
    }

    [TestMethod]
    public async Task GetRowsAsync_AfterTheTableWasFullyRead_Throws()
    {
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Whole(TwoTables), new TrackingArrayPool(), 16).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        KustoTable first = tables.Current;
        await foreach (KustoRow _ in first.GetRowsAsync())
        {
        }

        (await tables.MoveNextAsync()).Should().BeTrue();
        ((Action)(() => first.GetRowsAsync().GetAsyncEnumerator())).Should().ThrowExactly<InvalidOperationException>();
    }

    [TestMethod]
    public async Task UnreadRows_AreSkipped()
    {
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.OneByte(TwoTables), new TrackingArrayPool(), 1).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();
        KustoRow row = rows.Current;

        (await tables.MoveNextAsync()).Should().BeTrue();

        ShouldBeDisposed(row);
        ShouldNotBePositioned(rows);
        (await rows.MoveNextAsync()).Should().BeFalse();
        tables.Current.Name.Should().Be("T2");
        (await RestAsync(tables)).Should().Equal(TwoTablesEvents[^2..]);
    }

    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    [DataRow(2)]
    public async Task ErrorInSkippedRows_SurfacesFromTheTableEnumerator(int rowsRead)
    {
        byte[] payload = Harness.Utf8(Payloads.Response(
            Payloads.Table(1, """[["a"],["b"],{"OneApiErrors":[{"error":{"code":"LimitsExceeded","message":"m"}}]},["c"]]"""),
            Payloads.Table(2, """[["d"]]""")));
        TrackingArrayPool pool = new();
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.OneByte(payload, Completion.Mixed), pool, 2).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        for (int i = 0; i < rowsRead; i++)
        {
            (await rows.MoveNextAsync()).Should().BeTrue();
        }

        (await ((Func<Task>)(async () => await tables.MoveNextAsync())).Should().ThrowExactlyAsync<KustoException>())
            .Which.Errors.Should().ContainSingle().Which.Code.Should().Be("LimitsExceeded");
        pool.AssertAllReturned();
        (await tables.MoveNextAsync()).Should().BeFalse();
        (await rows.MoveNextAsync()).Should().BeFalse();
    }

    [TestMethod]
    public async Task StaleRowEnumerator_ReturnsFalseAndLeavesTheCurrentRowValid()
    {
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Whole(TwoTables), new TrackingArrayPool(), 16).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> stale = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();

        (await stale.MoveNextAsync()).Should().BeFalse();
        ShouldNotBePositioned(stale);
        await stale.DisposeAsync();

        Text(rows.Current).Should().Be("[\"d\"]");
        (await RestAsync(tables, rows)).Should().Equal("row [\"e\"]");
    }

    [TestMethod]
    public async Task RowEnumerator_AfterItsDisposal_ReturnsFalse()
    {
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Whole(TwoTables), new TrackingArrayPool(), 16).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();
        KustoRow row = rows.Current;

        await rows.DisposeAsync();

        ShouldBeDisposed(row);
        ShouldNotBePositioned(rows);
        (await rows.MoveNextAsync()).Should().BeFalse();
        await rows.DisposeAsync();
        (await RestAsync(tables, rows)).Should().Equal(TwoTablesEvents[^3..]);
    }

    [TestMethod]
    public async Task TableEnumerator_AfterTheEnd_KeepsReturningFalse()
    {
        TrackingArrayPool pool = new();
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Whole(TwoTables), pool, 16).ReadTablesAsync().GetAsyncEnumerator();
        (await RestAsync(tables, AsyncEnumerable.Empty<KustoRow>().GetAsyncEnumerator())).Should().HaveCount(7);
        (await tables.MoveNextAsync()).Should().BeFalse();
        (await tables.MoveNextAsync()).Should().BeFalse();
        pool.AssertAllReturned();
    }

    [TestMethod]
    public async Task Row_IsValidUntilTheNextMoveNext()
    {
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.OneByte(TwoTables), new TrackingArrayPool(), 1).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();
        KustoRow first = rows.Current;
        Text(first).Should().Be("[\"a\"]");
        Text(rows.Current).Should().Be("[\"a\"]");
        first.Table.Should().BeSameAs(tables.Current);

        (await rows.MoveNextAsync()).Should().BeTrue();
        ShouldBeDisposed(first);
        first.Table.Should().BeSameAs(tables.Current);
        Text(rows.Current).Should().Be("[\"b\"]");

        KustoRow second = rows.Current;
        (await rows.MoveNextAsync()).Should().BeTrue();
        (await rows.MoveNextAsync()).Should().BeFalse();
        ShouldBeDisposed(second);
        ShouldNotBePositioned(rows);
    }

    [TestMethod]
    public async Task Row_IsInvalidatedWhenTheReaderFails()
    {
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, """[["a"],5]""")));
        TrackingArrayPool pool = new();
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Whole(payload), pool, 1024).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();
        KustoRow row = rows.Current;

        (await ((Func<Task>)(async () => await rows.MoveNextAsync())).Should().ThrowAsync<JsonException>()).Which.ShouldBeJsonException();

        ShouldBeDisposed(row);
        ShouldNotBePositioned(rows);
        pool.AssertAllReturned();
    }

    [TestMethod]
    public void DefaultRow_ThrowsObjectDisposedExceptionFromEveryMember()
    {
        KustoRow row = default;
        ((Func<KustoTable>)(() => row.Table)).Should().ThrowExactly<ObjectDisposedException>();
        ShouldBeDisposed(row);
    }

    [TestMethod]
    public async Task Current_IsDefaultWhenTheRowEnumeratorIsNotOnARow()
    {
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Whole(TwoTables), new TrackingArrayPool(), 16).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        ShouldNotBePositioned(rows);
        while (await rows.MoveNextAsync())
        {
            rows.Current.Table.Should().BeSameAs(tables.Current);
        }

        ShouldNotBePositioned(rows);
    }

    [TestMethod]
    public async Task UseData_PassesExactlyTheBytesOfTheRow()
    {
        string first = "[ 1 ,\n \"a\\\"]\" ,\t{ \"x\" : [ ] } , null ]";
        string second = "[2,\"é☃\",[],\"\\u00e9\"]";
        string columns = """[{"ColumnName":"i","ColumnType":"int"},{"ColumnName":"s","ColumnType":"string"},{"ColumnName":"d","ColumnType":"dynamic"},{"ColumnName":"n","ColumnType":"string"}]""";
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, $"[ \n{first} ,\r\n\t{second}\n]", columns)));
        List<string> actual = [];
        await foreach (KustoTable table in new KustoTableReader(ChunkedStream.OneByte(payload), new TrackingArrayPool(), 1).ReadTablesAsync())
        {
            await foreach (KustoRow row in table.GetRowsAsync())
            {
                row.UseData(static (data, list) => list.Add(Encoding.UTF8.GetString(data)), actual);
                actual.Add(row.UseData(static (data, _) => Convert.ToHexString(data), 0));
            }
        }

        actual.Should().Equal(first, Convert.ToHexString(Encoding.UTF8.GetBytes(first)), second, Convert.ToHexString(Encoding.UTF8.GetBytes(second)));
    }

    [TestMethod]
    public async Task UseData_NullCallback_ThrowsArgumentNullException()
    {
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Whole(TwoTables), new TrackingArrayPool(), 16).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();
        KustoRow row = rows.Current;

        ((Action)(() => row.UseData<int>(null!, 0))).Should().ThrowExactly<ArgumentNullException>();
        ((Action)(() => row.UseData<int, int>(null!, 0))).Should().ThrowExactly<ArgumentNullException>();
        Text(row).Should().Be("[\"a\"]");
    }

    [TestMethod]
    public async Task UseData_CallbackException_PropagatesAndReleasesTheGuard()
    {
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Whole(TwoTables), new TrackingArrayPool(), 16).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();
        FormatException thrown = new();

        ((Action)(() => rows.Current.UseData<FormatException>(static (_, e) => throw e, thrown))).Should().Throw<FormatException>().Which.Should().BeSameAs(thrown);

        Text(rows.Current).Should().Be("[\"a\"]");
        (await RestAsync(tables, rows)).Should().Equal(TwoTablesEvents[2..]);
    }

    [TestMethod]
    public async Task ToDocument_IsAStandaloneCopy()
    {
        TrackingArrayPool pool = new();
        string row = """[ {"a" : [1, "x\"]"]} , "é" ]""";
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, $"[{row},[{{}},\"y\"]]", """[{"ColumnName":"d","ColumnType":"dynamic"},{"ColumnName":"s","ColumnType":"string"}]""")));
        List<JsonDocument> documents = [];
        await foreach (KustoTable table in new KustoTableReader(ChunkedStream.OneByte(payload), pool, 1).ReadTablesAsync())
        {
            await foreach (KustoRow r in table.GetRowsAsync())
            {
                documents.Add(r.ToDocument());
            }
        }

        pool.AssertAllReturned();
        documents.Should().HaveCount(2);
        documents[0].RootElement.GetRawText().Should().Be(row);
        documents[0].RootElement[0].GetProperty("a")[1].GetString().Should().Be("x\"]");
        documents[0].RootElement[1].GetString().Should().Be("é");
        documents[1].RootElement.GetRawText().Should().Be("[{},\"y\"]");
    }

    [TestMethod]
    public async Task ToValues_ConvertsALiveRowAndTheValuesOutliveIt()
    {
        TrackingArrayPool pool = new();
        string columns = """[{"ColumnName":"d","ColumnType":"dynamic"},{"ColumnName":"s","ColumnType":"string"},{"ColumnName":"l","ColumnType":"long"},{"ColumnName":"b","ColumnType":"bool"}]""";
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, """[[{"a":[1,"x"]},"é☃",-5,true],[null,null,null,null]]""", columns)));
        List<object?[]> values = [];
        await foreach (KustoTable table in new KustoTableReader(ChunkedStream.OneByte(payload, Completion.Async), pool, 1).ReadTablesAsync())
        {
            await foreach (KustoRow row in table.GetRowsAsync())
            {
                values.Add(row.ToValues());
            }
        }

        pool.AssertAllReturned();
        values.Should().HaveCount(2);
        ((JsonElement)values[0][0]!).GetProperty("a")[1].GetString().Should().Be("x");
        values[0][1..].Should().Equal("é☃", -5L, true);
        values[1].Should().Equal(new object?[] { null, null, null, null });
    }

    [TestMethod]
    public async Task GetValue_ConvertsEachColumnAndHandlesNulls()
    {
        string columns = """[{"ColumnName":"d","ColumnType":"dynamic"},{"ColumnName":"s","ColumnType":"string"},{"ColumnName":"l","ColumnType":"long"}]""";
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, """[[{"a":[1,"x"]},"é☃",-5],[null,null,null]]""", columns)));
        TrackingArrayPool pool = new();
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.OneByte(payload), pool, 1).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        tables.Current.GetColumnIndex("l").Should().Be(2);
        ((Action)(() => tables.Current.GetColumnIndex("L"))).Should().ThrowExactly<ArgumentException>();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();

        (await rows.MoveNextAsync()).Should().BeTrue();
        rows.Current.GetValue<long>(2).Should().Be(-5);
        rows.Current.GetValue<string>("s").Should().Be("é☃");
        rows.Current.GetValue<JsonElement>(0)!.Value.GetProperty("a")[1].GetString().Should().Be("x");
        rows.Current.GetValue<long>("l", 7).Should().Be(-5);
        rows.Current.GetValue<object>(2).Should().Be(-5L);
        rows.Current.GetValue<string>(1, "d").Should().Be("é☃");
        ((Action)(() => rows.Current.GetValue<int>(2))).Should().ThrowExactly<InvalidCastException>();
        ((Action)(() => rows.Current.GetValue<string>(2))).Should().ThrowExactly<InvalidCastException>();
        ((Action)(() => rows.Current.GetValue<long>(3))).Should().ThrowExactly<ArgumentOutOfRangeException>();
        ((Action)(() => rows.Current.GetValue<long>(-1))).Should().ThrowExactly<ArgumentOutOfRangeException>();
        ((Action)(() => rows.Current.GetValue<string>(3))).Should().ThrowExactly<ArgumentOutOfRangeException>();
        ((Action)(() => rows.Current.GetValue<long>("x"))).Should().ThrowExactly<ArgumentException>();
        ((Action)(() => rows.Current.GetValue<string>("x"))).Should().ThrowExactly<ArgumentException>();

        (await rows.MoveNextAsync()).Should().BeTrue();
        rows.Current.GetValue<long>(2).Should().BeNull();
        rows.Current.GetValue<long>(2, 7).Should().Be(7);
        rows.Current.GetValue<long>("l", 7).Should().Be(7);
        rows.Current.GetValue<JsonElement>(0).Should().BeNull();
        rows.Current.GetValue<string>(1).Should().BeNull();
        rows.Current.GetValue<string>("s", "d").Should().Be("d");
        rows.Current.GetValue<object>(2).Should().BeNull();

        (await rows.MoveNextAsync()).Should().BeFalse();
        (await tables.MoveNextAsync()).Should().BeFalse();
        pool.AssertAllReturned();
    }

    [TestMethod]
    public async Task ToValues_ConversionFailure_LeavesTheReaderUsable()
    {
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, """[[1],["ok"]]""")));
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Whole(payload), new TrackingArrayPool(), 16).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();

        ((Action)(() => rows.Current.ToValues())).Should().ThrowExactly<JsonException>();

        Text(rows.Current).Should().Be("[1]");
        (await rows.MoveNextAsync()).Should().BeTrue();
        rows.Current.ToValues().Should().Equal("ok");
    }
}

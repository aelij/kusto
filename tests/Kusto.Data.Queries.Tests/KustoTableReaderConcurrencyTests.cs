using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using AwesomeAssertions;
using static Kusto.Data.Queries.Tests.KustoTableReaderLifecycleTests;

namespace Kusto.Data.Queries.Tests;

[TestClass]
public class KustoTableReaderConcurrencyTests
{
    private static readonly List<string> TwoTablesEvents = ResponseOracle.Expect(TwoTables).Texts();

    private static void ShouldThrowSynchronously(Func<ValueTask<bool>> call) =>
        ((Action)(() => call())).Should().ThrowExactly<InvalidOperationException>();

    private static void ShouldThrowSynchronously(Func<ValueTask> call) =>
        ((Action)(() => call())).Should().ThrowExactly<InvalidOperationException>();

    private static Exception? Catch(Action action)
    {
        try
        {
            action();
            return null;
        }
        catch (Exception e)
        {
            return e;
        }
    }

    private static async Task<(GatedStream Stream, TrackingArrayPool Pool, IAsyncEnumerator<KustoTable> Tables)> OnFirstTableAsync(
        byte[] payload, CancellationToken cancellationToken = default)
    {
        GatedStream stream = new(payload);
        TrackingArrayPool pool = new();
        IAsyncEnumerator<KustoTable> tables = new KustoTableReader(stream, pool, 64 * 1024).ReadTablesAsync(cancellationToken).GetAsyncEnumerator();
        ValueTask<bool> first = tables.MoveNextAsync();
        stream.Release(RowsOffset(payload));
        (await first).Should().BeTrue();
        stream.IsPending.Should().BeFalse();
        return (stream, pool, tables);
    }

    private static async Task<(GatedStream Stream, TrackingArrayPool Pool, IAsyncEnumerator<KustoTable> Tables, IAsyncEnumerator<KustoRow> Rows)> OnFirstRowAsync(
        CancellationToken cancellationToken = default)
    {
        (GatedStream stream, TrackingArrayPool pool, IAsyncEnumerator<KustoTable> tables) = await OnFirstTableAsync(TwoTables, cancellationToken);
        IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        ValueTask<bool> first = rows.MoveNextAsync();
        stream.Release(4);
        first.IsCompleted.Should().BeFalse();
        stream.Release(1);
        (await first).Should().BeTrue();
        Text(rows.Current).Should().Be("[\"a\"]");
        return (stream, pool, tables, rows);
    }

    [TestMethod]
    public async Task TablesMoveNextOrDispose_WhileTablesMoveNextIsPending_ThrowsWithoutDisturbingIt()
    {
        GatedStream stream = new(TwoTables);
        TrackingArrayPool pool = new();
        IAsyncEnumerator<KustoTable> tables = new KustoTableReader(stream, pool, 64 * 1024).ReadTablesAsync().GetAsyncEnumerator();
        ValueTask<bool> pending = tables.MoveNextAsync();
        pending.IsCompleted.Should().BeFalse();

        ShouldThrowSynchronously(tables.MoveNextAsync);
        ShouldThrowSynchronously(tables.DisposeAsync);
        stream.Release(5);
        pending.IsCompleted.Should().BeFalse();
        ShouldThrowSynchronously(tables.MoveNextAsync);

        stream.Open();
        (await pending).Should().BeTrue();
        tables.Current.Name.Should().Be("T1");
        (await RestAsync(tables)).Should().Equal(TwoTablesEvents[1..]);
        await tables.DisposeAsync();
        pool.AssertAllReturned();
    }

    [TestMethod]
    public async Task Operations_WhileRowsMoveNextIsPending_ThrowWithoutDisturbingIt()
    {
        (GatedStream stream, TrackingArrayPool pool, IAsyncEnumerator<KustoTable> tables, IAsyncEnumerator<KustoRow> rows) = await OnFirstRowAsync();
        KustoRow previous = rows.Current;
        ValueTask<bool> pending = rows.MoveNextAsync();
        pending.IsCompleted.Should().BeFalse();

        ShouldThrowSynchronously(rows.MoveNextAsync);
        ShouldThrowSynchronously(tables.MoveNextAsync);
        ShouldThrowSynchronously(tables.DisposeAsync);
        ((Action)(() => previous.UseData(static (_, _) => 0, 0))).Should().ThrowExactly<InvalidOperationException>();
        ((Action)(() => previous.ToValues())).Should().ThrowExactly<InvalidOperationException>();
        ((Action)(() => previous.ToValues(new object?[8]))).Should().ThrowExactly<InvalidOperationException>();
        ((Action)(() => previous.ToDocument())).Should().ThrowExactly<InvalidOperationException>();
        ShouldNotBePositioned(rows);

        stream.Release(3);
        pending.IsCompleted.Should().BeFalse();
        stream.Open();
        (await pending).Should().BeTrue();
        ShouldBeDisposed(previous);
        Text(rows.Current).Should().Be("[\"b\"]");
        (await RestAsync(tables, rows)).Should().Equal(TwoTablesEvents[3..]);
        await tables.DisposeAsync();
        pool.AssertAllReturned();
    }

    [TestMethod]
    public async Task RowsDispose_WhileRowsMoveNextIsPending_ThrowsWithoutSideEffects()
    {
        (GatedStream stream, _, IAsyncEnumerator<KustoTable> tables, IAsyncEnumerator<KustoRow> rows) = await OnFirstRowAsync();
        ValueTask<bool> pending = rows.MoveNextAsync();

        ShouldThrowSynchronously(rows.DisposeAsync);

        stream.Open();
        (await pending).Should().BeTrue();
        Text(rows.Current).Should().Be("[\"b\"]");
        (await RestAsync(tables, rows)).Should().Equal(TwoTablesEvents[3..], "the rejected DisposeAsync must not end the row enumeration");
    }

    [TestMethod]
    public async Task RowsMoveNext_WhileTablesMoveNextIsPending_Throws()
    {
        (GatedStream stream, TrackingArrayPool pool, IAsyncEnumerator<KustoTable> tables) = await OnFirstTableAsync(TwoTables);
        IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        ValueTask<bool> pending = tables.MoveNextAsync();
        pending.IsCompleted.Should().BeFalse();

        ShouldThrowSynchronously(rows.MoveNextAsync);
        ShouldThrowSynchronously(tables.MoveNextAsync);

        stream.Open();
        (await pending).Should().BeTrue();
        tables.Current.Name.Should().Be("T2");
        (await rows.MoveNextAsync()).Should().BeFalse();
        (await RestAsync(tables)).Should().Equal(TwoTablesEvents[5..]);
        pool.AssertAllReturned();
    }

    [TestMethod]
    public async Task Operations_WhileRowDataIsInUse_ThrowWithoutDisturbingIt()
    {
        TrackingArrayPool pool = new();
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.OneByte(TwoTables), pool, 1).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();
        KustoRow row = rows.Current;
        List<Exception?> errors = [];

        string seen = row.UseData(
            (data, _) =>
            {
                errors.Add(Catch(() => rows.MoveNextAsync()));
                errors.Add(Catch(() => tables.MoveNextAsync()));
                errors.Add(Catch(() => tables.DisposeAsync()));
                errors.Add(Catch(() => row.UseData(static (_, _) => 0, 0)));
                errors.Add(Catch(() => rows.Current.UseData(static (_, _) => { }, 0)));
                errors.Add(Catch(() => row.ToValues()));
                errors.Add(Catch(() => row.ToValues(new object?[8])));
                errors.Add(Catch(() => row.ToDocument()));
                return Encoding.UTF8.GetString(data);
            },
            0);

        seen.Should().Be("[\"a\"]");
        errors.Should().HaveCount(8).And.AllSatisfy(e => e.Should().BeOfType<InvalidOperationException>());
        Text(row).Should().Be("[\"a\"]");
        row.ToValues().Should().Equal("a");
        (await RestAsync(tables, rows)).Should().Equal(TwoTablesEvents[2..]);
        pool.AssertAllReturned();
    }

    [TestMethod]
    public async Task RowsDispose_WhileRowDataIsInUse_ThrowsWithoutSideEffects()
    {
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Whole(TwoTables), new TrackingArrayPool(), 64).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await using IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();

        Exception? error = rows.Current.UseData((_, _) => Catch(() => rows.DisposeAsync()), 0);

        error.Should().BeOfType<InvalidOperationException>();
        Text(rows.Current).Should().Be("[\"a\"]");
        (await RestAsync(tables, rows)).Should().Equal(TwoTablesEvents[2..], "the rejected DisposeAsync must not end the row enumeration");
    }

    [TestMethod]
    public async Task ConcurrentCallsFromManyThreads_AreSerializedByTheGuard()
    {
        const int RowCount = 3000, ThreadCount = 8;
        string[] expectedRows = [.. Enumerable.Range(0, RowCount).Select(i => $"[\"{i:D4}{new string('x', i % 61)}\"]")];
        Dictionary<string, int> indexOf = expectedRows.Select((row, i) => (row, i)).ToDictionary(p => p.row, p => p.i);
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, $"[{string.Join(",", expectedRows)}]")));
        TrackingArrayPool pool = new();
        IAsyncEnumerator<KustoTable> tables = new KustoTableReader(ChunkedStream.Random(payload, 7, 97), pool, 16).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();

        int advanced = 0, seen = 0, rejected = 0, disposed = 0;
        bool ended = false;
        ConcurrentQueue<string> problems = new();
        long deadline = Environment.TickCount64 + 60_000;

        void Classify(Exception e)
        {
            if (e.GetType() == typeof(InvalidOperationException))
            {
                Interlocked.Increment(ref rejected);
            }
            else if (e.GetType() == typeof(ObjectDisposedException))
            {
                Interlocked.Increment(ref disposed);
            }
            else
            {
                problems.Enqueue(e.ToString());
            }
        }

        async Task WorkAsync()
        {
            int last = -1;
            while (!Volatile.Read(ref ended) && Environment.TickCount64 < deadline)
            {
                try
                {
                    if (await rows.MoveNextAsync())
                    {
                        Interlocked.Increment(ref advanced);
                    }
                    else
                    {
                        Volatile.Write(ref ended, true);
                    }
                }
                catch (Exception e)
                {
                    Classify(e);
                }

                KustoRow row = default;
                try
                {
                    row = rows.Current;
                }
                catch (Exception e)
                {
                    Classify(e);
                }

                try
                {
                    string text = row.UseData(static (data, _) => Encoding.UTF8.GetString(data), 0);
                    Interlocked.Increment(ref seen);
                    if (!indexOf.TryGetValue(text, out int index))
                    {
                        problems.Enqueue($"Unexpected row data: {text}");
                    }
                    else if (index < last)
                    {
                        problems.Enqueue($"Row {index} was seen after row {last}.");
                    }
                    else
                    {
                        last = index;
                    }
                }
                catch (Exception e)
                {
                    Classify(e);
                }
            }
        }

        await Task.WhenAll(Enumerable.Range(0, ThreadCount).Select(_ => Task.Run(WorkAsync)));

        problems.Should().BeEmpty();
        ended.Should().BeTrue("the threads must read the rows to the end");
        advanced.Should().Be(RowCount, "every successful MoveNextAsync advances exactly one row");
        seen.Should().BePositive();
        rejected.Should().BePositive("the threads must have contended for the reader");
        await rows.DisposeAsync();
        (await tables.MoveNextAsync()).Should().BeFalse();
        await tables.DisposeAsync();
        pool.AssertAllReturned();
    }

    public static IEnumerable<object[]> RowFailures =>
        from failure in new (string Json, Type Expected)[]
        {
            ("""{"OneApiErrors":[{"error":{"code":"c","message":"m"}}]}""", typeof(KustoException)),
            ("5", typeof(JsonException)),
            ("{}", typeof(JsonException)),
            ("[1,]", typeof(JsonException)),
        }
        from chunked in new[] { false, true }
        select new object[] { failure.Json, failure.Expected, chunked };

    [TestMethod]
    [DynamicData(nameof(RowFailures))]
    public async Task AfterARowFailure_EverythingIsReleased(string json, Type expected, bool chunked)
    {
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, $"""[["a"],{json},["b"]]"""), Payloads.Table(2, """[["c"]]""")));
        TrackingArrayPool pool = new();
        TestStream stream = chunked ? ChunkedStream.OneByte(payload, Completion.Mixed) : ChunkedStream.Whole(payload);
        IAsyncEnumerator<KustoTable> tables = new KustoTableReader(stream, pool, chunked ? 2 : 4096).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();
        KustoRow row = rows.Current;

        Exception e = (await ((Func<Task>)(async () => await rows.MoveNextAsync())).Should().ThrowAsync<Exception>()).Which;

        if (expected == typeof(JsonException))
        {
            e.ShouldBeJsonException();
        }
        else
        {
            e.Should().BeOfType(expected);
        }

        pool.AssertAllReturned();
        ShouldBeDisposed(row);
        ShouldNotBePositioned(rows);
        (await rows.MoveNextAsync()).Should().BeFalse();
        (await tables.MoveNextAsync()).Should().BeFalse();
        await rows.DisposeAsync();
        await tables.DisposeAsync();
        (await tables.MoveNextAsync()).Should().BeFalse();
        pool.AssertAllReturned();
    }

    [TestMethod]
    [DataRow(Completion.Sync)]
    [DataRow(Completion.Async)]
    public async Task StreamFailure_PropagatesUnchangedAndReleasesEverything(Completion completion)
    {
        ChunkedStream stream = ChunkedStream.FailAfter(TwoTables, RowsOffset(TwoTables) + 7, _ => 3, completion);
        TrackingArrayPool pool = new();
        IAsyncEnumerator<KustoTable> tables = new KustoTableReader(stream, pool, 16).ReadTablesAsync().GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync().GetAsyncEnumerator();
        (await rows.MoveNextAsync()).Should().BeTrue();
        KustoRow row = rows.Current;

        (await ((Func<Task>)(async () => await rows.MoveNextAsync())).Should().ThrowExactlyAsync<IOException>()).Which.Should().BeSameAs(stream.Thrown);

        pool.AssertAllReturned();
        ShouldBeDisposed(row);
        (await rows.MoveNextAsync()).Should().BeFalse();
        (await tables.MoveNextAsync()).Should().BeFalse();
        await tables.DisposeAsync();
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task TablesToken_ReachesTheStream(bool viaWithCancellation)
    {
        using CancellationTokenSource cts = new();
        ChunkedStream stream = ChunkedStream.OneByte(TwoTables, Completion.Mixed);
        KustoTableReader reader = new(stream, new TrackingArrayPool(), 4);
        List<string> events = [];
        if (viaWithCancellation)
        {
            await foreach (KustoTable table in reader.ReadTablesAsync().WithCancellation(cts.Token))
            {
                events.Add(table.Name);
                await foreach (KustoRow row in table.GetRowsAsync())
                {
                    events.Add(Text(row));
                }
            }
        }
        else
        {
            await foreach (KustoTable table in reader.ReadTablesAsync(cts.Token))
            {
                events.Add(table.Name);
                await foreach (KustoRow row in table.GetRowsAsync().WithCancellation(CancellationToken.None))
                {
                    events.Add(Text(row));
                }
            }
        }

        events.Should().Equal("T1", "[\"a\"]", "[\"b\"]", "[\"c\"]", "T2", "[\"d\"]", "[\"e\"]");
        stream.Tokens.Should().HaveCount(TwoTables.Length).And.OnlyContain(token => token == cts.Token);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task RowsToken_ReachesTheStreamForRowReads(bool viaWithCancellation)
    {
        using CancellationTokenSource tablesCts = new();
        using CancellationTokenSource rowsCts = new();
        ChunkedStream stream = ChunkedStream.OneByte(TwoTables, Completion.Mixed);
        List<(int Start, int End)> rowReads = [];
        int rowCount = 0;
        await foreach (KustoTable table in new KustoTableReader(stream, new TrackingArrayPool(), 4).ReadTablesAsync(tablesCts.Token))
        {
            int start = stream.ReadCount;
            if (viaWithCancellation)
            {
                await foreach (KustoRow _ in table.GetRowsAsync().WithCancellation(rowsCts.Token))
                {
                    rowCount++;
                }
            }
            else
            {
                await foreach (KustoRow _ in table.GetRowsAsync(rowsCts.Token))
                {
                    rowCount++;
                }
            }

            rowReads.Add((start, stream.ReadCount));
        }

        rowCount.Should().Be(5);
        rowReads.Should().AllSatisfy(range => range.End.Should().BeGreaterThan(range.Start));
        for (int i = 0; i < stream.ReadCount; i++)
        {
            bool isRowRead = rowReads.Any(range => i >= range.Start && i < range.End);
            (stream.Tokens[i] == (isRowRead ? rowsCts.Token : tablesCts.Token)).Should().BeTrue($"read {i} is a {(isRowRead ? "row" : "table")} read");
        }
    }

    [TestMethod]
    public async Task Cancellation_WhileAReadIsPending_PropagatesAndReleasesEverything()
    {
        using CancellationTokenSource cts = new();
        (GatedStream stream, TrackingArrayPool pool, IAsyncEnumerator<KustoTable> tables, IAsyncEnumerator<KustoRow> rows) = await OnFirstRowAsync(cts.Token);
        KustoRow row = rows.Current;
        ValueTask<bool> pending = rows.MoveNextAsync();
        pending.IsCompleted.Should().BeFalse();

        await cts.CancelAsync();

        stream.IsPending.Should().BeFalse();
        (await ((Func<Task>)(async () => await pending)).Should().ThrowExactlyAsync<TaskCanceledException>()).Which.CancellationToken.Should().Be(cts.Token);
        pool.AssertAllReturned();
        ShouldBeDisposed(row);
        (await rows.MoveNextAsync()).Should().BeFalse();
        (await tables.MoveNextAsync()).Should().BeFalse();
        await tables.DisposeAsync();
    }

    [TestMethod]
    [DataRow(Completion.Sync)]
    [DataRow(Completion.Async)]
    public async Task Cancellation_ExceptionFromTheStreamPropagatesUnchanged(Completion completion)
    {
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();
        ChunkedStream stream = ChunkedStream.OneByte(TwoTables, completion);
        TrackingArrayPool pool = new();
        IAsyncEnumerator<KustoTable> tables = new KustoTableReader(stream, pool, 4).ReadTablesAsync(cts.Token).GetAsyncEnumerator();

        (await ((Func<Task>)(async () => await tables.MoveNextAsync())).Should().ThrowExactlyAsync<OperationCanceledException>()).Which.Should().BeSameAs(stream.Thrown);

        pool.AssertAllReturned();
        (await tables.MoveNextAsync()).Should().BeFalse();
        stream.ReadCount.Should().Be(1);
    }

    [TestMethod]
    public async Task Cancellation_IsNotObservedWhileServingBufferedData()
    {
        using CancellationTokenSource tablesCts = new();
        using CancellationTokenSource rowsCts = new();
        ChunkedStream stream = ChunkedStream.Whole(TwoTables);
        TrackingArrayPool pool = new();
        await using IAsyncEnumerator<KustoTable> tables = new KustoTableReader(stream, pool, 4096).ReadTablesAsync(tablesCts.Token).GetAsyncEnumerator();
        (await tables.MoveNextAsync()).Should().BeTrue();
        await tablesCts.CancelAsync();
        await rowsCts.CancelAsync();

        List<string> events = [];
        IAsyncEnumerator<KustoRow> rows = tables.Current.GetRowsAsync(rowsCts.Token).GetAsyncEnumerator();
        while (await rows.MoveNextAsync())
        {
            events.Add(Text(rows.Current));
        }

        events.AddRange(await RestAsync(tables, AsyncEnumerable.Empty<KustoRow>().GetAsyncEnumerator()));
        events.Should().Equal(["[\"a\"]", "[\"b\"]", "[\"c\"]", .. TwoTablesEvents[4..]]);
        stream.ReadCount.Should().Be(1);
        pool.AssertAllReturned();
    }
}

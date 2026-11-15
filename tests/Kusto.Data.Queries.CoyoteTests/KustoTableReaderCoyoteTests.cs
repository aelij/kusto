using System.Text;
using System.Text.Json;
using AwesomeAssertions;
using Kusto.Data.Queries.Tests;
using Microsoft.Coyote.Random;
using Microsoft.Coyote.Runtime;

namespace Kusto.Data.Queries.CoyoteTests;

/// <summary>
/// Explores concurrent misuse of a <see cref="KustoTableReader"/>: whatever the interleaving, the reader must reject an
/// operation that overlaps another, never hand out stale or moving data, and return each buffer exactly once.
/// </summary>
[TestClass]
public class KustoTableReaderCoyoteTests
{
    private static readonly byte[] Payload = Response("""[["a"],["b"],["c"]]""");

    private static readonly string[] FirstRows = ["[\"a\"]", "[\"b\"]", "[\"c\"]"];

    private static readonly string[] SecondRows = ["[\"d\"]", "[\"e\"]"];

    private static byte[] Response(string firstRows) => Harness.Utf8(
        $$"""[{"FrameType":"DataTable","TableId":1,"TableName":"T1","Columns":[{"ColumnName":"s","ColumnType":"string"}],"Rows":{{firstRows}}},"""
        + """{"FrameType":"DataTable","TableId":2,"TableName":"T2","Columns":[{"ColumnName":"s","ColumnType":"string"}],"Rows":[["d"],["e"]]}]""");

    private static string Text(KustoRow row) => row.UseData(static (data, _) => Encoding.UTF8.GetString(data), 0);

    /// <summary>Runs the library's lazy initialization outside of Coyote, so that every schedule starts from the same state.</summary>
    [ClassInitialize]
    public static async Task WarmUpAsync(TestContext context)
    {
        await foreach (KustoTable table in new KustoTableReader(ChunkedStream.Whole(Payload)).ReadTablesAsync())
        {
            await foreach (KustoRow row in table.GetRowsAsync())
            {
                _ = Text(row);
                _ = row.ToValues();
                _ = row.GetValue<string>(0);
                _ = row.GetValue<string>("s");
                row.ToDocument().Dispose();
            }
        }

        List<string> events = await Harness.ReadEventsAsync(new KustoTableReader(ChunkedStream.Whole(Response("""[["a"],5,["c"]]"""))));
        events[^1].Should().Be(Events.Throws<System.Text.Json.JsonException>());
    }

    private static bool IsRejection(Exception e) => e.GetType() == typeof(InvalidOperationException);

    /// <summary>Calls <paramref name="read"/>, returning null if the row expired or the guard rejected the call.</summary>
    private static T? TryRead<T>(Func<T> read, Outcomes outcomes)
    {
        try
        {
            return read();
        }
        catch (ObjectDisposedException e) when (e.GetType() == typeof(ObjectDisposedException))
        {
            outcomes.Add("row expired");
            return default;
        }
        catch (InvalidOperationException e) when (IsRejection(e))
        {
            outcomes.Add("row read rejected");
            return default;
        }
    }

    private static void ShouldBeInOrder(IEnumerable<string> seen, string[] rows) =>
        seen.Select(row => Array.IndexOf(rows, row)).Should().OnlyContain(index => index >= 0).And.BeInAscendingOrder().And.OnlyHaveUniqueItems();

    private static void ShouldNotBePositioned(IAsyncEnumerator<KustoRow> rows) =>
        ((Func<KustoTable>)(() => rows.Current.Table)).Should().ThrowExactly<ObjectDisposedException>();

    /// <summary>A reader of <see cref="Payload"/> (or another response) over a <see cref="CoyoteStream"/> and a tracking pool.</summary>
    private sealed class Session
    {
        private Session(byte[] payload, int failAt, CancellationToken cancellationToken)
        {
            Stream = new(payload, failAt: failAt);
            Pool = new();

            // A small buffer, so that it grows and compacts while the operations race.
            Reader = new(Stream, Pool, 8);
            Tables = Reader.ReadTablesAsync(cancellationToken).GetAsyncEnumerator();
        }

        public CoyoteStream Stream { get; }

        public TrackingArrayPool Pool { get; }

        public KustoTableReader Reader { get; }

        public IAsyncEnumerator<KustoTable> Tables { get; }

        public static Session Create(byte[]? payload = null, int failAt = -1, CancellationToken cancellationToken = default) =>
            new(payload ?? Payload, failAt, cancellationToken);

        public static async Task<(Session Session, IAsyncEnumerator<KustoRow> Rows)> OnFirstTableAsync(byte[]? payload = null, int failAt = -1)
        {
            Session session = Create(payload, failAt);
            (await session.Tables.MoveNextAsync()).Should().BeTrue();
            session.Tables.Current.Name.Should().Be("T1");
            return (session, session.Tables.Current.GetRowsAsync().GetAsyncEnumerator());
        }

        /// <summary>
        /// Reads the rest of the response sequentially, from the rest of the first table's <paramref name="rows"/> or,
        /// if <paramref name="onSecondTable"/>, from the second table; then checks that every buffer was returned.
        /// </summary>
        public async Task FinishAsync(IAsyncEnumerator<KustoRow> rows, bool onSecondTable = false)
        {
            List<string> first = [];
            while (await rows.MoveNextAsync())
            {
                first.Add(Text(rows.Current));
            }

            if (onSecondTable)
            {
                first.Should().BeEmpty("the reader moved past the first table");
            }

            await rows.DisposeAsync();
            if (!onSecondTable)
            {
                (await Tables.MoveNextAsync()).Should().BeTrue();
            }

            Tables.Current.Name.Should().Be("T2");
            List<string> second = [];
            await foreach (KustoRow row in Tables.Current.GetRowsAsync())
            {
                second.Add(Text(row));
            }

            second.Should().Equal(SecondRows);
            (await Tables.MoveNextAsync()).Should().BeFalse();
            await Tables.DisposeAsync();
            ShouldHaveReleasedEverything();
        }

        /// <summary>Checks that the reader has ended, and has returned every buffer without disposing the stream.</summary>
        public async Task ShouldHaveEndedAsync(IAsyncEnumerator<KustoRow> rows)
        {
            ShouldNotBePositioned(rows);
            (await rows.MoveNextAsync()).Should().BeFalse();
            (await Tables.MoveNextAsync()).Should().BeFalse();
            await rows.DisposeAsync();
            await Tables.DisposeAsync();
            ShouldHaveReleasedEverything();
        }

        private void ShouldHaveReleasedEverything()
        {
            Pool.AssertAllReturned();
            Stream.IsDisposed.Should().BeFalse();
        }
    }

    /// <summary>Advances <paramref name="rows"/> to the end, reading each row it reaches; returns how many rows it advanced.</summary>
    private static async Task<int> ReadRowsAsync(IAsyncEnumerator<KustoRow> rows, List<string> seen, Outcomes outcomes)
    {
        int advanced = 0;
        while (true)
        {
            (bool done, bool moved) = await Systematic.RetryAsync(rows.MoveNextAsync, outcomes, "row advance rejected");
            if (!done || !moved)
            {
                return advanced;
            }

            advanced++;
            if (TryRead(() => Text(rows.Current), outcomes) is { } row)
            {
                seen.Add(row);
            }
        }
    }

    [TestMethod]
    public void ConcurrentRowAdvances_AdvanceExactlyOncePerRow()
    {
        Outcomes outcomes = new("row advance rejected", "row expired");
        Systematic.Test(async () =>
        {
            (Session session, IAsyncEnumerator<KustoRow> rows) = await Session.OnFirstTableAsync();
            List<string> first = [], second = [];

            int[] advanced = await Task.WhenAll(
                Task.Run(() => ReadRowsAsync(rows, first, outcomes)),
                Task.Run(() => ReadRowsAsync(rows, second, outcomes)));
            List<string> rest = [];
            int remaining = await ReadRowsAsync(rows, rest, outcomes);

            (advanced.Sum() + remaining).Should().Be(FirstRows.Length);
            ShouldBeInOrder(first, FirstRows);
            ShouldBeInOrder(second, FirstRows);
            ShouldBeInOrder([.. first.Union(second).Order(), .. rest], FirstRows);
            await session.FinishAsync(rows);
        }, outcomes);
    }

    [TestMethod]
    public void ConcurrentTableAdvances_AdvanceExactlyOncePerTable()
    {
        Outcomes outcomes = new("table advance rejected");
        Systematic.Test(async () =>
        {
            Session session = Session.Create();

            async Task<int> AdvanceAsync(List<string> seen)
            {
                int advanced = 0;
                while (true)
                {
                    (bool done, bool moved) = await Systematic.RetryAsync(session.Tables.MoveNextAsync, outcomes, "table advance rejected");
                    if (!done || !moved)
                    {
                        return advanced;
                    }

                    advanced++;
                    seen.Add(session.Tables.Current.Name);
                }
            }

            List<string> first = [], second = [];
            int[] advanced = await Task.WhenAll(Task.Run(() => AdvanceAsync(first)), Task.Run(() => AdvanceAsync(second)));
            List<string> rest = [];
            int remaining = await AdvanceAsync(rest);

            (advanced.Sum() + remaining).Should().Be(2);
            string[] names = ["T1", "T2"];
            ShouldBeInOrder(first, names);
            ShouldBeInOrder(second, names);
            ShouldBeInOrder([.. first.Union(second).Order(), .. rest], names);
            await session.Tables.DisposeAsync();
            session.Pool.AssertAllReturned();
        }, outcomes);
    }

    [TestMethod]
    public void TablesAdvance_RacingRowAdvances_SkipsTheRestOfTheTable()
    {
        Outcomes outcomes = new("row advance rejected", "table advance rejected", "skipped after 0 rows", "skipped after 3 rows");
        Systematic.Test(async () =>
        {
            (Session session, IAsyncEnumerator<KustoRow> rows) = await Session.OnFirstTableAsync();
            List<string> seen = [];

            Task<int> reading = Task.Run(() => ReadRowsAsync(rows, seen, outcomes));
            (bool skipped, bool moved) = await Task.Run(() => Systematic.RetryAsync(session.Tables.MoveNextAsync, outcomes, "table advance rejected"));
            int advanced = await reading;

            ShouldBeInOrder(seen, FirstRows);
            if (skipped)
            {
                moved.Should().BeTrue();
                outcomes.Add($"skipped after {advanced} rows");
            }

            await session.FinishAsync(rows, onSecondTable: skipped);
        }, outcomes);
    }

    [TestMethod]
    public void TablesDispose_RacingRowAdvances_EndsTheReader()
    {
        Outcomes outcomes = new("row advance rejected", "tables dispose rejected", "disposed after 0 rows", "disposed after 3 rows");
        Systematic.Test(async () =>
        {
            (Session session, IAsyncEnumerator<KustoRow> rows) = await Session.OnFirstTableAsync();
            List<string> seen = [];

            Task<int> reading = Task.Run(() => ReadRowsAsync(rows, seen, outcomes));
            (bool disposed, _) = await Task.Run(() => Systematic.RetryAsync(session.Tables.DisposeAsync, outcomes, "tables dispose rejected"));
            int advanced = await reading;

            ShouldBeInOrder(seen, FirstRows);
            if (disposed)
            {
                outcomes.Add($"disposed after {advanced} rows");
            }
            else
            {
                await session.Tables.DisposeAsync();
            }

            await session.ShouldHaveEndedAsync(rows);
        }, outcomes);
    }

    [TestMethod]
    public void RowsDispose_RacingRowAdvances_LeavesNoRow()
    {
        Outcomes outcomes = new("row advance rejected", "rows dispose rejected", "rows disposed after 0 rows", "rows disposed after 3 rows");
        Systematic.Test(async () =>
        {
            (Session session, IAsyncEnumerator<KustoRow> rows) = await Session.OnFirstTableAsync();
            List<string> seen = [];

            Task<int> reading = Task.Run(() => ReadRowsAsync(rows, seen, outcomes));
            (bool disposed, _) = await Task.Run(() => Systematic.RetryAsync(rows.DisposeAsync, outcomes, "rows dispose rejected"));
            int advanced = await reading;

            ShouldBeInOrder(seen, FirstRows);
            if (disposed)
            {
                outcomes.Add($"rows disposed after {advanced} rows");
            }
            else
            {
                await rows.DisposeAsync();
            }

            // However the advances and the disposal interleave, the enumeration ends with the disposal.
            ShouldNotBePositioned(rows);
            (await rows.MoveNextAsync()).Should().BeFalse();
            await session.FinishAsync(rows);
        }, outcomes);
    }

    [TestMethod]
    public void OperationsWhileRowDataIsInUse_AreRejectedAndLeaveTheDataUnchanged()
    {
        Outcomes outcomes = new("rejected while the row data is in use");
        Systematic.Test(async () =>
        {
            (Session session, IAsyncEnumerator<KustoRow> rows) = await Session.OnFirstTableAsync();
            bool inUse = false;

            string UseData(KustoRow row) => row.UseData(
                (data, _) =>
                {
                    string before = Encoding.UTF8.GetString(data);
                    inUse = true;
                    for (int i = 0; i < 3; i++)
                    {
                        SchedulingPoint.Interleave();
                    }

                    inUse = false;
                    Encoding.UTF8.GetString(data).Should().Be(before, "the row's data must not change while it is in use");
                    return before;
                },
                0);

            async Task ReadAsync(List<string> seen)
            {
                while (true)
                {
                    (bool done, bool moved) = await Systematic.RetryAsync(rows.MoveNextAsync, outcomes, "row advance rejected");
                    if (!done || !moved)
                    {
                        return;
                    }

                    if (TryRead(() => UseData(rows.Current), outcomes) is { } row)
                    {
                        seen.Add(row);
                    }
                }
            }

            // Every operation that moves or releases the buffer; Coyote chooses which ones to try, and when.
            async Task InterfereAsync()
            {
                Generator generator = Generator.Create();
                for (int i = 0; i < 6; i++)
                {
                    try
                    {
                        await (generator.NextInteger(4) switch
                        {
                            0 => rows.MoveNextAsync().AsTask(),
                            1 => session.Tables.MoveNextAsync().AsTask(),
                            2 => rows.DisposeAsync().AsTask(),
                            _ => session.Tables.DisposeAsync().AsTask(),
                        });
                    }
                    catch (InvalidOperationException e) when (IsRejection(e))
                    {
                        if (inUse)
                        {
                            outcomes.Add("rejected while the row data is in use");
                        }
                    }
                }
            }

            List<string> seen = [];
            await Task.WhenAll(Task.Run(() => ReadAsync(seen)), Task.Run(InterfereAsync));

            ShouldBeInOrder(seen, FirstRows);
            await rows.DisposeAsync();
            await session.Tables.DisposeAsync();
            session.Pool.AssertAllReturned();
        }, outcomes);
    }

    [TestMethod]
    public void RowReads_RacingRowAdvances_ReturnTheRowOrThrow()
    {
        Outcomes outcomes = new("row read", "row expired", "row read rejected");
        Systematic.Test(async () =>
        {
            (Session session, IAsyncEnumerator<KustoRow> rows) = await Session.OnFirstTableAsync();
            List<(KustoRow Row, string Text)> captured = [];
            bool ended = false;

            async Task AdvanceAsync()
            {
                while (true)
                {
                    (bool done, bool moved) = await Systematic.RetryAsync(rows.MoveNextAsync, outcomes, "row advance rejected");
                    if (!done || !moved)
                    {
                        ended = true;
                        return;
                    }

                    KustoRow row = rows.Current;
                    if (TryRead(() => Text(row), outcomes) is { } text)
                    {
                        captured.Add((row, text));
                    }
                }
            }

            // Reads a captured row in a way that Coyote chooses: each read must see that row's data, or throw.
            async Task ReadAsync()
            {
                Generator generator = Generator.Create();
                for (int i = 0; i < 8 && !ended; i++)
                {
                    await Task.Yield();
                    if (captured.Count == 0)
                    {
                        continue;
                    }

                    (KustoRow row, string text) = captured[generator.NextInteger(captured.Count)];
                    string expected = JsonDocument.Parse(text).RootElement[0].GetString()!;
                    string? value = TryRead<string?>(
                        () => generator.NextInteger(4) switch
                        {
                            0 => (string?)row.ToValues()[0],
                            1 => row.GetValue<string>(0),
                            2 => row.GetValue<string>("s"),
                            _ => ReadDocument(row),
                        },
                        outcomes);
                    if (value is not null)
                    {
                        value.Should().Be(expected);
                        outcomes.Add("row read");
                    }
                }
            }

            static string? ReadDocument(KustoRow row)
            {
                using JsonDocument document = row.ToDocument();
                return document.RootElement[0].GetString();
            }

            await Task.WhenAll(Task.Run(AdvanceAsync), Task.Run(ReadAsync));

            ShouldBeInOrder(captured.Select(c => c.Text), FirstRows);
            await session.FinishAsync(rows);
        }, outcomes);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void Cancellation_RacingReads_EndsTheReaderWithTheStreamsException(bool cancelRows)
    {
        // The first table is read with the tables' token, so only that token can cancel before it.
        Outcomes outcomes = new("completed", "canceled", cancelRows ? "canceled after 1 events" : "canceled after 0 events");
        Systematic.Test(async () =>
        {
            using CancellationTokenSource tablesSource = new(), rowsSource = new();
            CancellationTokenSource canceled = cancelRows ? rowsSource : tablesSource;
            Session session = Session.Create(cancellationToken: tablesSource.Token);
            List<string> events = [];
            Exception? error = null;

            async Task ReadAsync()
            {
                try
                {
                    while (await session.Tables.MoveNextAsync())
                    {
                        events.Add(session.Tables.Current.Name);
                        await foreach (KustoRow row in session.Tables.Current.GetRowsAsync(rowsSource.Token))
                        {
                            events.Add(Text(row));
                        }
                    }
                }
                catch (Exception e)
                {
                    error = e;
                }
            }

            await Task.WhenAll(Task.Run(ReadAsync), Task.Run(canceled.Cancel));

            string[] all = ["T1", .. FirstRows, "T2", .. SecondRows];
            events.Should().Equal(all.Take(events.Count));
            if (error is null)
            {
                events.Should().Equal(all);
                outcomes.Add("completed");
            }
            else
            {
                error.Should().BeSameAs(session.Stream.Thrown);
                error.Should().BeOfType<OperationCanceledException>().Which.CancellationToken.Should().Be(canceled.Token);
                session.Stream.Tokens[^1].Should().Be(canceled.Token);
                outcomes.Add("canceled");
                outcomes.Add($"canceled after {events.Count} events");
                (await session.Tables.MoveNextAsync()).Should().BeFalse();
            }

            await session.Tables.DisposeAsync();
            session.Pool.AssertAllReturned();
        }, outcomes);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void Failure_RacingOperations_IsReportedOnceAndReleasesEverythingOnce(bool streamFailure)
    {
        Outcomes outcomes = new("failed", "ended before the failure");
        Systematic.Test(async () =>
        {
            // The failure is where the second row would be: in the data, or in the stream that serves it.
            byte[] payload = streamFailure ? Payload : Response("""[["a"],5,["c"]]""");
            int failAt = streamFailure ? Encoding.UTF8.GetString(Payload).IndexOf("[\"b\"]", StringComparison.Ordinal) + 2 : -1;
            (Session session, IAsyncEnumerator<KustoRow> rows) = await Session.OnFirstTableAsync(payload, failAt);
            List<Exception> failures = [];

            async Task RunAsync(Func<Task> operation)
            {
                try
                {
                    await operation();
                }
                catch (InvalidOperationException e) when (IsRejection(e))
                {
                }
                catch (Exception e)
                {
                    failures.Add(e);
                }
            }

            List<string> seen = [];
            Task reading = Task.Run(() => RunAsync(() => ReadRowsAsync(rows, seen, outcomes)));

            Task interfering = Task.Run(async () =>
            {
                Generator generator = Generator.Create();
                for (int i = 0; i < 3; i++)
                {
                    await RunAsync(() => generator.NextInteger(3) switch
                    {
                        0 => rows.MoveNextAsync().AsTask(),
                        1 => session.Tables.MoveNextAsync().AsTask(),
                        _ => session.Tables.DisposeAsync().AsTask(),
                    });
                }
            });

            await Task.WhenAll(reading, interfering);

            ShouldBeInOrder(seen, ["[\"a\"]"]);
            failures.Should().HaveCountLessThanOrEqualTo(1);
            if (failures.Count == 1)
            {
                if (streamFailure)
                {
                    failures[0].Should().BeSameAs(session.Stream.Thrown).And.BeOfType<IOException>();
                }
                else
                {
                    failures[0].ShouldBeJsonException();
                }

                outcomes.Add("failed");
            }
            else
            {
                outcomes.Add("ended before the failure");
            }

            await session.Tables.DisposeAsync();
            await session.ShouldHaveEndedAsync(rows);
        }, outcomes);
    }

    [TestMethod]
    public void ConcurrentEnumerations_OnlyOneStarts()
    {
        Systematic.Test(async () =>
        {
            TrackingArrayPool pool = new();
            KustoTableReader reader = new(new CoyoteStream(Payload), pool, 8);

            async Task<IAsyncEnumerator<T>[]> StartAsync<T>(Func<IAsyncEnumerable<T>> enumerable)
            {
                IAsyncEnumerator<T>?[] started = await Task.WhenAll(Enumerable.Range(0, 2).Select(_ => Task.Run(() =>
                {
                    try
                    {
                        return enumerable().GetAsyncEnumerator();
                    }
                    catch (InvalidOperationException e) when (IsRejection(e))
                    {
                        return null;
                    }
                })));

                return [.. started.OfType<IAsyncEnumerator<T>>()];
            }

            IAsyncEnumerator<KustoTable> tables = (await StartAsync(() => reader.ReadTablesAsync())).Should().ContainSingle().Which;
            (await tables.MoveNextAsync()).Should().BeTrue();
            KustoTable table = tables.Current;
            IAsyncEnumerator<KustoRow> rows = (await StartAsync(() => table.GetRowsAsync())).Should().ContainSingle().Which;

            List<string> seen = [];
            while (await rows.MoveNextAsync())
            {
                seen.Add(Text(rows.Current));
            }

            seen.Should().Equal(FirstRows);
            await tables.DisposeAsync();
            pool.AssertAllReturned();
        });
    }
}

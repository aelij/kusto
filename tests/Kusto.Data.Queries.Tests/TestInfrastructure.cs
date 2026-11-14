using AwesomeAssertions;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace Kusto.Data.Queries.Tests;

public enum Completion
{
    Sync,
    Async,
    Mixed,
}

/// <summary>A read-only stream that only supports <see cref="Stream.ReadAsync(Memory{byte}, CancellationToken)"/>.</summary>
internal abstract class TestStream : Stream
{
    private readonly List<CancellationToken> _tokens = [];

    public IReadOnlyList<CancellationToken> Tokens => _tokens;

    public int ReadCount => _tokens.Count;

    public bool IsDisposed { get; private set; }

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

    public sealed override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        _tokens.Add(cancellationToken);
        return ReadCoreAsync(buffer, cancellationToken);
    }

    protected abstract ValueTask<int> ReadCoreAsync(Memory<byte> buffer, CancellationToken cancellationToken);

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => throw Misuse();

    public override int Read(byte[] buffer, int offset, int count) => throw Misuse();

    public override int Read(Span<byte> buffer) => throw Misuse();

    public override int ReadByte() => throw Misuse();

    public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback? callback, object? state) => throw Misuse();

    public override void Flush() => throw Misuse();

    public override long Seek(long offset, SeekOrigin origin) => throw Misuse();

    public override void SetLength(long value) => throw Misuse();

    public override void Write(byte[] buffer, int offset, int count) => throw Misuse();

    protected override void Dispose(bool disposing)
    {
        IsDisposed = true;
        base.Dispose(disposing);
    }

    private static NotSupportedException Misuse() => new("The reader must only use ReadAsync(Memory<byte>, CancellationToken).");
}

/// <summary>
/// Serves <c>data</c> in chunks whose size <c>chunkAt(position)</c> decides, completing each read synchronously or after a
/// yield, and throwing an <see cref="IOException"/> once <c>failAt</c> bytes have been served.
/// </summary>
internal sealed class ChunkedStream(byte[] data, Func<int, int> chunkAt, Completion completion, int failAt = -1) : TestStream
{
    public int Offset { get; private set; }

    public Exception? Thrown { get; private set; }

    public static ChunkedStream Whole(byte[] data, Completion completion = Completion.Sync) => new(data, _ => int.MaxValue, completion);

    public static ChunkedStream OneByte(byte[] data, Completion completion = Completion.Sync) => new(data, _ => 1, completion);

    public static ChunkedStream Fixed(byte[] data, int size, Completion completion = Completion.Sync) => new(data, _ => size, completion);

    public static ChunkedStream SplitAt(byte[] data, Completion completion, params int[] splits) =>
        new(data, position => splits.Where(s => s > position).Select(s => s - position).DefaultIfEmpty(int.MaxValue).Min(), completion);

    public static ChunkedStream Random(byte[] data, int seed, int maxChunk, Completion completion = Completion.Mixed)
    {
        Random random = new(seed);
        return new(data, _ => random.Next(1, maxChunk + 1), completion);
    }

    public static ChunkedStream FailAfter(byte[] data, int failAt, Func<int, int> chunkAt, Completion completion) => new(data, chunkAt, completion, failAt);

    protected override async ValueTask<int> ReadCoreAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        if (completion == Completion.Async || (completion == Completion.Mixed && ReadCount % 2 == 0))
        {
            await Task.Yield();
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Offset == failAt)
            {
                throw new IOException($"Simulated failure after {failAt} bytes.");
            }
        }
        catch (Exception e)
        {
            Thrown = e;
            throw;
        }

        int end = failAt >= 0 ? failAt : data.Length;
        int count = Math.Min(Math.Min(buffer.Length, chunkAt(Offset)), end - Offset);
        data.AsSpan(Offset, count).CopyTo(buffer.Span);
        Offset += count;
        return count;
    }
}

/// <summary>A stream whose reads stay pending until the test releases them.</summary>
internal sealed class GatedStream(byte[] data) : TestStream
{
    private TaskCompletionSource<int>? _pending;
    private Memory<byte> _target;
    private CancellationTokenRegistration _registration;
    private bool _open;

    public int Offset { get; private set; }

    public bool IsPending => _pending is not null;

    public int Remaining => data.Length - Offset;

    protected override ValueTask<int> ReadCoreAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        if (_pending is not null)
        {
            throw new InvalidOperationException("Overlapping reads.");
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<int>(cancellationToken);
        }

        if (_open)
        {
            int count = Math.Min(buffer.Length, data.Length - Offset);
            data.AsSpan(Offset, count).CopyTo(buffer.Span);
            Offset += count;
            return new(count);
        }

        TaskCompletionSource<int> pending = new();
        _pending = pending;
        _target = buffer;
        _registration = cancellationToken.Register(() =>
        {
            _pending = null;
            pending.TrySetCanceled(cancellationToken);
        });
        return new(pending.Task);
    }

    /// <summary>Completes the pending read with at most <paramref name="max"/> bytes; continuations run inline.</summary>
    public void Release(int max = int.MaxValue)
    {
        TaskCompletionSource<int> pending = _pending ?? throw new InvalidOperationException("No read is pending.");
        _pending = null;
        _registration.Dispose();
        int count = Math.Min(Math.Min(max, _target.Length), data.Length - Offset);
        data.AsSpan(Offset, count).CopyTo(_target.Span);
        Offset += count;
        pending.SetResult(count);
    }

    /// <summary>Completes the pending read, if any, and every later read immediately.</summary>
    public void Open()
    {
        _open = true;
        if (_pending is not null)
        {
            Release();
        }
    }

    /// <summary>Releases reads until <paramref name="task"/> completes.</summary>
    public async ValueTask<T> CompleteAsync<T>(ValueTask<T> task)
    {
        while (!task.IsCompleted)
        {
            Release();
        }

        return await task;
    }
}

/// <summary>
/// An array pool that rents arrays of exactly the requested size, poisons them on rent and return, and checks that
/// every rented array is returned exactly once with <c>clearArray: true</c>.
/// </summary>
internal sealed class TrackingArrayPool : ArrayPool<byte>
{
    private readonly Lock _lock = new();
    private readonly Dictionary<byte[], int> _returns = new(ReferenceEqualityComparer.Instance);
    private readonly List<int> _rentedSizes = [];
    private readonly List<string> _violations = [];

    public IReadOnlyList<int> RentedSizes
    {
        get
        {
            lock (_lock)
            {
                return [.. _rentedSizes];
            }
        }
    }

    public int Outstanding
    {
        get
        {
            lock (_lock)
            {
                return _returns.Values.Count(count => count == 0);
            }
        }
    }

    public override byte[] Rent(int minimumLength)
    {
        byte[] array = new byte[minimumLength];
        array.AsSpan().Fill(0xCC);
        lock (_lock)
        {
            _returns.Add(array, 0);
            _rentedSizes.Add(minimumLength);
        }

        return array;
    }

    public override void Return(byte[] array, bool clearArray = false)
    {
        lock (_lock)
        {
            if (!_returns.TryGetValue(array, out int count))
            {
                _violations.Add($"An array of {array.Length} bytes that was not rented was returned.");
            }
            else if (count != 0)
            {
                _violations.Add($"An array of {array.Length} bytes was returned more than once.");
            }

            if (!clearArray)
            {
                _violations.Add($"An array of {array.Length} bytes was returned without clearArray.");
            }

            _returns[array] = count + 1;
        }

        array.AsSpan().Fill(0xFF);
    }

    public void AssertAllReturned(string context = "")
    {
        lock (_lock)
        {
            List<string> problems = [.. _violations];
            int outstanding = _returns.Values.Count(count => count == 0);
            if (outstanding != 0)
            {
                problems.Add($"{outstanding} rented array(s) were not returned.");
            }

            if (problems.Count != 0)
            {
                Assert.Fail($"{context} Pool misuse: {string.Join(" ", problems)}");
            }
        }
    }
}

internal static class Events
{
    public static string Table(int id, string name, KustoTableKind kind, IEnumerable<KustoColumn> columns) =>
        $"table {id} '{name}' {kind} ({string.Join(", ", columns.Select(c => $"{c.Name}:{c.Type}"))})";

    public static string Row(ReadOnlySpan<byte> json) => "row " + Encoding.UTF8.GetString(json);

    public static string Throws(Exception e) => "throws " + e switch
    {
        JsonException => nameof(JsonException),
        _ => e.GetType().Name,
    };

    public static string Throws<T>() => "throws " + typeof(T).Name;
}

/// <summary>The independent expectation for a payload: what the reader must yield, and the offset by which each is available.</summary>
internal sealed record ExpectedEvent(string Text, int End);

internal static class ResponseOracle
{
    private static ReadOnlySpan<byte> Bom => [0xEF, 0xBB, 0xBF];

    public static List<ExpectedEvent> Expect(byte[] payload)
    {
        ReadOnlyMemory<byte> json = payload.AsMemory(payload.AsSpan().StartsWith(Bom) ? Bom.Length : 0);
        using JsonDocument document = JsonDocument.Parse(json);
        JsonElement root = document.RootElement;
        List<ExpectedEvent> events = [];
        if (root.ValueKind == JsonValueKind.Object)
        {
            events.Add(new(Events.Throws<KustoException>(), End(payload, root)));
            return events;
        }

        foreach (JsonElement frame in root.EnumerateArray())
        {
            switch (frame.GetProperty("FrameType").GetString())
            {
                case "DataTable":
                    JsonElement rows = frame.GetProperty("Rows");
                    events.Add(new(TableEvent(frame), Start(payload, rows) + 1));
                    foreach (JsonElement row in rows.EnumerateArray())
                    {
                        if (row.ValueKind == JsonValueKind.Object)
                        {
                            events.Add(new(Events.Throws<KustoException>(), End(payload, row)));
                            return events;
                        }

                        events.Add(new(Events.Row(JsonMarshal.GetRawUtf8Value(row)), End(payload, row)));
                    }

                    break;

                case "DataSetCompletion" when IsTrue(frame, "HasErrors") || IsTrue(frame, "Cancelled")
                    || (frame.TryGetProperty("OneApiErrors", out JsonElement errors) && errors.ValueKind == JsonValueKind.Array && errors.GetArrayLength() > 0):
                    events.Add(new(Events.Throws<KustoException>(), End(payload, frame)));
                    return events;
            }
        }

        return events;
    }

    public static List<string> Texts(this IEnumerable<ExpectedEvent> events) => [.. events.Select(e => e.Text)];

    /// <summary>The events a reader must yield before failing when the stream ends (or fails) after <paramref name="length"/> bytes.</summary>
    public static List<string> Prefix(this IEnumerable<ExpectedEvent> events, int length, string error) =>
        [.. events.TakeWhile(e => e.End <= length).Select(e => e.Text), error];

    private static string TableEvent(JsonElement frame)
    {
        string? kind = frame.TryGetProperty("TableKind", out JsonElement k) ? k.GetString() : null;
        return Events.Table(
            frame.TryGetProperty("TableId", out JsonElement id) ? id.GetInt32() : 0,
            frame.TryGetProperty("TableName", out JsonElement name) ? name.GetString() ?? "" : "",
            Enum.GetValues<KustoTableKind>().FirstOrDefault(value => value.ToString() == kind),
            frame.GetProperty("Columns").EnumerateArray().Select(c => new KustoColumn(
                c.GetProperty("ColumnName").GetString()!,
                Enum.Parse<KustoColumnType>(c.GetProperty("ColumnType").GetString()!, ignoreCase: true))));
    }

    private static bool IsTrue(JsonElement frame, string name) => frame.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.True;

    private static int Start(byte[] payload, JsonElement element)
    {
        if (!payload.AsSpan().Overlaps(JsonMarshal.GetRawUtf8Value(element), out int offset))
        {
            throw new InvalidOperationException("The element is not a slice of the payload.");
        }

        return offset;
    }

    private static int End(byte[] payload, JsonElement element) => Start(payload, element) + JsonMarshal.GetRawUtf8Value(element).Length;
}

internal static class Harness
{
    public static byte[] Utf8(string json) => Encoding.UTF8.GetBytes(json);

    /// <summary>Reads every table and row, recording each as an event; a failure ends the sequence.</summary>
    public static async Task<List<string>> ReadEventsAsync(KustoTableReader reader, CancellationToken cancellationToken = default)
    {
        List<string> events = [];
        try
        {
            await foreach (KustoTable table in reader.ReadTablesAsync(cancellationToken))
            {
                events.Add(Events.Table(table.Id, table.Name, table.Kind, table.Columns));
                await foreach (KustoRow row in table.GetRowsAsync())
                {
                    events.Add(row.UseData(static (data, _) => Events.Row(data), 0));
                }
            }
        }
        catch (Exception e)
        {
            events.Add(Events.Throws(e));
        }

        return events;
    }

    /// <summary>Reads <paramref name="stream"/> through a tracking pool and checks the events and the pool.</summary>
    public static async Task VerifyAsync(TestStream stream, int bufferSize, IReadOnlyList<string> expected, string context = "")
    {
        TrackingArrayPool pool = new();
        List<string> actual = await ReadEventsAsync(new KustoTableReader(stream, pool, bufferSize));
        if (!actual.SequenceEqual(expected))
        {
            actual.Should().Equal(expected, context);
        }

        pool.AssertAllReturned(context);
        if (stream.IsDisposed)
        {
            Assert.Fail($"{context} The reader disposed the stream.");
        }
    }

    public static Task VerifyAsync(TestStream stream, int bufferSize, IEnumerable<ExpectedEvent> expected, string context = "") =>
        VerifyAsync(stream, bufferSize, expected.Texts(), context);

    /// <summary>Asserts that <paramref name="e"/> is a <see cref="JsonException"/> (System.Text.Json's reader throws an internal subclass).</summary>
    public static void ShouldBeJsonException(this Exception? e) =>
        e.Should().BeAssignableTo<JsonException>().Which.GetType().Namespace.Should().Be("System.Text.Json");
}

internal static class Payloads
{
    public const string Header = """{"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0","IsFragmented":false,"ErrorReportingPlacement":"InData"}""";

    public const string Completion = """{"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":false}""";

    public const string StringColumn = """[{"ColumnName":"s","ColumnType":"string"}]""";

    public static string Table(int id, string rows, string columns = StringColumn, string kind = "PrimaryResult", string name = "PrimaryResult") =>
        $$"""{"FrameType":"DataTable","TableId":{{id}},"TableKind":"{{kind}}","TableName":"{{name}}","Columns":{{columns}},"Rows":{{rows}}}""";

    public static string Response(params string[] frames) => "[" + string.Join(",", [Header, .. frames, Completion]) + "]";

    /// <summary>
    /// A compact response with several tables (one empty), nested dynamic values, strings with structural characters,
    /// escapes and multi-byte UTF-8, numbers, nulls, whitespace and unknown properties.
    /// </summary>
    public static readonly byte[] Representative = Harness.Utf8(
        """
        [{"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0","Extra":{"a":[1,{"b":null}]}},
         {"FrameType":"DataTable","TableId":0,"TableKind":"QueryProperties","TableName":"@ExtendedProperties",
          "Columns":[{"ColumnName":"Key","ColumnType":"string"},{"ColumnName":"Value","ColumnType":"dynamic"}],
          "Rows":[["a",{"x":[1,{"y":null}],"z":"]}"}],[ "b" , "[]{}\"\\" ],["cé",[[[]],{}]]]},
         {"FrameType":"DataTable","TableId":1,"TableKind":"PrimaryResult","TableName":"Empty","Columns":[{"ColumnName":"n","ColumnType":"long"}],"Rows":[]},
         {"FrameType":"DataTable","TableId":2,"TableKind":"PrimaryResult","TableName":"T2",
          "Columns":[{"ColumnName":"i","ColumnType":"long"},{"ColumnName":"r","ColumnType":"real"},{"ColumnName":"s","ColumnType":"string"},{"ColumnName":"d","ColumnType":"dynamic"}],
          "Rows":[[1,-2.5e3,"é☃😀\t",[[[1]],{"k":[]}]],
                  [null,null,null,null],
                  [9223372036854775807, 1E+300, "x]y}z{w[\"", {"k":"v\n","n":{"m":[true,false]}}]],
          "After":[{"Rows":1}]},
         {"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":false}]
        """);

    /// <summary>A smaller response for tests that try every pair of interruption points.</summary>
    public static readonly byte[] Small = Harness.Utf8(
        """[{"FrameType":"DataTable","TableId":3,"Columns":[{"ColumnName":"d","ColumnType":"dynamic"}],"Rows":[[{"a":["x\"]",1.5e2]}],[ "é☃" ],[]]},{"FrameType":"DataTable","TableKind":"QueryPlan","Columns":[],"Rows":[[]]}]""");
}

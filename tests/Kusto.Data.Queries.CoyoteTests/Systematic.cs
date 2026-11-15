using System.Runtime.CompilerServices;
using AwesomeAssertions;
using Kusto.Data.Queries.Tests;
using Microsoft.Coyote;
using Microsoft.Coyote.Random;
using Microsoft.Coyote.SystematicTesting;

namespace Kusto.Data.Queries.CoyoteTests;

/// <summary>Runs a test under Coyote, which explores the interleavings of its tasks one serialized schedule at a time.</summary>
internal static class Systematic
{
    // Set to a trace file that a failure wrote, to replay the failing schedule of that test under a debugger.
    private const string ReplayVariable = "COYOTE_REPLAY";

    /// <summary>
    /// Runs <paramref name="test"/> for <paramref name="iterations"/> schedules, then checks that they reached every
    /// outcome that <paramref name="outcomes"/> expects. Set <c>COYOTE_REPLAY</c> to the trace of a failure to replay it.
    /// </summary>
    /// <remarks>
    /// Replaying runs the test in a fresh process, so the code under test must not lazily initialize state on its first
    /// run: a test class warms up such code outside of Coyote first, or the replayed schedule would diverge.
    /// </remarks>
    public static void Test(Func<Task> test, Outcomes? outcomes = null, uint iterations = 300, [CallerMemberName] string name = "")
    {
        Configuration configuration = Configuration.Create()
            .WithTestingIterations(iterations)
            .WithMemoryAccessRaceCheckingEnabled()
            .WithPartiallyControlledConcurrencyAllowed(false)
            .WithSystematicFuzzingFallbackEnabled(false)
            .WithFailureOnMaxStepsBoundEnabled()
            .WithTelemetryEnabled(false);

        string? replay = Environment.GetEnvironmentVariable(ReplayVariable);
        bool replaying = replay is not null && Path.GetFileNameWithoutExtension(replay) == name;
        if (replaying)
        {
            configuration = configuration.WithReproducibleTrace(File.ReadAllText(replay!));
        }

        using TestingEngine engine = TestingEngine.Create(configuration, test);
        engine.Run();
        TestReport report = engine.TestReport;
        Console.WriteLine(engine.GetReport());

        report.InternalErrors.Should().BeEmpty();
        if (report.NumOfFoundBugs != 0)
        {
            string trace = Path.Combine(AppContext.BaseDirectory, "CoyoteTraces", name + ".trace");
            Directory.CreateDirectory(Path.GetDirectoryName(trace)!);
            File.WriteAllText(trace, engine.ReproducibleTrace);
            Assert.Fail($"{string.Join(Environment.NewLine, report.BugReports)}{Environment.NewLine}Replay with {ReplayVariable}={trace}");
        }

        report.UncontrolledInvocations.Should().BeEmpty("Coyote must control every operation, or it cannot explore them");
        if (replaying)
        {
            Assert.Inconclusive($"The schedule in {replay} did not fail.");
        }

        outcomes?.ShouldHaveBeenReached();
    }

    /// <summary>
    /// Retries <paramref name="operation"/> while the reader's guard rejects it, recording each rejection, and gives up
    /// after a few attempts so that unfair schedules still terminate. Any other exception propagates.
    /// </summary>
    public static async Task<(bool Done, T Result)> RetryAsync<T>(Func<ValueTask<T>> operation, Outcomes outcomes, string rejection)
    {
        for (int attempt = 0; attempt < 8; attempt++)
        {
            try
            {
                return (true, await operation());
            }
            catch (InvalidOperationException e) when (e.GetType() == typeof(InvalidOperationException))
            {
                outcomes.Add(rejection);
                await Task.Yield();
            }
        }

        return (false, default!);
    }

    public static Task<(bool Done, bool Result)> RetryAsync(Func<ValueTask> operation, Outcomes outcomes, string rejection) =>
        RetryAsync(async () =>
        {
            await operation();
            return true;
        }, outcomes, rejection);
}

/// <summary>
/// The outcomes that the iterations of a test reached, so that a test can check that Coyote explored the interleavings
/// it is about rather than passing vacuously. Coyote runs one operation at a time, so no locking is needed.
/// </summary>
internal sealed class Outcomes(params string[] expected)
{
    private readonly HashSet<string> _seen = [];

    public void Add(string outcome) => _seen.Add(outcome);

    public void ShouldHaveBeenReached() => _seen.Should().Contain(expected, "Coyote must explore these interleavings");
}

/// <summary>
/// Serves <c>data</c> in chunks of 1 to <c>maxChunk</c> bytes, failing with an <see cref="IOException"/> once
/// <c>failAt</c> bytes have been served. Coyote chooses each chunk's size and whether the read completes
/// synchronously, so the iterations also explore where the data is split and when the reader suspends.
/// </summary>
internal sealed class CoyoteStream(byte[] data, int maxChunk = 24, int failAt = -1) : TestStream
{
    private readonly Generator _generator = Generator.Create();
    private bool _reading;

    public int Offset { get; private set; }

    public Exception? Thrown { get; private set; }

    protected override async ValueTask<int> ReadCoreAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        _reading.Should().BeFalse("the reader must never overlap reads");
        _reading = true;
        try
        {
            if (_generator.NextBoolean())
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
            int count = Math.Min(Math.Min(buffer.Length, 1 + _generator.NextInteger(maxChunk)), end - Offset);
            data.AsSpan(Offset, count).CopyTo(buffer.Span);
            Offset += count;
            return count;
        }
        finally
        {
            _reading = false;
        }
    }
}

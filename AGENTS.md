See README.md for what the library does. This file covers what isn't obvious from the code.

## Invariants of `KustoTableReader`

- It is a resumable state machine: a `Utf8JsonReader` cannot cross an `await`, so state changes only after a
  fully read token and the position is committed once per attempt. It must be able to stop at any byte.
- Every operation that reads or moves the buffer holds the `_busy` guard (`Enter`/`Exit`), including row
  callbacks. Concurrent use fails fast; it never waits.
- A `KustoRow` is valid only until the next advance; validity is the version check in `EnterRow`.
- The buffer is rented on first read and returned exactly once, cleared, on end, failure or disposal.
- Valid input pays no per-row cost: the raw-row path allocates nothing. Run the benchmarks before and after
  changing the reader.

## Tests

- Reader tests go through `TrackingArrayPool` and the fake streams in `TestInfrastructure.cs`.
- New reader behaviour needs byte-by-byte and split-at-every-index coverage; keep `KustoTableReader.cs` near 100%.
- Malformed input must throw `JsonException` and nothing else; assert exact exception types.
- Don't weaken a test to make it pass.
- Parallel agents must each pass their own `--artifacts-path`, or their builds collide.

## Coyote tests

- `Kusto.Data.Queries.CoyoteTests` uses [Coyote](https://microsoft.github.io/coyote/) from the `external/coyote`
  submodule until a release can rewrite the library.
- Each test lists the `Outcomes` it must reach, so that it cannot pass without exploring the interleavings it is about.
- Warm up lazily initialized library code in `[ClassInitialize]`, or replaying a failure diverges.

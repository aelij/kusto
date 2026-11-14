using System.Buffers;

namespace Kusto.Data.Queries;

/// <summary>
/// An <see cref="IBufferWriter{T}"/> over an array rented from <see cref="ArrayPool{T}.Shared"/>, which grows by
/// renting a larger array. Arrays are cleared when returned, since they may hold sensitive data.
/// </summary>
internal sealed class PooledByteBufferWriter(int initialCapacity) : IBufferWriter<byte>, IDisposable
{
    private byte[]? _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
    private int _written;

    public ReadOnlyMemory<byte> WrittenMemory => Buffer.AsMemory(0, _written);

    private byte[] Buffer => _buffer ?? throw new ObjectDisposedException(nameof(PooledByteBufferWriter));

    public void Advance(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (count > Buffer.Length - _written)
        {
            throw new InvalidOperationException("Cannot advance past the end of the buffer.");
        }

        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0) => Grow(sizeHint).AsMemory(_written);

    public Span<byte> GetSpan(int sizeHint = 0) => Grow(sizeHint).AsSpan(_written);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _buffer, null) is { } buffer)
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    private byte[] Grow(int sizeHint)
    {
        byte[] buffer = Buffer;
        int required = Math.Max(sizeHint, 1);
        if (required <= buffer.Length - _written)
        {
            return buffer;
        }

        int minimum = checked(_written + required);
        byte[] larger = ArrayPool<byte>.Shared.Rent(Math.Max(minimum, (int)Math.Min(Array.MaxLength, 2L * buffer.Length)));
        buffer.AsSpan(0, _written).CopyTo(larger);
        _buffer = larger;
        ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        return larger;
    }
}

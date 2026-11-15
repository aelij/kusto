using System.Buffers;
using System.Diagnostics;
using System.Text.Json;

namespace Kusto.Data.Queries;

/// <summary>
/// Reads a Kusto v2 (non-progressive) query response as a forward-only stream of tables and rows. Rows are not
/// materialized: each <see cref="KustoRow"/> is a view over the pooled buffer the response is read into.
/// </summary>
/// <remarks>
/// The reader does not own the stream. Its buffer is rented when reading starts and returned when the table
/// enumerator completes, fails or is disposed.
/// </remarks>
public sealed class KustoTableReader
{
    private const int DefaultBufferSize = 64 * 1024;

    // An incomplete token at least this long is only rescanned once the unscanned data has doubled, which keeps
    // the total cost of a huge string value linear even when the stream returns it in small chunks.
    private const int StallThreshold = 4 * 1024;

    private static ReadOnlySpan<byte> Utf8Bom => [0xEF, 0xBB, 0xBF];

    private readonly Stream _stream;
    private readonly ArrayPool<byte> _pool;
    private readonly int _initialBufferSize;

    private CancellationToken _cancellationToken;
    private bool _started;

    // Held by every operation that reads or moves the buffer, including the callbacks that are given row data.
    private bool _busy;

    // Buffer layout: [_itemStart, _scanStart) is the tokenized part of the item being scanned, if any, and
    // [_scanStart, _end) has not been tokenized yet. Everything before that is free to be discarded.
    private byte[]? _buffer;
    private int _scanStart;
    private int _end;
    private bool _isFinalBlock;
    private bool _stalled;
    private JsonReaderState _jsonState;

    private State _state;
    private Frame _frame;
    private Property _property;
    private ItemKind _itemKind;
    private int _itemStart;
    private int _itemDepth;

    private KustoTable? _table;
    private KustoTable? _rowsTable;
    private int _rowStart;
    private int _rowLength;
    private long _rowVersion;
    private KustoRowIndex _rowIndex;
    private long _rowIndexVersion = -1;
    private Exception? _error;

    /// <summary>Creates a reader of the response in <paramref name="stream"/>, which it reads from but doesn't dispose.</summary>
    /// <param name="stream">The response body: a Kusto v2 (non-progressive) query response.</param>
    public KustoTableReader(Stream stream)
        : this(stream, ArrayPool<byte>.Shared, DefaultBufferSize)
    {
    }

    internal KustoTableReader(Stream stream, ArrayPool<byte> pool, int initialBufferSize)
    {
        ArgumentNullException.ThrowIfNull(stream);
        Debug.Assert(initialBufferSize > 0);
        _stream = stream;
        _pool = pool;
        _initialBufferSize = initialBufferSize;
    }

    private enum State : byte
    {
        Bom,
        Start,
        Frames,
        FrameProperty,
        PropertyValue,
        Rows,
        Item,
        End,
    }

    private enum ReadResult : byte
    {
        NeedMoreData,
        Item,
        End,
    }

    private enum ItemKind : byte
    {
        Property,
        Row,
        Error,
    }

    private enum FrameType : byte
    {
        None,
        DataSetHeader,
        DataTable,
        DataSetCompletion,
    }

    private enum Property : byte
    {
        Unknown,
        FrameType,
        IsProgressive,
        TableId,
        TableKind,
        TableName,
        Columns,
        Rows,
        HasErrors,
        Cancelled,
        OneApiErrors,
    }

    /// <summary>
    /// Streams the tables of the response. The tables can be enumerated once; each table's rows must be read
    /// before moving to the next table, otherwise they are skipped.
    /// </summary>
    /// <exception cref="KustoException">The service reported an error, either instead of or within the results.</exception>
    /// <exception cref="JsonException">The response is truncated or is not a valid Kusto v2 response.</exception>
    /// <exception cref="NotSupportedException">The response is progressive.</exception>
    public IAsyncEnumerable<KustoTable> ReadTablesAsync(CancellationToken cancellationToken = default) => new Tables(this, cancellationToken);

    /// <summary>The exception that ended the reading of the response, if any.</summary>
    internal Exception? Error => _error;

    internal bool IsReadingRowsOf(KustoTable table) => _rowsTable == table;

    internal KustoRow GetCurrentRow(KustoTable table) => _rowLength != 0 && _rowsTable == table ? new(table, _rowVersion) : default;

    internal ValueTask<bool> MoveNextRowAsync(KustoTable table, CancellationToken cancellationToken)
        => MoveNextAsync(table, cancellationToken.CanBeCanceled ? cancellationToken : _cancellationToken);

    /// <summary>Ends the rows of <paramref name="table"/>, so that an advance that races the disposal finds no row.</summary>
    internal void DisposeRows(KustoTable table)
    {
        Enter();
        if (_rowsTable == table)
        {
            _rowsTable = null;
            InvalidateRow();
        }

        Exit();
    }

    internal ReadOnlySpan<byte> EnterRow(long version)
    {
        Enter();

        // Current is read without the guard, so a racing advance can mint a row for a version that has no row.
        if (version != _rowVersion || _rowLength == 0)
        {
            Exit();
            throw new ObjectDisposedException(nameof(KustoRow), "The row is only valid until its enumerator advances or is disposed.");
        }

        return _buffer.AsSpan(_rowStart, _rowLength);
    }

    /// <summary>Returns the offset of a value within the current row; called while the row's data is in use.</summary>
    internal int GetValueOffset(int ordinal)
    {
        if (_rowIndexVersion != _rowVersion)
        {
            _rowIndex.Reset(_rowsTable!.ColumnSpan.Length);
            _rowIndexVersion = _rowVersion;
        }

        return _rowIndex.GetOffset(_buffer.AsSpan(_rowStart, _rowLength), ordinal);
    }

    internal void Exit() => Volatile.Write(ref _busy, false);

    private void Enter()
    {
        if (Interlocked.CompareExchange(ref _busy, true, false))
        {
            throw new InvalidOperationException("The reader does not support concurrent operations, nor being advanced or disposed while row data is in use.");
        }
    }

    private void InvalidateRow()
    {
        _rowVersion++;
        _rowLength = 0;
    }

    private void Release()
    {
        _state = State.End;
        _rowsTable = null;
        InvalidateRow();
        _scanStart = _end = 0;
        if (Interlocked.Exchange(ref _buffer, null) is { } buffer)
        {
            _pool.Return(buffer, clearArray: true);
        }
    }

    /// <summary>Advances to the next row of <paramref name="rowsOf"/>, or to the next table when it is null.</summary>
    private ValueTask<bool> MoveNextAsync(KustoTable? rowsOf, CancellationToken cancellationToken)
    {
        Enter();
        ReadResult result;
        try
        {
            result = Advance(rowsOf);
        }
        catch (Exception e)
        {
            _error = e;
            Release();
            Exit();
            throw;
        }

        if (result == ReadResult.NeedMoreData)
        {
            return FillAndAdvanceAsync(rowsOf, cancellationToken);
        }

        Exit();
        return new(result == ReadResult.Item);
    }

    private async ValueTask<bool> FillAndAdvanceAsync(KustoTable? rowsOf, CancellationToken cancellationToken)
    {
        try
        {
            ReadResult result;
            do
            {
                await FillAsync(cancellationToken).ConfigureAwait(false);
                result = Advance(rowsOf);
            }
            while (result == ReadResult.NeedMoreData);

            return result == ReadResult.Item;
        }
        catch (Exception e)
        {
            _error = e;
            Release();
            throw;
        }
        finally
        {
            Exit();
        }
    }

    private async ValueTask FillAsync(CancellationToken cancellationToken)
    {
        if (_isFinalBlock)
        {
            // The JSON reader throws when the final block is truncated, so it never asks for data past the end.
            throw new UnreachableException();
        }

        byte[] buffer = _buffer ??= _pool.Rent(_initialBufferSize);
        int keepFrom = _scanStart;
        if (_state == State.Item)
        {
            keepFrom = _itemStart;
            _itemStart = 0;
        }

        int kept = _end - keepFrom;
        if ((uint)kept > (uint)buffer.Length / 2)
        {
            byte[] larger = _pool.Rent(checked(buffer.Length * 2));
            buffer.AsSpan(keepFrom, kept).CopyTo(larger);
            _buffer = larger;
            _pool.Return(buffer, clearArray: true);
            buffer = larger;
        }
        else if (keepFrom != 0)
        {
            buffer.AsSpan(keepFrom, kept).CopyTo(buffer);
        }

        _scanStart -= keepFrom;
        _end = kept;

        // At least half of the buffer is free here, which is never less than the unscanned data.
        int unscanned = _end - _scanStart;
        int minimum = _stalled && unscanned >= StallThreshold ? unscanned : 1;
        do
        {
            int read = await _stream.ReadAsync(buffer.AsMemory(_end), cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                _isFinalBlock = true;
                return;
            }

            _end += read;
            minimum -= read;
        }
        while (minimum > 0);
    }

    private ReadResult Advance(KustoTable? rowsOf)
    {
        if (rowsOf is not null && rowsOf != _rowsTable)
        {
            return ReadResult.End;
        }

        InvalidateRow();
        if (_state == State.Bom && !TrySkipBom())
        {
            return ReadResult.NeedMoreData;
        }

        // The reader cannot live across an await, so each attempt resumes from the saved state and the position
        // is only committed here: whatever an attempt consumed is reflected in the fields of the state machine.
        Utf8JsonReader reader = new(_buffer.AsSpan(_scanStart, _end - _scanStart), _isFinalBlock, _jsonState);
        ReadResult result = AdvanceCore(ref reader, rowsOf is not null);
        _stalled = reader.BytesConsumed == 0;
        _scanStart += (int)reader.BytesConsumed;
        _jsonState = reader.CurrentState;

        if (result == ReadResult.End && rowsOf is null)
        {
            Release();
        }

        return result;
    }

    private bool TrySkipBom()
    {
        ReadOnlySpan<byte> data = _buffer.AsSpan(_scanStart, _end - _scanStart);
        if (data.Length < Utf8Bom.Length && !_isFinalBlock && Utf8Bom.StartsWith(data))
        {
            return false;
        }

        if (data.StartsWith(Utf8Bom))
        {
            _scanStart += Utf8Bom.Length;
        }

        _state = State.Start;
        return true;
    }

    /// <summary>
    /// Runs the state machine until it reaches a row (when <paramref name="rows"/>) or a table, the end of the
    /// rows or tables, or the end of the buffered data. Every state transition follows a fully read token, so the
    /// machine can be suspended wherever the data happens to end.
    /// </summary>
    private ReadResult AdvanceCore(ref Utf8JsonReader reader, bool rows)
    {
        while (true)
        {
            if (_state == State.End)
            {
                return ReadResult.End;
            }

            if (_state == State.Item)
            {
                // The item ends with the token that returns to its depth, which for a scalar is its only token.
                while (reader.CurrentDepth > _itemDepth || reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray)
                {
                    if (!reader.Read())
                    {
                        return ReadResult.NeedMoreData;
                    }
                }

                ReadOnlySpan<byte> item = _buffer.AsSpan(_itemStart, _scanStart + (int)reader.BytesConsumed - _itemStart);
                switch (_itemKind)
                {
                    case ItemKind.Property:
                        SetProperty(item);
                        _state = State.FrameProperty;
                        break;

                    case ItemKind.Row:
                        _state = State.Rows;
                        if (rows)
                        {
                            _rowStart = _itemStart;
                            _rowLength = item.Length;
                            return ReadResult.Item;
                        }

                        break;

                    default:
                        throw CreateError(item);
                }

                continue;
            }

            if (!reader.Read())
            {
                return ReadResult.NeedMoreData;
            }

            JsonTokenType token = reader.TokenType;
            switch (_state)
            {
                case State.Start when token == JsonTokenType.StartArray:
                    _state = State.Frames;
                    break;

                // A response that is an object rather than an array of frames is an error.
                case State.Start when token == JsonTokenType.StartObject:
                    BeginItem(ref reader, ItemKind.Error);
                    break;

                case State.Frames when token == JsonTokenType.StartObject:
                    _frame = default;
                    _state = State.FrameProperty;
                    break;

                case State.Frames when token == JsonTokenType.EndArray:
                    _state = State.End;
                    break;

                case State.FrameProperty when token == JsonTokenType.EndObject:
                    CompleteFrame();
                    _state = State.Frames;
                    break;

                case State.FrameProperty:
                    _property = GetProperty(ref reader);
                    _state = State.PropertyValue;
                    break;

                case State.PropertyValue when _property != Property.Rows:
                    BeginItem(ref reader, ItemKind.Property);
                    break;

                case State.PropertyValue when token == JsonTokenType.StartArray && _frame is { Type: FrameType.DataTable, Columns: not null }:
                    // Enum parsing also accepts numbers and comma-separated lists, which are not kinds.
                    KustoTableKind kind = Enum.TryParse(_frame.TableKind, out KustoTableKind parsed) && Enum.GetName(parsed) == _frame.TableKind ? parsed : default;
                    _frame.HasRows = true;
                    _table = _rowsTable = new KustoTable(this, _frame.TableId, _frame.TableName ?? "", kind, _frame.Columns);
                    _state = State.Rows;
                    return ReadResult.Item;

                case State.PropertyValue:
                    throw new JsonException("'Rows' must be an array that follows the 'FrameType' and 'Columns' of a DataTable frame.");

                case State.Rows when token == JsonTokenType.StartArray:
                    BeginItem(ref reader, ItemKind.Row);
                    break;

                // With in-data error reporting, a failure during the query takes the place of a row.
                case State.Rows when token == JsonTokenType.StartObject:
                    BeginItem(ref reader, ItemKind.Error);
                    break;

                case State.Rows when token == JsonTokenType.EndArray:
                    _rowsTable = null;
                    _state = State.FrameProperty;
                    if (rows)
                    {
                        return ReadResult.End;
                    }

                    break;

                default:
                    throw new JsonException($"Unexpected '{token}' token in the Kusto response (while reading {_state}).");
            }
        }
    }

    private void BeginItem(ref Utf8JsonReader reader, ItemKind kind)
    {
        _itemKind = kind;
        _itemStart = _scanStart + (int)reader.TokenStartIndex;
        _itemDepth = reader.CurrentDepth;
        _state = State.Item;
    }

    private static Property GetProperty(ref Utf8JsonReader reader) =>
        reader.ValueTextEquals("Rows"u8) ? Property.Rows :
        reader.ValueTextEquals("FrameType"u8) ? Property.FrameType :
        reader.ValueTextEquals("IsProgressive"u8) ? Property.IsProgressive :
        reader.ValueTextEquals("TableId"u8) ? Property.TableId :
        reader.ValueTextEquals("TableKind"u8) ? Property.TableKind :
        reader.ValueTextEquals("TableName"u8) ? Property.TableName :
        reader.ValueTextEquals("Columns"u8) ? Property.Columns :
        reader.ValueTextEquals("HasErrors"u8) ? Property.HasErrors :
        reader.ValueTextEquals("Cancelled"u8) ? Property.Cancelled :
        reader.ValueTextEquals("OneApiErrors"u8) ? Property.OneApiErrors :
        Property.Unknown;

    private void SetProperty(ReadOnlySpan<byte> json)
    {
        switch (_property)
        {
            case Property.FrameType:
                _frame.Type = ReadFrameType(json);
                break;

            case Property.IsProgressive:
                if (ReadBoolean(json))
                {
                    throw new NotSupportedException("Progressive results are not supported and must be disabled (results_progressive_enabled=false).");
                }

                break;

            case Property.TableId:
                _frame.TableId = ReadInt32(json);
                break;

            case Property.TableKind:
                _frame.TableKind = ReadString(json);
                break;

            case Property.TableName:
                _frame.TableName = ReadString(json);
                break;

            case Property.Columns:
                _frame.Columns = JsonSerializer.Deserialize(json, KustoJsonContext.Default.KustoColumnArray);
                if (_frame.Columns is not null && Array.IndexOf(_frame.Columns, null) >= 0)
                {
                    throw new JsonException("A column of a DataTable frame cannot be null.");
                }

                break;

            case Property.HasErrors:
                _frame.HasErrors = ReadBoolean(json);
                break;

            case Property.Cancelled:
                _frame.Cancelled = ReadBoolean(json);
                break;

            case Property.OneApiErrors:
                _frame.Errors = JsonSerializer.Deserialize(json, KustoJsonContext.Default.ErrorPayloadArray);
                break;
        }
    }

    // A property value is a complete JSON value, so its first token is always available.
    private static Utf8JsonReader ReadValue(ReadOnlySpan<byte> json)
    {
        Utf8JsonReader reader = new(json);
        reader.Read();
        return reader;
    }

    private FrameType ReadFrameType(ReadOnlySpan<byte> json)
    {
        Utf8JsonReader reader = ReadValue(json);
        if (reader.TokenType == JsonTokenType.String)
        {
            if (reader.ValueTextEquals("DataTable"u8))
            {
                return FrameType.DataTable;
            }

            if (reader.ValueTextEquals("DataSetHeader"u8))
            {
                return FrameType.DataSetHeader;
            }

            if (reader.ValueTextEquals("DataSetCompletion"u8))
            {
                return FrameType.DataSetCompletion;
            }
        }

        string? type = ReadString(json);
        throw new NotSupportedException($"The frame type '{type}' is not supported. Progressive results must be disabled (results_progressive_enabled=false).");
    }

    private bool ReadBoolean(ReadOnlySpan<byte> json) => ReadValue(json).TokenType switch
    {
        JsonTokenType.True => true,
        JsonTokenType.False => false,
        _ => throw InvalidValue("a boolean"),
    };

    private int ReadInt32(ReadOnlySpan<byte> json)
    {
        Utf8JsonReader reader = ReadValue(json);
        return reader.TokenType == JsonTokenType.Number && reader.TryGetInt32(out int value) ? value : throw InvalidValue("a 32-bit integer");
    }

    private string? ReadString(ReadOnlySpan<byte> json)
    {
        Utf8JsonReader reader = ReadValue(json);
        try
        {
            return reader.TokenType switch
            {
                JsonTokenType.String => reader.GetString(),
                JsonTokenType.Null => null,
                _ => throw InvalidValue("a string"),
            };
        }
        catch (InvalidOperationException e)
        {
            throw new JsonException($"The value of '{_property}' is not valid UTF-8.", e);
        }
    }

    private JsonException InvalidValue(string expected) => new($"The value of '{_property}' must be {expected}.");

    private void CompleteFrame()
    {
        switch (_frame.Type)
        {
            case FrameType.None:
                throw new JsonException("A frame of the Kusto response has no 'FrameType'.");

            case FrameType.DataTable when !_frame.HasRows:
                throw new JsonException("A DataTable frame of the Kusto response has no 'Rows'.");

            case FrameType.DataSetCompletion when _frame.HasErrors || _frame.Cancelled || _frame.Errors is { Length: > 0 }:
                throw new KustoException(
                    ErrorPayload.Flatten(_frame.Errors),
                    _frame.Cancelled ? "The query was cancelled." : "The query completed with errors that the service did not describe.");
        }
    }

    private static Exception CreateError(ReadOnlySpan<byte> json)
    {
        List<KustoError> errors = ErrorPayload.Flatten([JsonSerializer.Deserialize(json, KustoJsonContext.Default.ErrorPayload)]);
        return errors.Count != 0 ? new KustoException(errors) : new JsonException("Unexpected JSON object in the Kusto response.");
    }

    private struct Frame
    {
        public FrameType Type;
        public int TableId;
        public string? TableKind;
        public string? TableName;
        public KustoColumn[]? Columns;
        public ErrorPayload?[]? Errors;
        public bool HasRows;
        public bool HasErrors;
        public bool Cancelled;
    }

    private sealed class Tables(KustoTableReader reader, CancellationToken cancellationToken) : IAsyncEnumerable<KustoTable>, IAsyncEnumerator<KustoTable>
    {
        public KustoTable Current => reader._table!;

        public IAsyncEnumerator<KustoTable> GetAsyncEnumerator(CancellationToken enumeratorCancellationToken)
        {
            if (Interlocked.Exchange(ref reader._started, true))
            {
                throw new InvalidOperationException("The tables of a reader can only be enumerated once.");
            }

            reader._cancellationToken = enumeratorCancellationToken.CanBeCanceled ? enumeratorCancellationToken : cancellationToken;
            return this;
        }

        public ValueTask<bool> MoveNextAsync() => reader.MoveNextAsync(null, reader._cancellationToken);

        public ValueTask DisposeAsync()
        {
            reader.Enter();
            reader.Release();
            reader.Exit();
            return default;
        }
    }
}

using System.Text.Json;

namespace Kusto.Data.Queries;

/// <summary>The offsets of the values of a row, which are found only as far as they are asked for.</summary>
internal struct KustoRowIndex
{
    private int[]? _offsets;
    private int _count;
    private int _scanned;
    private JsonReaderState _state;

    /// <summary>Starts the index of another row, of a table with <paramref name="columns"/> columns.</summary>
    public void Reset(int columns)
    {
        if (_offsets?.Length != columns)
        {
            _offsets = new int[columns];
        }

        _count = _scanned = 0;
        _state = default;
    }

    /// <summary>Returns the offset of the value at <paramref name="ordinal"/> in <paramref name="row"/>, a well-formed JSON array.</summary>
    /// <exception cref="JsonException">The row has no value at <paramref name="ordinal"/>.</exception>
    public int GetOffset(ReadOnlySpan<byte> row, int ordinal)
    {
        if (ordinal >= _count)
        {
            Utf8JsonReader reader = new(row[_scanned..], isFinalBlock: true, _state);
            if (_scanned == 0)
            {
                reader.Read();
            }

            // The position is only committed once the value is found, so a row that is too short leaves the index as it was.
            int count = _count;
            do
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.EndArray)
                {
                    throw KustoRowValues.TooFewValues(count, _offsets!.Length);
                }

                _offsets![count++] = _scanned + (int)reader.TokenStartIndex;
                reader.Skip();
            }
            while (count <= ordinal);

            _count = count;
            _scanned += (int)reader.BytesConsumed;
            _state = reader.CurrentState;
        }

        return _offsets![ordinal];
    }
}

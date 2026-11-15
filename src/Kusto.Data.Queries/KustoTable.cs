namespace Kusto.Data.Queries;

/// <summary>A table of a Kusto query response, whose rows are streamed by the <see cref="KustoTableReader"/> that produced it.</summary>
public sealed class KustoTable
{
    private readonly KustoColumn[] _columns;
    private bool _rowsStarted;

    internal KustoTable(KustoTableReader reader, int id, string name, KustoTableKind kind, KustoColumn[] columns)
    {
        Reader = reader;
        Id = id;
        Name = name;
        Kind = kind;
        _columns = columns;
    }

    /// <summary>The id of the table within the response.</summary>
    public int Id { get; }

    /// <summary>The name of the table, such as <c>PrimaryResult</c>; empty when the response has none.</summary>
    public string Name { get; }

    /// <summary>The role of the table within the response.</summary>
    public KustoTableKind Kind { get; }

    /// <summary>The columns of the table, in the order of the values of each row.</summary>
    public IReadOnlyList<KustoColumn> Columns => _columns;

    /// <summary>Returns the index of the column named <paramref name="columnName"/> (case-sensitive).</summary>
    /// <exception cref="ArgumentException">The table has no column named <paramref name="columnName"/>.</exception>
    public int GetColumnIndex(string columnName)
    {
        ArgumentNullException.ThrowIfNull(columnName);
        for (var i = 0; i < _columns.Length; i++)
        {
            if (_columns[i].Name == columnName)
            {
                return i;
            }
        }

        throw new ArgumentException($"The table has no column named '{columnName}'.", nameof(columnName));
    }

    internal KustoTableReader Reader { get; }

    internal ReadOnlySpan<KustoColumn> ColumnSpan => _columns;

    /// <summary>
    /// Streams the rows of the table. The rows can be enumerated once, and only while this is the reader's current
    /// table; rows that are not enumerated are skipped when the reader moves to the next table.
    /// </summary>
    public IAsyncEnumerable<KustoRow> GetRowsAsync(CancellationToken cancellationToken = default) => new Rows(this, cancellationToken);

    private sealed class Rows(KustoTable table, CancellationToken cancellationToken) : IAsyncEnumerable<KustoRow>, IAsyncEnumerator<KustoRow>
    {
        private bool _disposed;

        public KustoRow Current => table.Reader.GetCurrentRow(table);

        public IAsyncEnumerator<KustoRow> GetAsyncEnumerator(CancellationToken enumeratorCancellationToken)
        {
            if (Interlocked.Exchange(ref table._rowsStarted, true))
            {
                throw new InvalidOperationException("The rows of a table can only be enumerated once.");
            }

            if (!table.Reader.IsReadingRowsOf(table))
            {
                throw new InvalidOperationException("The rows of the table are no longer available because the reader has moved past it.");
            }

            if (enumeratorCancellationToken.CanBeCanceled)
            {
                cancellationToken = enumeratorCancellationToken;
            }

            return this;
        }

        public ValueTask<bool> MoveNextAsync() => _disposed ? new(false) : table.Reader.MoveNextRowAsync(table, cancellationToken);

        public ValueTask DisposeAsync()
        {
            // The guard may reject the call, which must then leave the enumeration as it was.
            table.Reader.DisposeRows(table);
            _disposed = true;
            return default;
        }
    }
}

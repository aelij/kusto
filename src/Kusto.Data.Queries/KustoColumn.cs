using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace Kusto.Data.Queries;

/// <summary>A column of a <see cref="KustoTable"/>.</summary>
/// <param name="Name">The name of the column.</param>
/// <param name="Type">The Kusto data type of the column's values.</param>
public sealed record KustoColumn(
    [property: JsonPropertyName("ColumnName")] string Name,
    [property: JsonPropertyName("ColumnType")] KustoColumnType Type);

/// <summary>The Kusto scalar data types, and the .NET types that <see cref="KustoRow.ToValues()"/> converts them to.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<KustoColumnType>))]
[SuppressMessage("Naming", "CA1720:Identifier contains type name", Justification = "The members are named after the Kusto types, which is also how they are matched when deserialized.")]
public enum KustoColumnType
{
    /// <summary><c>bool</c>, converted to <see cref="bool"/>.</summary>
    Bool,

    /// <summary><c>datetime</c>, converted to a UTC <see cref="System.DateTime"/>.</summary>
    DateTime,

    /// <summary><c>dynamic</c>, converted to a <see cref="System.Text.Json.JsonElement"/>.</summary>
    Dynamic,

    /// <summary><c>guid</c>, converted to <see cref="System.Guid"/>.</summary>
    Guid,

    /// <summary><c>int</c>, converted to <see cref="int"/>.</summary>
    Int,

    /// <summary><c>long</c>, converted to <see cref="long"/>.</summary>
    Long,

    /// <summary><c>real</c>, converted to <see cref="double"/>, including the non-finite values.</summary>
    Real,

    /// <summary><c>string</c>, converted to <see cref="string"/>.</summary>
    String,

    /// <summary><c>timespan</c>, converted to <see cref="System.TimeSpan"/>.</summary>
    TimeSpan,

    /// <summary><c>decimal</c>, converted to <see cref="decimal"/>.</summary>
    Decimal,
}

/// <summary>The role of a table within a query response.</summary>
public enum KustoTableKind
{
    /// <summary>The kind is missing or not one of the known kinds.</summary>
    Unknown,

    /// <summary>A result of the query.</summary>
    PrimaryResult,

    /// <summary>The query's completion status, resource usage and warnings.</summary>
    QueryCompletionInformation,

    /// <summary>The query's trace log.</summary>
    QueryTraceLog,

    /// <summary>The query's performance log.</summary>
    QueryPerfLog,

    /// <summary>The list of the response's tables (v1 responses).</summary>
    TableOfContents,

    /// <summary>The properties of the query and its results, such as visualization hints.</summary>
    QueryProperties,

    /// <summary>The query's execution plan.</summary>
    QueryPlan,
}

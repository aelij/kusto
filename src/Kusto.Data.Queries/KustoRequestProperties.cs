using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace Kusto.Data.Queries;

/// <summary>
/// The request properties of a query or a management command. Unset properties are not sent, so the service defaults apply.
/// See <see href="https://learn.microsoft.com/kusto/api/rest/request-properties">request properties</see>.
/// </summary>
/// <remarks>Progressive results are always disabled, since the client only supports non-progressive responses.</remarks>
public sealed class KustoRequestProperties
{
    internal static readonly KustoRequestProperties Default = new();

    private Dictionary<string, object>? _parameters;

    /// <summary>
    /// Identifies the request (the <c>x-ms-client-request-id</c> header); generated when not set. It is also used to
    /// cancel a query on the service when its request is cancelled.
    /// </summary>
    [JsonIgnore]
    public string? ClientRequestId { get; set; }

    /// <summary>
    /// The values of the parameters that the query declares with <c>declare query_parameters</c>. Supported values are
    /// <see cref="string"/>, <see cref="bool"/>, <see cref="int"/>, <see cref="long"/>, <see cref="double"/>,
    /// <see cref="decimal"/>, <see cref="DateTime"/>, <see cref="DateTimeOffset"/>, <see cref="TimeSpan"/>,
    /// <see cref="Guid"/>, and <see cref="System.Text.Json.JsonElement"/> or <see cref="JsonNode"/> for <c>dynamic</c>.
    /// </summary>
    [JsonIgnore]
    public IDictionary<string, object> Parameters => _parameters ??= new(StringComparer.Ordinal);

    /// <summary>Options that have no dedicated property, written as is.</summary>
    [JsonExtensionData]
    public JsonObject? AdditionalOptions { get; set; }

    [JsonInclude]
    [JsonPropertyName("results_progressive_enabled")]
    [SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "Serialized as an option.")]
    internal bool ResultsProgressiveEnabled => false;

    internal IReadOnlyDictionary<string, object>? ParametersOrNull => _parameters;

    /// <summary>Allows fuzzy resolution of union legs: sources that don't exist or are inaccessible are skipped with a warning.</summary>
    [JsonPropertyName("best_effort")]
    public bool? BestEffort { get; set; }

    /// <summary>Suppresses reporting of partial query failures within the result set.</summary>
    [JsonPropertyName("deferpartialqueryfailures")]
    public bool? DeferPartialQueryFailures { get; set; }

    /// <summary>Hints to use the shuffle strategy for the referenced materialized views.</summary>
    [JsonPropertyName("materialized_view_shuffle_query")]
    public IList<KustoMaterializedViewShuffleHint>? MaterializedViewShuffleQuery { get; set; }

    /// <summary>Overrides the maximum amount of memory a query can allocate per node.</summary>
    [JsonPropertyName("max_memory_consumption_per_query_per_node")]
    public long? MaxMemoryConsumptionPerQueryPerNode { get; set; }

    /// <summary>Overrides the maximum amount of memory a query operator can allocate.</summary>
    [JsonPropertyName("maxmemoryconsumptionperiterator")]
    public long? MaxMemoryConsumptionPerIterator { get; set; }

    /// <summary>Overrides the maximum number of columns a query is allowed to produce.</summary>
    [JsonPropertyName("maxoutputcolumns")]
    public long? MaxOutputColumns { get; set; }

    /// <summary>Sets the request timeout to its maximum value.</summary>
    [JsonPropertyName("norequesttimeout")]
    public bool? NoRequestTimeout { get; set; }

    /// <summary>Disables truncation of the query results.</summary>
    [JsonPropertyName("notruncation")]
    public bool? NoTruncation { get; set; }

    /// <summary>Allows pushing simple selection through aggregation.</summary>
    [JsonPropertyName("push_selection_through_aggregation")]
    public bool? PushSelectionThroughAggregation { get; set; }

    /// <summary>The start value (a KQL literal) to use when evaluating <c>bin_auto()</c>.</summary>
    [JsonPropertyName("query_bin_auto_at")]
    public string? QueryBinAutoAt { get; set; }

    /// <summary>The bin size (a KQL literal) to use when evaluating <c>bin_auto()</c>.</summary>
    [JsonPropertyName("query_bin_auto_size")]
    public string? QueryBinAutoSize { get; set; }

    /// <summary>The default value of <c>cursor_after()</c> when called without parameters.</summary>
    [JsonPropertyName("query_cursor_after_default")]
    public string? QueryCursorAfterDefault { get; set; }

    /// <summary>The default value of <c>cursor_before_or_at()</c> when called without parameters.</summary>
    [JsonPropertyName("query_cursor_before_or_at_default")]
    public string? QueryCursorBeforeOrAtDefault { get; set; }

    /// <summary>Overrides the value returned by <c>cursor_current()</c>.</summary>
    [JsonPropertyName("query_cursor_current")]
    public string? QueryCursorCurrent { get; set; }

    /// <summary>Disables the cursor functions.</summary>
    [JsonPropertyName("query_cursor_disabled")]
    public bool? QueryCursorDisabled { get; set; }

    /// <summary>The tables to scope to the default cursor range.</summary>
    [JsonPropertyName("query_cursor_scoped_tables")]
    public IList<string>? QueryCursorScopedTables { get; set; }

    /// <summary>Controls the data to which the query applies.</summary>
    [JsonPropertyName("query_datascope")]
    public KustoQueryDataScope? QueryDataScope { get; set; }

    /// <summary>The column that <see cref="QueryDateTimeScopeFrom"/> and <see cref="QueryDateTimeScopeTo"/> filter on.</summary>
    [JsonPropertyName("query_datetimescope_column")]
    public string? QueryDateTimeScopeColumn { get; set; }

    /// <summary>The minimum value of <see cref="QueryDateTimeScopeColumn"/>.</summary>
    [JsonPropertyName("query_datetimescope_from")]
    public DateTime? QueryDateTimeScopeFrom { get; set; }

    /// <summary>The maximum value of <see cref="QueryDateTimeScopeColumn"/>.</summary>
    [JsonPropertyName("query_datetimescope_to")]
    public DateTime? QueryDateTimeScopeTo { get; set; }

    /// <summary>The size of the node subgroups that subquery merges are done in.</summary>
    [JsonPropertyName("query_distribution_nodes_span")]
    public int? QueryDistributionNodesSpan { get; set; }

    /// <summary>The percentage of nodes to fan out execution to.</summary>
    [JsonPropertyName("query_fanout_nodes_percent")]
    public int? QueryFanoutNodesPercent { get; set; }

    /// <summary>The percentage of threads to fan out execution to.</summary>
    [JsonPropertyName("query_fanout_threads_percent")]
    public int? QueryFanoutThreadsPercent { get; set; }

    /// <summary>Enforces row level security rules, even if the policy is disabled.</summary>
    [JsonPropertyName("query_force_row_level_security")]
    public bool? QueryForceRowLevelSecurity { get; set; }

    /// <summary>How the query text is interpreted.</summary>
    [JsonPropertyName("query_language")]
    public KustoQueryLanguage? QueryLanguage { get; set; }

    /// <summary>Logs the query parameters in the <c>.show queries</c> journal.</summary>
    [JsonPropertyName("query_log_query_parameters")]
    public bool? QueryLogQueryParameters { get; set; }

    /// <summary>Overrides the maximum number of entities in a union.</summary>
    [JsonPropertyName("query_max_entities_in_union")]
    public long? QueryMaxEntitiesInUnion { get; set; }

    /// <summary>Overrides the value returned by <c>now()</c>.</summary>
    [JsonPropertyName("query_now")]
    public DateTime? QueryNow { get; set; }

    /// <summary>Enables an experimental optimization of costly free-text search operations.</summary>
    [JsonPropertyName("query_optimize_fts_at_relop")]
    public bool? QueryOptimizeFtsAtRelop { get; set; }

    /// <summary>Generates a Python debug query for the enumerated Python node.</summary>
    [JsonPropertyName("query_python_debug")]
    public bool? QueryPythonDebug { get; set; }

    /// <summary>Returns the schema of each table in the results instead of its data.</summary>
    [JsonPropertyName("query_results_apply_getschema")]
    public bool? QueryResultsApplyGetSchema { get; set; }

    /// <summary>Forces a refresh of the cached results; requires <see cref="QueryResultsCacheMaxAge"/>.</summary>
    [JsonPropertyName("query_results_cache_force_refresh")]
    public bool? QueryResultsCacheForceRefresh { get; set; }

    /// <summary>The maximum age of cached results that the service is allowed to return.</summary>
    [JsonPropertyName("query_results_cache_max_age")]
    public TimeSpan? QueryResultsCacheMaxAge { get; set; }

    /// <summary>Enables per-extent query results caching.</summary>
    [JsonPropertyName("query_results_cache_per_shard")]
    public bool? QueryResultsCachePerShard { get; set; }

    /// <summary>Limits the query results to the given number of records.</summary>
    [JsonPropertyName("query_take_max_records")]
    public long? QueryTakeMaxRecords { get; set; }

    /// <summary>The session of <see cref="KustoQueryConsistency.WeakBySessionId"/> consistency.</summary>
    [JsonPropertyName("query_weakconsistency_session_id")]
    public string? QueryWeakConsistencySessionId { get; set; }

    /// <summary>Controls the query consistency. Not available in Fabric.</summary>
    [JsonPropertyName("queryconsistency")]
    public KustoQueryConsistency? QueryConsistency { get; set; }

    /// <summary>The application name to report, for example in <c>.show queries</c>.</summary>
    [JsonPropertyName("request_app_name")]
    public string? RequestAppName { get; set; }

    /// <summary>Blocks access to tables with an enabled row level security policy.</summary>
    [JsonPropertyName("request_block_row_level_security")]
    public bool? RequestBlockRowLevelSecurity { get; set; }

    /// <summary>Prevents callouts to user-provided services.</summary>
    [JsonPropertyName("request_callout_disabled")]
    public bool? RequestCalloutDisabled { get; set; }

    /// <summary>Arbitrary text that describes the request.</summary>
    [JsonPropertyName("request_description")]
    public string? RequestDescription { get; set; }

    /// <summary>Prevents access to external data through <c>externaldata</c> or external tables.</summary>
    [JsonPropertyName("request_external_data_disabled")]
    public bool? RequestExternalDataDisabled { get; set; }

    /// <summary>Prevents access to external tables.</summary>
    [JsonPropertyName("request_external_table_disabled")]
    public bool? RequestExternalTableDisabled { get; set; }

    /// <summary>Prevents the service from using the caller's identity to access downstream services.</summary>
    [JsonPropertyName("request_impersonation_disabled")]
    public bool? RequestImpersonationDisabled { get; set; }

    /// <summary>Prevents write access.</summary>
    [JsonPropertyName("request_readonly")]
    public bool? RequestReadOnly { get; set; }

    /// <summary>Runs the request in a strict read-only mode, which also disables noncompliant functionality such as plugins.</summary>
    [JsonPropertyName("request_readonly_hardline")]
    public bool? RequestReadOnlyHardline { get; set; }

    /// <summary>Prevents access to remote databases and entities.</summary>
    [JsonPropertyName("request_remote_entities_disabled")]
    public bool? RequestRemoteEntitiesDisabled { get; set; }

    /// <summary>Prevents invoking code in the sandbox.</summary>
    [JsonPropertyName("request_sandboxed_execution_disabled")]
    public bool? RequestSandboxedExecutionDisabled { get; set; }

    /// <summary>Identifies the user on whose behalf the request is made, in service-side traces only.</summary>
    [JsonPropertyName("request_user")]
    public string? RequestUser { get; set; }

    /// <summary>Where errors are placed in the result set.</summary>
    [JsonPropertyName("results_error_reporting_placement")]
    public KustoErrorReportingPlacement? ResultsErrorReportingPlacement { get; set; }

    /// <summary>
    /// Overrides the request timeout. The client waits this long (or, when not set, 4 minutes for a query and 10 for a
    /// command) plus a grace period before it gives up on the response.
    /// </summary>
    [JsonPropertyName("servertimeout")]
    public TimeSpan? ServerTimeout { get; set; }

    /// <summary>Overrides the maximum number of records the query may return before it's truncated.</summary>
    [JsonPropertyName("truncationmaxrecords")]
    public long? TruncationMaxRecords { get; set; }

    /// <summary>Overrides the maximum data size the query may return before it's truncated.</summary>
    [JsonPropertyName("truncationmaxsize")]
    public long? TruncationMaxSize { get; set; }

    /// <summary>Validates the permissions to run the query without running it.</summary>
    [JsonPropertyName("validatepermissions")]
    public bool? ValidatePermissions { get; set; }
}

/// <summary>A materialized view to shuffle, by the given keys or, when there are none, by all of its keys.</summary>
/// <param name="Name">The name of the materialized view.</param>
/// <param name="Keys">The keys to shuffle by, or <see langword="null"/> for all of the view's keys.</param>
public sealed record KustoMaterializedViewShuffleHint(
    [property: JsonPropertyName("Name")] string Name,
    [property: JsonPropertyName("Keys")] IReadOnlyList<string>? Keys = null);

/// <summary>The data a query applies to.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<KustoQueryDataScope>))]
public enum KustoQueryDataScope
{
    /// <summary>The default data scope (<c>default</c>).</summary>
    [JsonStringEnumMemberName("default")]
    Default,

    /// <summary>All the data, hot and cold (<c>all</c>).</summary>
    [JsonStringEnumMemberName("all")]
    All,

    /// <summary>Only the data in the hot cache (<c>hotcache</c>).</summary>
    [JsonStringEnumMemberName("hotcache")]
    HotCache,
}

/// <summary>The language of the query text.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<KustoQueryLanguage>))]
public enum KustoQueryLanguage
{
    /// <summary>Kusto Query Language (<c>kql</c>).</summary>
    [JsonStringEnumMemberName("kql")]
    Kql,

    /// <summary>T-SQL (<c>sql</c>).</summary>
    [JsonStringEnumMemberName("sql")]
    Sql,
}

/// <summary>The consistency of a query.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<KustoQueryConsistency>))]
public enum KustoQueryConsistency
{
    /// <summary>Strong consistency (<c>strongconsistency</c>): queries see all the committed data.</summary>
    [JsonStringEnumMemberName("strongconsistency")]
    Strong,

    /// <summary>Weak consistency (<c>weakconsistency</c>): queries may see slightly stale data, in exchange for less load on the cluster.</summary>
    [JsonStringEnumMemberName("weakconsistency")]
    Weak,

    /// <summary>Weak consistency with the same node per query text (<c>weakconsistency_by_query</c>).</summary>
    [JsonStringEnumMemberName("weakconsistency_by_query")]
    WeakByQuery,

    /// <summary>Weak consistency with the same node per database (<c>weakconsistency_by_database</c>).</summary>
    [JsonStringEnumMemberName("weakconsistency_by_database")]
    WeakByDatabase,

    /// <summary>Weak consistency with the same node per <see cref="KustoRequestProperties.QueryWeakConsistencySessionId"/> (<c>weakconsistency_by_session_id</c>).</summary>
    [JsonStringEnumMemberName("weakconsistency_by_session_id")]
    WeakBySessionId,
}

/// <summary>Where errors are placed in the result set.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<KustoErrorReportingPlacement>))]
public enum KustoErrorReportingPlacement
{
    /// <summary>Errors take the place of rows, where they occurred (<c>in_data</c>).</summary>
    [JsonStringEnumMemberName("in_data")]
    InData,

    /// <summary>Errors are reported at the end of the table (<c>end_of_table</c>).</summary>
    [JsonStringEnumMemberName("end_of_table")]
    EndOfTable,

    /// <summary>Errors are reported at the end of the response (<c>end_of_dataset</c>).</summary>
    [JsonStringEnumMemberName("end_of_dataset")]
    EndOfDataSet,
}

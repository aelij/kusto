using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Text.Json;
using AwesomeAssertions;

namespace Kusto.Data.Queries.Tests;

[TestClass]
public class KustoRowValuesTests
{
    private const KustoColumnType KBool = KustoColumnType.Bool, KDateTime = KustoColumnType.DateTime, KDynamic = KustoColumnType.Dynamic,
        KGuid = KustoColumnType.Guid, KInt = KustoColumnType.Int, KLong = KustoColumnType.Long, KReal = KustoColumnType.Real,
        KString = KustoColumnType.String, KTimeSpan = KustoColumnType.TimeSpan, KDecimal = KustoColumnType.Decimal;

    private static readonly KustoColumnType[] AllTypes = [KBool, KDateTime, KDynamic, KGuid, KInt, KLong, KReal, KString, KTimeSpan, KDecimal];

    private static object?[] Parse(string json, params KustoColumnType[] types) =>
        KustoRowValues.ToValues(Encoding.UTF8.GetBytes(json), types.Select((t, i) => new KustoColumn($"c{i}", t)).ToArray());

    private static object? ParseOne(string value, KustoColumnType type) => Parse($"[{value}]", type)[0];

    private static string Escape(string value) => "\"" + string.Concat(value.Select(c => $"\\u{(int)c:x4}")) + "\"";

    private static void AssertRejected(string json, params KustoColumnType[] types) =>
        ((Action)(() => Parse(json, types))).Should().ThrowExactly<JsonException>().WithMessage("*c0*");

    [TestMethod]
    public void AllTypes_FirstRow()
    {
        var values = Parse(
            """[true, "2020-01-02T03:04:05.1234567Z", {"a": [1, 2, {"b": null}], "s": "x\"y"}, "74be27de-1e4e-49d9-b579-fe0b331d3642", 42, 9223372036854775807, 1.5, "hello \"world\" \u00e9 \u2603", "1.02:03:04.5670000", "1.2345678901234567890123456789"]""",
            AllTypes);

        values[0].Should().BeOfType<bool>().Which.Should().BeTrue();
        var dt = values[1].Should().BeOfType<DateTime>().Subject;
        dt.Should().Be(new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc).AddTicks(1234567));
        dt.Kind.Should().Be(DateTimeKind.Utc);
        var dyn = values[2].Should().BeOfType<JsonElement>().Subject;
        dyn.GetProperty("a")[2].GetProperty("b").ValueKind.Should().Be(JsonValueKind.Null);
        dyn.GetProperty("s").GetString().Should().Be("x\"y");
        values[3].Should().BeOfType<Guid>().Which.Should().Be(new Guid("74be27de-1e4e-49d9-b579-fe0b331d3642"));
        values[4].Should().BeOfType<int>().Which.Should().Be(42);
        values[5].Should().BeOfType<long>().Which.Should().Be(long.MaxValue);
        values[6].Should().BeOfType<double>().Which.Should().Be(1.5);
        values[7].Should().BeOfType<string>().Which.Should().Be("hello \"world\" \u00e9 \u2603");
        values[8].Should().BeOfType<TimeSpan>().Which.Should().Be(new TimeSpan(1, 2, 3, 4, 567));
        values[9].Should().BeOfType<decimal>().Which.Should().Be(1.2345678901234567890123456789m);
    }

    [TestMethod]
    public void AllTypes_NullsAndEmptyString()
    {
        var values = Parse("""[false, null, null, null, null, null, null, "", null, null]""", AllTypes);
        values.Should().Equal(false, null, null, null, null, null, null, "", null, null);
    }

    [TestMethod]
    public void AllTypes_Extremes()
    {
        var values = Parse(
            """[null, "0001-01-01T00:00:00Z", [1, "two", 3.5, true, null], "00000000-0000-0000-0000-000000000000", -2147483648, -9223372036854775808, "NaN", "multi\nline\ttab", "-00:00:01", "-0.5"]""",
            AllTypes);
        values[0].Should().BeNull();
        values[1].Should().Be(new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        values[2].Should().BeOfType<JsonElement>().Which.GetArrayLength().Should().Be(5);
        values[3].Should().Be(System.Guid.Empty);
        values[4].Should().Be(int.MinValue);
        values[5].Should().Be(long.MinValue);
        double.IsNaN((double)values[6]!).Should().BeTrue();
        values[7].Should().Be("multi\nline\ttab");
        values[8].Should().Be(-System.TimeSpan.FromSeconds(1));
        values[9].Should().Be(-0.5m);

        values = Parse(
            """[true, "9999-12-31T23:59:59.9999999Z", "just a string", "74be27de-1e4e-49d9-b579-fe0b331d3642", 0, 0, "Infinity", "x", "10675199.02:48:05.4775807", "79228162514264337593543950335"]""",
            AllTypes);
        values[1].Should().Be(System.DateTime.SpecifyKind(System.DateTime.MaxValue, DateTimeKind.Utc));
        ((JsonElement)values[2]!).GetString().Should().Be("just a string");
        values[6].Should().Be(double.PositiveInfinity);
        values[8].Should().Be(System.TimeSpan.MaxValue);
        values[9].Should().Be(decimal.MaxValue);
    }

    [TestMethod]
    public void AllTypes_RemainingRows()
    {
        var values = Parse(
            """[true, "2020-01-02T00:00:00Z", 123, "74be27de-1e4e-49d9-b579-fe0b331d3642", 1, 1, "-Infinity", "y", "00:00:00", "0"]""", AllTypes);
        ((JsonElement)values[2]!).GetInt32().Should().Be(123);
        values[6].Should().Be(double.NegativeInfinity);
        values[8].Should().Be(System.TimeSpan.Zero);
        values[9].Should().Be(0m);

        values = Parse(
            """[true, "2020-01-02T03:04:05Z", 1.5, "74be27de-1e4e-49d9-b579-fe0b331d3642", 1, 1, 1e+300, "z", "00:00:00.0000001", "0.0000000001"]""", AllTypes);
        ((JsonElement)values[2]!).GetDouble().Should().Be(1.5);
        values[6].Should().Be(1e300);
        values[8].Should().Be(System.TimeSpan.FromTicks(1));
        values[9].Should().Be(0.0000000001m);
    }

    [TestMethod]
    public void DynamicScalars()
    {
        ParseOne("true", KDynamic).Should().BeOfType<JsonElement>().Which.GetBoolean().Should().BeTrue();
        ParseOne("{}", KDynamic).Should().BeOfType<JsonElement>().Which.ValueKind.Should().Be(JsonValueKind.Object);
    }

    [TestMethod]
    public void RealAndDecimalAcceptNumbers()
    {
        ParseOne("-3", KReal).Should().Be(-3d);
        ParseOne("1.25", KDecimal).Should().Be(1.25m);
        ParseOne("7", KDecimal).Should().Be(7m);
    }

    [TestMethod]
    public void Whitespace_IsIgnored()
    {
        var values = Parse(" [ true , { \"a\" : [ 1 , 2 ] } , \"x\" ,\n 5 ] \n", KBool, KDynamic, KString, KInt);
        values[0].Should().Be(true);
        ((JsonElement)values[1]!).GetProperty("a").GetArrayLength().Should().Be(2);
        values[2].Should().Be("x");
        values[3].Should().Be(5);
    }

    [TestMethod]
    public void EscapedStrings_AreUnescapedBeforeParsing()
    {
        ParseOne("\"2020-01-02T03:04:05\\u005A\"", KDateTime)
            .Should().Be(new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc));
        ParseOne("\"74be27de-1e4e-49d9-b579-fe0b331d3642\"".Replace("74be", "\\u0037\\u0034be"), KGuid)
            .Should().Be(new Guid("74be27de-1e4e-49d9-b579-fe0b331d3642"));
        ParseOne("\"1.02:03:04.5670000\"".Replace("1.02", "\\u0031.0\\u0032"), KTimeSpan).Should().Be(new TimeSpan(1, 2, 3, 4, 567));
        ParseOne("\"\\u002D0.5\"", KDecimal).Should().Be(-0.5m);
        ParseOne("\"\\u002DInfinity\"", KReal).Should().Be(double.NegativeInfinity);
        ParseOne("\"\\u004EaN\"", KReal).Should().Be(double.NaN);
    }

    [TestMethod]
    public void FullyEscapedDecimal_IsAccepted() =>
        ParseOne(Escape("79228162514264337593543950335"), KDecimal).Should().Be(decimal.MaxValue);

    [TestMethod]
    public void FullyEscapedTimeSpan_IsAccepted() =>
        ParseOne(Escape("10675199.02:48:05.4775807"), KTimeSpan).Should().Be(System.TimeSpan.MaxValue);

    [TestMethod]
    public void EscapedStringLongerThanBuffer_IsRejected() =>
        AssertRejected($"[\"{new string('1', 200)}\\u0031\"]", KDecimal);

    [TestMethod]
    public void NonUtcDateTimes_AreNormalizedToUtc()
    {
        ParseOne("\"2020-01-02T03:04:05+02:00\"", KDateTime).Should().Be(new DateTime(2020, 1, 2, 1, 4, 5, DateTimeKind.Utc));
        ((DateTime)ParseOne("\"2020-01-02T03:04:05\"", KDateTime)!).Kind.Should().Be(DateTimeKind.Utc);
    }

    [TestMethod]
    [DataRow("\"x\"", KBool)]
    [DataRow("1", KBool)]
    [DataRow("{}", KBool)]
    [DataRow("\"1\"", KInt)]
    [DataRow("true", KInt)]
    [DataRow("{}", KInt)]
    [DataRow("[]", KInt)]
    [DataRow("\"1\"", KLong)]
    [DataRow("false", KLong)]
    [DataRow("{}", KLong)]
    [DataRow("true", KReal)]
    [DataRow("{}", KReal)]
    [DataRow("[1]", KReal)]
    [DataRow("true", KDecimal)]
    [DataRow("{}", KDecimal)]
    [DataRow("1", KDateTime)]
    [DataRow("true", KDateTime)]
    [DataRow("[]", KDateTime)]
    [DataRow("1", KTimeSpan)]
    [DataRow("true", KTimeSpan)]
    [DataRow("{}", KTimeSpan)]
    [DataRow("1", KGuid)]
    [DataRow("true", KGuid)]
    [DataRow("[]", KGuid)]
    [DataRow("1", KString)]
    [DataRow("true", KString)]
    [DataRow("{}", KString)]
    [DataRow("[\"a\"]", KString)]
    public void WrongTokenKind_Throws(string value, KustoColumnType type) => AssertRejected($"[{value}]", type);

    [TestMethod]
    [DataRow("2147483648", KInt)]
    [DataRow("-2147483649", KInt)]
    [DataRow("1.5", KInt)]
    [DataRow("1e2", KInt)]
    [DataRow("9223372036854775808", KLong)]
    [DataRow("-9223372036854775809", KLong)]
    [DataRow("1.5", KLong)]
    [DataRow("1e400", KReal)]
    [DataRow("1e400", KDecimal)]
    public void BadNumbers_Throw(string value, KustoColumnType type) => AssertRejected($"[{value}]", type);

    [TestMethod]
    [DataRow("\"abc\"", KReal)]
    [DataRow("\"\"", KReal)]
    [DataRow("\"1.5\"", KReal)]
    [DataRow("\"1e400\"", KReal)]
    [DataRow("\"-1e400\"", KReal)]
    [DataRow("\"nan\"", KReal)]
    [DataRow("\"Inf\"", KReal)]
    [DataRow("\"+Infinity\"", KReal)]
    [DataRow("\"abc\"", KDecimal)]
    [DataRow("\"\"", KDecimal)]
    [DataRow("\"79228162514264337593543950336\"", KDecimal)]
    [DataRow("\"yesterday\"", KDateTime)]
    [DataRow("\"2020-13-45T00:00:00Z\"", KDateTime)]
    [DataRow("\"\"", KDateTime)]
    [DataRow("\"abc\"", KTimeSpan)]
    [DataRow("\"\"", KTimeSpan)]
    [DataRow("\"00:00:01 \"", KTimeSpan)]
    [DataRow("\"00:00:01x\"", KTimeSpan)]
    [DataRow("\"10675199.02:48:08.4775808\"", KTimeSpan)]
    [DataRow("\"not-a-guid\"", KGuid)]
    [DataRow("\"\"", KGuid)]
    [DataRow("\"74be27de-1e4e-49d9-b579-fe0b331d364\"", KGuid)]
    public void UnparsableStrings_Throw(string value, KustoColumnType type) => AssertRejected($"[{value}]", type);

    [TestMethod]
    [DataRow(KString)]
    [DataRow(KReal)]
    [DataRow(KDecimal)]
    [DataRow(KDateTime)]
    [DataRow(KTimeSpan)]
    [DataRow(KGuid)]
    public void InvalidUtf8InAString_Throws(KustoColumnType type)
    {
        byte[] row = Encoding.UTF8.GetBytes("[\"a_b\"]");
        row[3] = 0xFF;
        ((Action)(() => KustoRowValues.ToValues(row, [new("c0", type)]))).Should().ThrowExactly<JsonException>().WithMessage("*c0*");
    }

    [TestMethod]
    [DataRow(KString)]
    [DataRow(KReal)]
    [DataRow(KDecimal)]
    [DataRow(KDateTime)]
    [DataRow(KTimeSpan)]
    [DataRow(KGuid)]
    public void LoneSurrogateEscapeInAString_Throws(KustoColumnType type) => AssertRejected("[\"a\\uD800b\"]", type);

    private static T? ReadStruct<T>(byte[] value, KustoColumnType type) where T : struct
    {
        Utf8JsonReader reader = new(value);
        reader.Read();
        return KustoRowValues.ReadValue<T>(ref reader, new("c0", type), null);
    }

    private static T? ReadClass<T>(byte[] value, KustoColumnType type) where T : class
    {
        Utf8JsonReader reader = new(value);
        reader.Read();
        return KustoRowValues.ReadValue<T>(ref reader, new("c0", type), null);
    }

    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void TypedRead_InvalidTextInAString_Throws(bool invalidUtf8)
    {
        byte[] value = Encoding.UTF8.GetBytes(invalidUtf8 ? "\"a_b\"" : "\"a\\uD800b\"");
        if (invalidUtf8)
        {
            value[2] = 0xFF;
        }

        ((Action)(() => ReadClass<string>(value, KString))).Should().ThrowExactly<JsonException>().WithMessage("*c0*");
        ((Action)(() => ReadClass<object>(value, KString))).Should().ThrowExactly<JsonException>().WithMessage("*c0*");
        ((Action)(() => ReadStruct<double>(value, KReal))).Should().ThrowExactly<JsonException>().WithMessage("*c0*");
        ((Action)(() => ReadStruct<decimal>(value, KDecimal))).Should().ThrowExactly<JsonException>().WithMessage("*c0*");
        ((Action)(() => ReadStruct<DateTime>(value, KDateTime))).Should().ThrowExactly<JsonException>().WithMessage("*c0*");
        ((Action)(() => ReadStruct<TimeSpan>(value, KTimeSpan))).Should().ThrowExactly<JsonException>().WithMessage("*c0*");
        ((Action)(() => ReadStruct<Guid>(value, KGuid))).Should().ThrowExactly<JsonException>().WithMessage("*c0*");
    }

    [TestMethod]
    public void TypedRead_WrongType_NamesTheColumnAndTheTypes()
    {
        ((Action)(() => ReadStruct<int>("5"u8.ToArray(), KLong))).Should().ThrowExactly<InvalidCastException>().WithMessage("Column 'c0' (Long) cannot be read as System.Int32.");
        ((Action)(() => ReadStruct<long>("\"x\""u8.ToArray(), KString))).Should().ThrowExactly<InvalidCastException>().WithMessage("Column 'c0' (String) cannot be read as System.Int64.");
        ((Action)(() => ReadClass<string>("5"u8.ToArray(), KLong))).Should().ThrowExactly<InvalidCastException>().WithMessage("Column 'c0' (Long) cannot be read as System.String.");
        ReadClass<object>("5"u8.ToArray(), KLong).Should().Be(5L);
        ReadStruct<long>("5"u8.ToArray(), KLong).Should().Be(5L);
        ReadStruct<int>("null"u8.ToArray(), KLong).Should().BeNull();
        ReadClass<string>("null"u8.ToArray(), KLong).Should().BeNull();
    }

    [TestMethod]
    public void TypedRead_OfAValueType_DoesNotBox()
    {
        if (typeof(KustoRowValues).Assembly.GetCustomAttribute<DebuggableAttribute>()?.IsJITOptimizerDisabled == true)
        {
            Assert.Inconclusive("The JIT only removes the box from an optimized (Release) build of the library.");
        }

        AllocatedByTypedReads<bool>("true", KBool).Should().Be(0, "bool");
        AllocatedByTypedReads<int>("1", KInt).Should().Be(0, "int");
        AllocatedByTypedReads<long>("2", KLong).Should().Be(0, "long");
        AllocatedByTypedReads<double>("2.5", KReal).Should().Be(0, "real");
        AllocatedByTypedReads<double>("\"NaN\"", KReal).Should().Be(0, "non-finite real");
        AllocatedByTypedReads<decimal>("1.5", KDecimal).Should().Be(0, "decimal");
        AllocatedByTypedReads<decimal>("\"2.5\"", KDecimal).Should().Be(0, "decimal as a string");
        AllocatedByTypedReads<DateTime>("\"2024-01-02T03:04:05Z\"", KDateTime).Should().Be(0, "datetime");
        AllocatedByTypedReads<TimeSpan>("\"01:02:03\"", KTimeSpan).Should().Be(0, "timespan");
        AllocatedByTypedReads<Guid>("\"00000000-0000-0000-0000-000000000001\"", KGuid).Should().Be(0, "guid");
    }

    // Relies on tiered compilation being off (see the project file), so that the first call already runs optimized code.
    private static long AllocatedByTypedReads<T>(string value, KustoColumnType type) where T : struct
    {
        byte[] json = Encoding.UTF8.GetBytes(value);
        KustoColumn column = new("c0", type);

        // The first read runs static constructors, which allocate.
        Read().Should().NotBeNull();
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 100; i++)
        {
            Read();
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;

        T? Read()
        {
            Utf8JsonReader reader = new(json);
            reader.Read();
            return KustoRowValues.ReadValue<T>(ref reader, column, null);
        }
    }

    [TestMethod]
    public void TypedRead_WrongTokenForTheColumn_Throws()
    {
        ((Action)(() => ReadStruct<long>("\"x\""u8.ToArray(), KLong))).Should().ThrowExactly<JsonException>().WithMessage("*c0*");
        ((Action)(() => ReadClass<string>("1"u8.ToArray(), KString))).Should().ThrowExactly<JsonException>().WithMessage("*c0*");
    }

    [TestMethod]
    public void NonLeadingColumnFailure_NamesTheColumn()
    {
        var act = () => KustoRowValues.ToValues("[1,\"x\"]"u8, [new("a", KInt), new("theName", KLong)]);
        act.Should().ThrowExactly<JsonException>().WithMessage("*theName*");
    }

    [TestMethod]
    [DataRow("[]")]
    [DataRow("[1]")]
    public void TooFewValues_Throws(string json) => ((Action)(() => Parse(json, KInt, KInt))).Should().ThrowExactly<JsonException>();

    [TestMethod]
    [DataRow("[1,2,3]")]
    [DataRow("[1,2,[3]]")]
    [DataRow("[1,2,null]")]
    public void TooManyValues_Throws(string json) => ((Action)(() => Parse(json, KInt, KInt))).Should().ThrowExactly<JsonException>();

    [TestMethod]
    public void ZeroColumns()
    {
        Parse("[]").Should().BeEmpty();
        ((Action)(() => Parse("[1]"))).Should().ThrowExactly<JsonException>();
    }

    [TestMethod]
    public void Dynamic_DoesNotReferenceInputBuffer()
    {
        var buffer = Encoding.UTF8.GetBytes("""[{"a":[1,"two"],"s":"hello"},"str",[true]]""");
        var values = KustoRowValues.ToValues(buffer, [new("a", KDynamic), new("b", KDynamic), new("c", KDynamic)]);
        Array.Clear(buffer);

        var obj = (JsonElement)values[0]!;
        obj.GetProperty("a")[1].GetString().Should().Be("two");
        obj.GetProperty("s").GetString().Should().Be("hello");
        ((JsonElement)values[1]!).GetString().Should().Be("str");
        ((JsonElement)values[2]!)[0].GetBoolean().Should().BeTrue();
    }
}

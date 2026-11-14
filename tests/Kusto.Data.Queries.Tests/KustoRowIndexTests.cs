using System.Text;
using System.Text.Json;
using AwesomeAssertions;

namespace Kusto.Data.Queries.Tests;

[TestClass]
public class KustoRowIndexTests
{
    private static readonly byte[] Row = Harness.Utf8("""[ 1 , "a,]\"[" ,{"k":[1,{"x":"]"}]},null,[[],[ ] ] , true ]""");

    private static readonly string[] Values = [.. JsonDocument.Parse(Row).RootElement.EnumerateArray().Select(value => value.GetRawText())];

    private static string ValueAt(byte[] row, int offset, string expected) =>
        Encoding.UTF8.GetString(row.AsSpan(offset, Math.Min(Encoding.UTF8.GetByteCount(expected), row.Length - offset)));

    [TestMethod]
    public void EveryPairOfOrdinals_FindsTheValuesInAnyOrder()
    {
        Values.Should().HaveCount(6);
        for (int first = 0; first < Values.Length; first++)
        {
            for (int second = 0; second < Values.Length; second++)
            {
                KustoRowIndex index = default;
                index.Reset(Values.Length);

                foreach (int ordinal in (int[])[first, second, .. Enumerable.Range(0, Values.Length)])
                {
                    ValueAt(Row, index.GetOffset(Row, ordinal), Values[ordinal]).Should().Be(Values[ordinal], $"first {first}, second {second}, ordinal {ordinal}");
                }
            }
        }
    }

    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    public void RowThatIsTooShort_ThrowsAndKeepsWhatItFound(int found)
    {
        byte[] row = Harness.Utf8("[10,20]");
        KustoRowIndex index = default;
        index.Reset(4);
        for (int ordinal = 0; ordinal < found; ordinal++)
        {
            index.GetOffset(row, ordinal).Should().Be(1 + (3 * ordinal));
        }

        for (int attempt = 0; attempt < 2; attempt++)
        {
            ((Action)(() => index.GetOffset(row, 3))).Should().ThrowExactly<JsonException>().WithMessage("Row has 2 values but the table has 4 columns.");
            ((Action)(() => index.GetOffset(row, 2))).Should().ThrowExactly<JsonException>().WithMessage("Row has 2 values but the table has 4 columns.");
            index.GetOffset(row, 1).Should().Be(4);
            index.GetOffset(row, 0).Should().Be(1);
        }
    }

    [TestMethod]
    public void EmptyRow_Throws()
    {
        KustoRowIndex index = default;
        index.Reset(1);
        ((Action)(() => index.GetOffset("[ ]"u8.ToArray(), 0))).Should().ThrowExactly<JsonException>().WithMessage("Row has 0 values but the table has 1 columns.");
    }

    [TestMethod]
    public void Reset_StartsAnotherRow()
    {
        KustoRowIndex index = default;
        index.Reset(2);
        index.GetOffset("[1,2]"u8, 1).Should().Be(3);

        index.Reset(2);
        index.GetOffset("[100,2]"u8, 1).Should().Be(5);
        index.GetOffset("[100,2]"u8, 0).Should().Be(1);

        index.Reset(3);
        index.GetOffset("[ 7,8,9]"u8, 2).Should().Be(6);
        ((Action)(() => index.GetOffset("[ 7,8,9]"u8.ToArray(), 3))).Should().ThrowExactly<JsonException>().WithMessage("Row has 3 values but the table has 3 columns.");
    }
}

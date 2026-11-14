using System.Text;
using System.Text.Json;
using AwesomeAssertions;

namespace Kusto.Data.Queries.Tests;

[TestClass]
public class KustoTableReaderErrorTests
{
    private const string FullError = """
        {"code":"General_BadRequest","message":"Request is invalid.","@type":"Kusto.BadRequest","@message":"Syntax error: SYN0002",
         "@failureCode":400,"@permanent":true,"@database":"Samples",
         "@context":{"timestamp":"2026-11-13T11:50:18Z","clientRequestId":"crid","activityId":"aid","serviceAlias":"HELP","nested":{"a":[1]}},
         "innererror":{"code":"SYN0002","message":"Expected: ]","@line":"5","innererror":{"code":"Deepest"}}}
        """;

    /// <summary>Reads everything, through both the synchronous and the asynchronous path, and returns the failure.</summary>
    private static async Task<Exception> ReadFailureAsync(string json)
    {
        byte[] payload = Harness.Utf8(json);
        Exception? first = null;
        foreach ((TestStream stream, int bufferSize) in new (TestStream, int)[]
        {
            (ChunkedStream.Whole(payload), 64 * 1024),
            (ChunkedStream.OneByte(payload, Completion.Mixed), 1),
        })
        {
            TrackingArrayPool pool = new();
            Exception? e = null;
            try
            {
                await foreach (KustoTable table in new KustoTableReader(stream, pool, bufferSize).ReadTablesAsync())
                {
                    await foreach (KustoRow _ in table.GetRowsAsync())
                    {
                    }
                }
            }
            catch (Exception caught)
            {
                e = caught;
            }

            e.Should().NotBeNull($"reading {json} must fail");
            pool.AssertAllReturned();
            if (first is not null)
            {
                e!.GetType().Should().Be(first.GetType(), "the synchronous and asynchronous paths must fail the same way");
            }

            first ??= e;
        }

        return first!;
    }

    private static async Task<KustoException> ReadKustoExceptionAsync(string json) =>
        (await ReadFailureAsync(json)).Should().BeOfType<KustoException>().Subject;

    [TestMethod]
    public async Task WholeResponseError_IsMappedWithEveryDetail()
    {
        KustoException e = await ReadKustoExceptionAsync("""{"error":""" + FullError + "}");

        KustoError error = e.Errors.Should().ContainSingle().Subject;
        e.Message.Should().Be("Syntax error: SYN0002");
        error.Code.Should().Be("General_BadRequest");
        error.Message.Should().Be("Request is invalid.");
        error.Type.Should().Be("Kusto.BadRequest");
        error.Description.Should().Be("Syntax error: SYN0002");
        error.IsPermanent.Should().BeTrue();
        error.Context!.ClientRequestId.Should().Be("crid");
        error.Context.ActivityId.Should().Be("aid");
        error.Context.AdditionalProperties!.Keys.Should().BeEquivalentTo("timestamp", "serviceAlias", "nested");
        error.Context.AdditionalProperties["nested"].GetProperty("a")[0].GetInt32().Should().Be(1);
        error.AdditionalProperties!.Keys.Should().BeEquivalentTo("@failureCode", "@database");
        error.AdditionalProperties["@failureCode"].GetInt32().Should().Be(400);
        error.AdditionalProperties["@database"].GetString().Should().Be("Samples");
        error.InnerError!.Code.Should().Be("SYN0002");
        error.InnerError.Message.Should().Be("Expected: ]");
        error.InnerError.IsPermanent.Should().BeFalse();
        error.InnerError.AdditionalProperties!["@line"].GetString().Should().Be("5");
        error.InnerError.InnerError!.Code.Should().Be("Deepest");
    }

    [TestMethod]
    public async Task WholeResponseError_FromTheSample()
    {
        byte[] payload = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "testdata", "Error.BadRequest.json"));
        KustoException e = await ReadKustoExceptionAsync(Encoding.UTF8.GetString(payload));

        KustoError error = e.Errors.Should().ContainSingle().Subject;
        error.Code.Should().Be("General_BadRequest");
        error.Description.Should().StartWith("Request is invalid and cannot be processed: Syntax error");
        e.Message.Should().Be(error.Description);
        error.IsPermanent.Should().BeTrue();
        error.Context!.ClientRequestId.Should().NotBeNullOrEmpty();
        error.Context.ActivityId.Should().NotBeNullOrEmpty();
        error.InnerError!.Code.Should().Be("SYN0002");
    }

    [TestMethod]
    [DataRow("""{"code":"c","message":"m","@message":"d"}""", "d")]
    [DataRow("""{"code":"c","message":"m"}""", "m")]
    [DataRow("""{"code":"c"}""", "c")]
    public async Task ExceptionMessage_IsDescriptionThenMessageThenCode(string error, string expected)
    {
        (await ReadKustoExceptionAsync("""{"error":""" + error + "}")).Message.Should().Be(expected);
        (await ReadKustoExceptionAsync(Payloads.Response(Payloads.Table(1, """[{"OneApiErrors":[{"error":""" + error + "}]}]")))).Message.Should().Be(expected);
    }

    [TestMethod]
    public async Task RowError_ListsEveryError()
    {
        KustoException e = await ReadKustoExceptionAsync(Payloads.Response(Payloads.Table(1,
            """[["a"], {"OneApiErrors":[{"error":""" + FullError + """},{"error":{"code":"Second","@permanent":false}}]}, ["never"]]""")));

        e.Errors.Select(error => error.Code).Should().Equal("General_BadRequest", "Second");
        e.Errors[0].InnerError!.InnerError!.Code.Should().Be("Deepest");
        e.Message.Should().Be("Syntax error: SYN0002");
    }

    [TestMethod]
    public async Task RowError_FollowsTheRowsBeforeIt()
    {
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, """[["a"],["b"],{"OneApiErrors":[{"error":{"code":"E"}}]}]""")));
        List<ExpectedEvent> expected = ResponseOracle.Expect(payload);
        expected.Texts().Should().Equal("table 1 'PrimaryResult' PrimaryResult (s:String)", "row [\"a\"]", "row [\"b\"]", Events.Throws<KustoException>());
        await Harness.VerifyAsync(ChunkedStream.OneByte(payload), 2, expected);
    }

    [TestMethod]
    [DataRow("""{"FrameType":"DataSetCompletion","HasErrors":true,"Cancelled":false}""", false)]
    [DataRow("""{"FrameType":"DataSetCompletion","HasErrors":true}""", false)]
    [DataRow("""{"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":true}""", true)]
    [DataRow("""{"FrameType":"DataSetCompletion","Cancelled":true,"OneApiErrors":null}""", true)]
    public async Task CompletionWithoutErrorDetails_ThrowsWithAFallbackMessage(string completion, bool cancelled)
    {
        KustoException e = await ReadKustoExceptionAsync($"[{Payloads.Header},{Payloads.Table(1, "[[\"a\"]]")},{completion}]");

        e.Errors.Should().BeEmpty();
        e.Message.Should().NotBeNullOrWhiteSpace();
        e.Message.Contains("cancel", StringComparison.OrdinalIgnoreCase).Should().Be(cancelled);
    }

    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public async Task CompletionWithErrors_ThrowsThem(bool hasErrors)
    {
        string completion = """{"FrameType":"DataSetCompletion","HasErrors":""" + (hasErrors ? "true" : "false") + ""","Cancelled":false,"OneApiErrors":[{"error":{"code":"X","@message":"boom"}}]}""";
        KustoException e = await ReadKustoExceptionAsync($"[{Payloads.Header},{completion}]");

        e.Errors.Should().ContainSingle().Which.Code.Should().Be("X");
        e.Message.Should().Be("boom");
    }

    [TestMethod]
    public async Task CompletionWithAnEmptyErrorList_Succeeds()
    {
        byte[] payload = Harness.Utf8($"""[{Payloads.Header},{Payloads.Table(1, "[[\"a\"]]")},{"{"}"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":false,"OneApiErrors":[]{"}"}]""");
        await Harness.VerifyAsync(ChunkedStream.OneByte(payload), 1, ResponseOracle.Expect(payload));
    }

    [TestMethod]
    public async Task ErrorDetails_OutliveTheBuffer()
    {
        TrackingArrayPool pool = new();
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, """[{"OneApiErrors":[{"error":""" + FullError + "}]}]")));
        KustoException? e = null;
        try
        {
            await foreach (KustoTable table in new KustoTableReader(ChunkedStream.OneByte(payload), pool, 1).ReadTablesAsync())
            {
                await foreach (KustoRow _ in table.GetRowsAsync())
                {
                }
            }
        }
        catch (KustoException caught)
        {
            e = caught;
        }

        pool.AssertAllReturned();
        e!.Errors[0].AdditionalProperties!["@database"].GetString().Should().Be("Samples");
        e.Errors[0].Context!.AdditionalProperties!["nested"].GetRawText().Should().Be("""{"a":[1]}""");
    }

    [TestMethod]
    [DataRow("5", DisplayName = "top-level number")]
    [DataRow("\"x\"", DisplayName = "top-level string")]
    [DataRow("true", DisplayName = "top-level true")]
    [DataRow("null", DisplayName = "top-level null")]
    [DataRow("{}", DisplayName = "top-level object without errors")]
    [DataRow("""{"error":null}""", DisplayName = "top-level null error")]
    [DataRow("""{"OneApiErrors":[]}""", DisplayName = "top-level empty OneApiErrors")]
    [DataRow("""{"error":5}""", DisplayName = "top-level error of the wrong type")]
    [DataRow("[5]", DisplayName = "number frame")]
    [DataRow("[[]]", DisplayName = "array frame")]
    [DataRow("[\"x\"]", DisplayName = "string frame")]
    [DataRow("[null]", DisplayName = "null frame")]
    [DataRow("[{}]", DisplayName = "frame without FrameType")]
    [DataRow("""[{"TableId":1,"Columns":[]}]""", DisplayName = "frame without FrameType but with properties")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[]}]""", DisplayName = "DataTable without Rows")]
    [DataRow("""[{"Columns":[],"Rows":[],"FrameType":"DataTable"}]""", DisplayName = "Rows before FrameType")]
    [DataRow("""[{"FrameType":"DataTable","Rows":[],"Columns":[]}]""", DisplayName = "Rows before Columns")]
    [DataRow("""[{"FrameType":"DataTable","Columns":null,"Rows":[]}]""", DisplayName = "null Columns")]
    [DataRow("""[{"FrameType":"DataSetHeader","Rows":[]}]""", DisplayName = "Rows in the header")]
    [DataRow("""[{"FrameType":"DataSetCompletion","Rows":[]}]""", DisplayName = "Rows in the completion")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":{}}]""", DisplayName = "object Rows")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":null}]""", DisplayName = "null Rows")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":5}]""", DisplayName = "number Rows")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":"x"}]""", DisplayName = "string Rows")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":[1]}]""", DisplayName = "number row")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":[[],null]}]""", DisplayName = "null row")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":["x"]}]""", DisplayName = "string row")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":[false]}]""", DisplayName = "boolean row")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":[{}]}]""", DisplayName = "object row without errors")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":[{"OneApiErrors":[]}]}]""", DisplayName = "row with empty OneApiErrors")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":[{"OneApiErrors":null}]}]""", DisplayName = "row with null OneApiErrors")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":[{"OneApiErrors":5}]}]""", DisplayName = "row with OneApiErrors of the wrong type")]
    [DataRow("""[{"FrameType":"DataTable","TableId":"x","Columns":[],"Rows":[]}]""", DisplayName = "string TableId")]
    [DataRow("""[{"FrameType":"DataTable","TableId":null,"Columns":[],"Rows":[]}]""", DisplayName = "null TableId")]
    [DataRow("""[{"FrameType":"DataTable","TableId":1.5,"Columns":[],"Rows":[]}]""", DisplayName = "fractional TableId")]
    [DataRow("""[{"FrameType":"DataTable","TableId":4294967296,"Columns":[],"Rows":[]}]""", DisplayName = "TableId out of range")]
    [DataRow("""[{"FrameType":"DataTable","TableKind":5,"Columns":[],"Rows":[]}]""", DisplayName = "number TableKind")]
    [DataRow("""[{"FrameType":"DataTable","TableName":[],"Columns":[],"Rows":[]}]""", DisplayName = "array TableName")]
    [DataRow("""[{"FrameType":5}]""", DisplayName = "number FrameType")]
    [DataRow("""[{"FrameType":{}}]""", DisplayName = "object FrameType")]
    [DataRow("""[{"FrameType":"DataSetHeader","IsProgressive":"no"}]""", DisplayName = "string IsProgressive")]
    [DataRow("""[{"FrameType":"DataSetHeader","IsProgressive":null}]""", DisplayName = "null IsProgressive")]
    [DataRow("""[{"FrameType":"DataSetCompletion","HasErrors":"x"}]""", DisplayName = "string HasErrors")]
    [DataRow("""[{"FrameType":"DataSetCompletion","Cancelled":1}]""", DisplayName = "number Cancelled")]
    [DataRow("""[{"FrameType":"DataSetCompletion","OneApiErrors":{}}]""", DisplayName = "object OneApiErrors")]
    [DataRow("""[{"FrameType":"DataTable","Columns":{},"Rows":[]}]""", DisplayName = "object Columns")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[5],"Rows":[]}]""", DisplayName = "number column")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[null],"Rows":[]}]""", DisplayName = "null column")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[{"ColumnName":"a","ColumnType":"string"},null],"Rows":[]}]""", DisplayName = "null second column")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[{"ColumnName":"a","ColumnType":"foo"}],"Rows":[]}]""", DisplayName = "unknown ColumnType")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[{"ColumnName":"a","ColumnType":""}],"Rows":[]}]""", DisplayName = "empty ColumnType")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[{"ColumnName":"a","ColumnType":null}],"Rows":[]}]""", DisplayName = "null ColumnType")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[{"ColumnName":"a"}],"Rows":[]}]""", DisplayName = "missing ColumnType")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[{"ColumnType":"int"}],"Rows":[]}]""", DisplayName = "missing ColumnName")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[{"ColumnName":null,"ColumnType":"int"}],"Rows":[]}]""", DisplayName = "null ColumnName")]
    [DataRow("""[{"FrameType":"DataSetHeader"},]""", DisplayName = "trailing comma")]
    [DataRow("""[{"FrameType":"DataSetHeader",}]""", DisplayName = "trailing comma in a frame")]
    [DataRow("""[/* c */{"FrameType":"DataSetHeader"}]""", DisplayName = "comment")]
    [DataRow("""[{'FrameType':"DataSetHeader"}]""", DisplayName = "single quotes")]
    [DataRow("""[{"FrameType":"DataSetHeader"}}""", DisplayName = "mismatched brackets")]
    [DataRow("[{\"FrameType\":\"DataTable\",\"Columns\":[],\"Rows\":[[\"a\tb\"]]}]", DisplayName = "control character in a string")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":[[01]]}]""", DisplayName = "leading zero")]
    [DataRow("""[{"FrameType":"DataTable","Columns":[],"Rows":[[nul]]}]""", DisplayName = "bad literal")]
    [DataRow("""[{"FrameType":"DataSetHeader"} {"FrameType":"DataSetHeader"}]""", DisplayName = "missing comma")]
    public async Task MalformedOrUnexpectedInput_ThrowsJsonException(string json) => (await ReadFailureAsync(json)).ShouldBeJsonException();

    [TestMethod]
    [DataRow("DataTable", DisplayName = "invalid UTF-8 in the frame type")]
    public async Task InvalidUtf8_ThrowsJsonException(string value)
    {
        byte[] payload = Harness.Utf8(Payloads.Response(Payloads.Table(1, """[["ab"]]""")));
        int offset = payload.AsSpan().IndexOf(Encoding.UTF8.GetBytes($"\"{value}\"")) + 1;
        payload[offset] = 0xFF;
        TrackingArrayPool pool = new();
        List<string> events = await Harness.ReadEventsAsync(new KustoTableReader(ChunkedStream.OneByte(payload), pool, 1));
        events[^1].Should().Be(Events.Throws<JsonException>());
        pool.AssertAllReturned();
    }

    [TestMethod]
    [DataRow("""{"FrameType":"DataSetHeader","IsProgressive":true}""")]
    [DataRow("""{"IsProgressive":true,"FrameType":"DataSetHeader"}""")]
    [DataRow("""{"FrameType":"TableHeader","TableId":1}""")]
    [DataRow("""{"FrameType":"TableFragment","TableFragmentType":"DataAppend","Rows":[]}""")]
    [DataRow("""{"FrameType":"TableProgress"}""")]
    [DataRow("""{"FrameType":"TableCompletion"}""")]
    [DataRow("""{"FrameType":"Bogus"}""")]
    [DataRow("""{"FrameType":""}""")]
    [DataRow("""{"FrameType":"datatable"}""")]
    public async Task ProgressiveOrUnknownFrames_ThrowNotSupportedException(string frame) =>
        (await ReadFailureAsync($"[{frame}]")).Should().BeOfType<NotSupportedException>();

    [TestMethod]
    public async Task Whitespace_IsAllowedWhereverJsonAllowsIt()
    {
        byte[] payload = Spread(Payloads.Representative);
        List<ExpectedEvent> expected = ResponseOracle.Expect(payload);
        List<string> original = ResponseOracle.Expect(Payloads.Representative).Texts();
        expected.Should().HaveCount(original.Count);
        expected.Texts().Where(IsTable).Should().Equal(original.Where(IsTable));
        foreach ((int bufferSize, Completion completion) in new[] { (1, Completion.Sync), (3, Completion.Async), (64, Completion.Mixed) })
        {
            await Harness.VerifyAsync(ChunkedStream.OneByte(payload, completion), bufferSize, expected);
        }
    }

    [TestMethod]
    public async Task UnknownProperties_OfAnyShape_AreSkipped()
    {
        const string unknown = """
            "Null":null,"Obj":{"Rows":[1],"FrameType":"TableHeader","x":{"y":[]}},"Arr":[[{}],"]"],"Str":"Rows","Num":-1.5e10,"True":true,"Rows2":[5]
            """;
        byte[] payload = Harness.Utf8("""
            [{UNKNOWN,"FrameType":"DataSetHeader",UNKNOWN,"IsProgressive":false},
             {UNKNOWN,"FrameType":"DataTable","TableId":4,UNKNOWN,"TableName":"T","Columns":[{"ColumnName":"a","ColumnType":"int","Extra":[1]}],UNKNOWN,"Rows":[[1],[2]],UNKNOWN},
             {"FrameType":"DataSetCompletion",UNKNOWN,"HasErrors":false,UNKNOWN}]
            """.Replace("UNKNOWN", unknown, StringComparison.Ordinal));
        List<ExpectedEvent> expected = ResponseOracle.Expect(payload);
        expected.Should().HaveCount(3);
        await Harness.VerifyAsync(ChunkedStream.OneByte(payload, Completion.Mixed), 2, expected);
        await Harness.VerifyAsync(ChunkedStream.Whole(payload), 4096, expected);
    }

    [TestMethod]
    public async Task FrameProperties_InAnyOrderBeforeRows_AreTolerated()
    {
        byte[] payload = Harness.Utf8("""
            [{"IsProgressive":false,"FrameType":"DataSetHeader"},
             {"Columns":[{"ColumnName":"a","ColumnType":"int"}],"TableName":"T","TableKind":"QueryPlan","FrameType":"DataTable","TableId":9,"Rows":[[1]]},
             {"HasErrors":false,"Cancelled":false,"FrameType":"DataSetCompletion"}]
            """);
        await Harness.VerifyAsync(ChunkedStream.OneByte(payload), 1, ["table 9 'T' QueryPlan (a:Int)", "row [1]"]);
    }

    // The table is yielded when its rows start, so metadata that follows the rows cannot be reflected.
    [TestMethod]
    public async Task FrameMetadata_AfterRows_IsToleratedButIgnored()
    {
        byte[] payload = Harness.Utf8("""
            [{"FrameType":"DataTable","Columns":[{"ColumnName":"a","ColumnType":"int"}],"Rows":[[1]],"TableName":"T","TableKind":"QueryPlan","TableId":9,"Columns":[]}]
            """);
        await Harness.VerifyAsync(ChunkedStream.OneByte(payload), 1, ["table 0 '' Unknown (a:Int)", "row [1]"]);
    }

    [TestMethod]
    public async Task EscapedPropertyNamesAndValues_AreRecognised()
    {
        byte[] payload = Harness.Utf8("""
            [{"FrameType":"DataTable","TableName":"T","TableKind":"PrimaryResult","Columns":[{"ColumnName":"a","ColumnType":"int"}],"Rows":[[1]]}]
            """);
        await Harness.VerifyAsync(ChunkedStream.OneByte(payload), 1, ["table 0 'T' PrimaryResult (a:Int)", "row [1]"]);
    }

    [TestMethod]
    public async Task FramesAfterTheCompletion_AreRead()
    {
        byte[] payload = Harness.Utf8($"[{Payloads.Header},{Payloads.Completion},{Payloads.Table(5, "[[\"late\"]]")},{Payloads.Completion}]");
        await Harness.VerifyAsync(ChunkedStream.OneByte(payload), 1, ResponseOracle.Expect(payload));
        ResponseOracle.Expect(payload).Texts().Should().HaveCount(2);
    }

    [TestMethod]
    public async Task EmptyFrameArray_HasNoTables() =>
        await Harness.VerifyAsync(ChunkedStream.OneByte(Harness.Utf8(" [ ] ")), 1, Array.Empty<string>());

    [TestMethod]
    [DataRow(Completion.Sync)]
    [DataRow(Completion.Async)]
    public async Task DataAfterTheResponse_IsNotRead(Completion completion)
    {
        byte[] payload = Payloads.Representative;
        byte[] withGarbage = [.. payload, .. "garbage]}\""u8];
        List<ExpectedEvent> expected = ResponseOracle.Expect(payload);

        await Harness.VerifyAsync(ChunkedStream.FailAfter(withGarbage, payload.Length, _ => 1, completion), 1, expected, "one byte, failing past the end");
        await Harness.VerifyAsync(ChunkedStream.FailAfter(withGarbage, payload.Length, _ => 100, completion), 16, expected, "chunks, failing past the end");
        await Harness.VerifyAsync(ChunkedStream.Whole(withGarbage, completion), 4096, expected, "garbage in the last read");

        byte[] errorWithGarbage = Harness.Utf8("""{"error":{"code":"c"}} garbage""");
        ChunkedStream stream = ChunkedStream.FailAfter(errorWithGarbage, errorWithGarbage.Length - " garbage".Length, _ => 1, completion);
        (await Harness.ReadEventsAsync(new KustoTableReader(stream, new TrackingArrayPool(), 1))).Should().Equal(Events.Throws<KustoException>());
    }

    private static bool IsTable(string e) => e.StartsWith("table ", StringComparison.Ordinal);

    /// <summary>Surrounds every structural character outside strings with whitespace.</summary>
    private static byte[] Spread(byte[] json)
    {
        List<byte> result = [.. " \r\n\t"u8];
        bool inString = false;
        bool escaped = false;
        foreach (byte b in json)
        {
            if (inString)
            {
                result.Add(b);
                (inString, escaped) = escaped ? (true, false) : (b != '"', b == '\\');
            }
            else if (b is (byte)',' or (byte)':' or (byte)'[' or (byte)']' or (byte)'{' or (byte)'}')
            {
                result.AddRange(" \t\r\n"u8);
                result.Add(b);
                result.AddRange("\n  "u8);
            }
            else
            {
                inString = b == '"';
                result.Add(b);
            }
        }

        return [.. result];
    }
}

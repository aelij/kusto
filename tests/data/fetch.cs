#:package Azure.Core
#:property PublishAot=false

using Azure.Core;
using Azure.Identity;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

Directory.SetCurrentDirectory(GetScriptDirectory());

var token = await new AzureCliCredential().GetTokenAsync(
    new TokenRequestContext(["https://help.kusto.windows.net/.default"]));

var names = args.Length > 0
    ? args
    : Directory.GetFiles("queries", "*.kql")
        .Select(Path.GetFileNameWithoutExtension)
        .Order(StringComparer.Ordinal)
        .ToArray();

using var client = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);

foreach (var name in names)
{
    var query = await File.ReadAllTextAsync($"queries/{name}.kql");

    using var response = await client.PostAsJsonAsync("https://help.kusto.windows.net/v2/rest/query",
      new
      {
          db = "Samples",
          csl = query,
          properties = new
          {
              Options = new { results_progressive_enabled = false }
          }
      });
    using var output = File.Create($"{name}.json");
    await response.Content.CopyToAsync(output);
    Console.WriteLine($"{name}.json {(int)response.StatusCode} {output.Length} bytes");
}

return 0;

static string GetScriptDirectory([CallerFilePath] string path = "") =>
    Path.GetDirectoryName(path) ?? throw new InvalidOperationException("Could not locate the script directory.");

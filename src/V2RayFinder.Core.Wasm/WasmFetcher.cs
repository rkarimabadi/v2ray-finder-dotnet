using System.Text;
using V2RayFinder.Core.Wasm.Models;

namespace V2RayFinder.Core.Wasm;

/// <summary>
/// Browser-compatible fetcher using HttpClient (injected by Blazor DI).
/// No TcpClient — only HTTP fetch, which is what browsers support.
/// Uses semaphore for concurrency control.
/// </summary>
public sealed class WasmFetcher
{
    private readonly HttpClient _http;

    public WasmFetcher(HttpClient http)
    {
        _http = http;
    }

    public async Task<IReadOnlyList<FetchResult>> FetchAllAsync(
        IEnumerable<SourceEntry> sources,
        bool useCorsProxy,
        int maxConcurrency = 6,         // browsers limit to ~6 parallel requests/host
        int timeoutSeconds = 20,
        IProgress<string>? progress = null,
        CancellationToken ct = default)
    {
        var semaphore = new SemaphoreSlim(maxConcurrency);
        var results = new List<FetchResult>();
        var tasks = sources.Select(s => FetchOneAsync(s, useCorsProxy, semaphore, timeoutSeconds, progress, ct));
        var fetched = await Task.WhenAll(tasks);
        results.AddRange(fetched);
        return results;
    }

    private async Task<FetchResult> FetchOneAsync(
        SourceEntry source,
        bool useProxy,
        SemaphoreSlim semaphore,
        int timeoutSeconds,
        IProgress<string>? progress,
        CancellationToken ct)
    {
        await semaphore.WaitAsync(ct);
        try
        {
            progress?.Report(source.Name);
            var url = SubscriptionSources.BuildUrl(source.Url, useProxy);
            return await FetchWithRetryAsync(source.Name, url, timeoutSeconds, ct);
        }
        finally { semaphore.Release(); }
    }

    private async Task<FetchResult> FetchWithRetryAsync(
        string name, string url, int timeoutSeconds, CancellationToken ct)
    {
        StructuredError? lastError = null;
        var delay = TimeSpan.FromSeconds(1);

        for (int attempt = 0; attempt <= 2; attempt++)
        {
            if (attempt > 0)
                await Task.Delay(delay * attempt, ct);

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

            try
            {
                var resp = await _http.GetAsync(url, cts.Token);
                if (!resp.IsSuccessStatusCode)
                {
                    var code = (int)resp.StatusCode;
                    lastError = new StructuredError("network", $"http_{code}", $"HTTP {code}", code >= 500);
                    if (code < 500) break;
                    continue;
                }

                var bytes = await resp.Content.ReadAsByteArrayAsync(ct);
                var text = Decode(bytes);
                var lines = text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
                return new FetchResult(url, true, lines);
            }
            catch (TaskCanceledException) when (!ct.IsCancellationRequested)
            {
                lastError = new StructuredError("network", "timeout", $"Timeout after {timeoutSeconds}s", true);
            }
            catch (HttpRequestException ex)
            {
                lastError = new StructuredError("network", "request", ex.Message, true);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                lastError = new StructuredError("unknown", "exception", ex.Message, false);
                break;
            }
        }

        return new FetchResult(url, false, Array.Empty<string>(), lastError);
    }

    private static string Decode(byte[] bytes)
    {
        var text = Encoding.UTF8.GetString(bytes).Trim();

        // Detect if entire body is base64-encoded subscription
        if (IsLikelyBase64(text))
        {
            try
            {
                var padded = text.Replace("\n", "").Replace("\r", "");
                padded = padded.PadRight(padded.Length + (4 - padded.Length % 4) % 4, '=');
                return Encoding.UTF8.GetString(Convert.FromBase64String(padded));
            }
            catch { /* fall through */ }
        }
        return text;
    }

    private static bool IsLikelyBase64(string s)
    {
        if (s.Length < 20) return false;
        if (s.Contains("vmess://") || s.Contains("vless://") ||
            s.Contains("trojan://") || s.Contains("ss://"))
            return false;

        var clean = s.Replace("\n", "").Replace("\r", "");
        return System.Text.RegularExpressions.Regex.IsMatch(clean, @"^[A-Za-z0-9+/]+=*$");
    }
}

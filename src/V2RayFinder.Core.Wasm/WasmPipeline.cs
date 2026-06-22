using V2RayFinder.Core.Wasm.Models;

namespace V2RayFinder.Core.Wasm;

/// <summary>
/// Browser pipeline: fetch → parse → dedup → score.
/// Health checking (TCP) is not available in WebAssembly.
/// Progress is reported via IProgress so the UI can react.
/// </summary>
public sealed class WasmPipeline
{
    private readonly WasmFetcher _fetcher;

    public WasmPipeline(WasmFetcher fetcher) => _fetcher = fetcher;

    public async Task<PipelineResult> RunAsync(
        IReadOnlyList<SourceEntry>? sources = null,
        bool useCorsProxy = true,
        int limit = 0,
        int concurrency = 6,
        IProgress<PipelineProgress>? progress = null,
        CancellationToken ct = default)
    {
        sources ??= SubscriptionSources.Entries;
        var total = sources.Count;

        // ── Stage 1: Fetch ───────────────────────────────────────────────────
        Report(progress, "fetch", 0, total, "در حال دریافت منابع...");

        int fetched = 0;
        var fetchProgress = new Progress<string>(_ =>
        {
            Interlocked.Increment(ref fetched);
            Report(progress, "fetch", fetched, total, $"دریافت {fetched}/{total}...");
        });

        var fetchResults = await _fetcher.FetchAllAsync(
            sources, useCorsProxy, concurrency,
            progress: fetchProgress, ct: ct);

        ct.ThrowIfCancellationRequested();

        var successCount = fetchResults.Count(r => r.Success);
        var failedUrls = fetchResults.Where(r => !r.Success).Select(r => r.Url).ToList();
        var allLines = fetchResults.Where(r => r.Success).SelectMany(r => r.RawLines).ToList();

        Report(progress, "fetch", total, total, $"{successCount} منبع موفق از {total}");

        // ── Stage 2: Parse ───────────────────────────────────────────────────
        Report(progress, "parse", 0, allLines.Count, "پارس کردن کانفیگ‌ها...");

        // Build source map: fingerprint → source name
        var sourceMap = new Dictionary<string, string>();
        foreach (var fr in fetchResults.Where(r => r.Success))
        {
            var sourceName = sources.FirstOrDefault(s =>
                SubscriptionSources.BuildUrl(s.Url, useCorsProxy) == fr.Url ||
                s.Url == fr.Url)?.Name ?? fr.Url;

            foreach (var line in fr.RawLines)
            {
                foreach (var cfg in ConfigParser.ExtractFromText(line))
                {
                    if (!sourceMap.ContainsKey(cfg.Fingerprint))
                        sourceMap[cfg.Fingerprint] = sourceName;
                }
            }
        }

        var parsed = allLines
            .SelectMany(line => ConfigParser.ExtractFromText(line))
            .ToList();

        Report(progress, "parse", allLines.Count, allLines.Count, $"{parsed.Count} کانفیگ یافت شد");
        ct.ThrowIfCancellationRequested();

        // ── Stage 3: Dedup ───────────────────────────────────────────────────
        Report(progress, "dedup", 0, 1, "حذف موارد تکراری...");

        var unique = parsed
            .GroupBy(c => c.Fingerprint)
            .Select(g => g.First())
            .ToList();

        if (limit > 0 && unique.Count > limit)
            unique = unique.Take(limit).ToList();

        Report(progress, "dedup", 1, 1, $"{parsed.Count} → {unique.Count} کانفیگ منحصربه‌فرد");
        ct.ThrowIfCancellationRequested();

        // ── Stage 4: Score ───────────────────────────────────────────────────
        Report(progress, "score", 0, 1, "امتیازدهی کانفیگ‌ها...");

        var scores = ConfigScorer.Score(unique, sourceMap);

        Report(progress, "score", 1, 1, $"{scores.Count} کانفیگ امتیازدهی شد");

        var stats = new PipelineStats(
            Fetched: parsed.Count,
            Deduped: unique.Count,
            Scored: scores.Count,
            FailedSources: failedUrls.Count,
            TotalSources: total
        );

        return new PipelineResult(unique, scores, stats, failedUrls);
    }

    private static void Report(IProgress<PipelineProgress>? p, string stage, int cur, int tot, string msg)
        => p?.Report(new PipelineProgress(stage, cur, tot, msg));
}

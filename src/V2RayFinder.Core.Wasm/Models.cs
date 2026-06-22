namespace V2RayFinder.Core.Wasm.Models;

public enum V2RayProtocol { Vmess, Vless, Trojan, Shadowsocks, Unknown }

public record V2RayConfig(
    string Raw,
    V2RayProtocol Protocol,
    string? Host,
    int? Port,
    string Fingerprint
);

public record StructuredError(
    string Category,
    string Kind,
    string Message,
    bool Retryable
);

public record FetchResult(
    string Url,
    bool Success,
    IReadOnlyList<string> RawLines,
    StructuredError? StructuredError = null
);

public record ConfigScore(
    string Config,
    V2RayProtocol Protocol,
    double Total,
    string Grade,
    string? Source = null
)
{
    public static string CalculateGrade(double score) => score switch
    {
        >= 0.85 => "A",
        >= 0.70 => "B",
        >= 0.55 => "C",
        >= 0.40 => "D",
        _       => "F"
    };
}

public record PipelineStats(
    int Fetched,
    int Deduped,
    int Scored,
    int FailedSources,
    int TotalSources
);

public record PipelineResult(
    IReadOnlyList<V2RayConfig> Configs,
    IReadOnlyList<ConfigScore> Scores,
    PipelineStats Stats,
    IReadOnlyList<string> FailedUrls
);

// Progress events
public record PipelineProgress(
    string Stage,
    int Current,
    int Total,
    string Message
);

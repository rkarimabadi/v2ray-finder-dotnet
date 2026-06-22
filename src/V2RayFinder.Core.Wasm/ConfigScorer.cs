using V2RayFinder.Core.Wasm.Models;

namespace V2RayFinder.Core.Wasm;

public static class ConfigScorer
{
    private static readonly Dictionary<V2RayProtocol, double> ProtocolWeight = new()
    {
        [V2RayProtocol.Vless]       = 1.00,
        [V2RayProtocol.Trojan]      = 0.95,
        [V2RayProtocol.Vmess]       = 0.85,
        [V2RayProtocol.Shadowsocks] = 0.80,
        [V2RayProtocol.Unknown]     = 0.40,
    };

    public static IReadOnlyList<ConfigScore> Score(
        IEnumerable<V2RayConfig> configs,
        IReadOnlyDictionary<string, string>? sourceMap = null)
    {
        var list = configs.ToList();
        if (list.Count == 0) return [];

        var scored = list.Select(c => Compute(c, sourceMap)).ToList();
        scored.Sort((a, b) => b.Total.CompareTo(a.Total));
        return scored;
    }

    private static ConfigScore Compute(
        V2RayConfig c,
        IReadOnlyDictionary<string, string>? sourceMap)
    {
        double score = 0;

        // Protocol weight (35%)
        score += 0.35 * ProtocolWeight.GetValueOrDefault(c.Protocol, 0.4);

        // Has parseable host+port (25%)
        if (c.Host is not null && c.Port is not null)
            score += 0.25;

        // Config length heuristic — longer configs tend to have TLS params (15%)
        var rawLen = c.Raw.Length;
        score += 0.15 * Math.Min(1.0, rawLen / 200.0);

        // Has a readable fragment/remark (10%)
        if (c.Raw.Contains('#'))
            score += 0.10;

        // Has TLS indicators in URL (15%)
        var lower = c.Raw.ToLowerInvariant();
        if (lower.Contains("tls") || lower.Contains("security=tls") ||
            c.Protocol == V2RayProtocol.Trojan)
            score += 0.15;

        score = Math.Clamp(score, 0.0, 1.0);
        var source = sourceMap?.GetValueOrDefault(c.Fingerprint);
        return new ConfigScore(c.Raw, c.Protocol, score, ConfigScore.CalculateGrade(score), source);
    }
}

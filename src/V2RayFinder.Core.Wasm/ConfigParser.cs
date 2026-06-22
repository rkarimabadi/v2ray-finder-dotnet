using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using V2RayFinder.Core.Wasm.Models;

namespace V2RayFinder.Core.Wasm;

public static partial class ConfigParser
{
    [GeneratedRegex(@"(vmess|vless|trojan|ss|ssr)://[^\s""'<>\r\n]+", RegexOptions.IgnoreCase)]
    private static partial Regex ConfigRegex();

    public static IEnumerable<V2RayConfig> ExtractFromText(string text)
    {
        foreach (Match m in ConfigRegex().Matches(text))
        {
            var raw = m.Value.Trim().TrimEnd('=');  // clean trailing noise
            if (TryParse(raw, out var cfg) && cfg is not null)
                yield return cfg;
        }
    }

    public static bool TryParse(string raw, out V2RayConfig? config)
    {
        config = null;
        if (string.IsNullOrWhiteSpace(raw)) return false;

        try
        {
            var protocol = DetectProtocol(raw);
            if (protocol == V2RayProtocol.Unknown) return false;

            var (host, port) = ExtractHostPort(raw, protocol);
            var fingerprint = ComputeFingerprint(raw);
            config = new V2RayConfig(raw, protocol, host, port, fingerprint);
            return true;
        }
        catch { return false; }
    }

    private static V2RayProtocol DetectProtocol(string raw)
    {
        if (raw.StartsWith("vmess://",  StringComparison.OrdinalIgnoreCase)) return V2RayProtocol.Vmess;
        if (raw.StartsWith("vless://",  StringComparison.OrdinalIgnoreCase)) return V2RayProtocol.Vless;
        if (raw.StartsWith("trojan://", StringComparison.OrdinalIgnoreCase)) return V2RayProtocol.Trojan;
        if (raw.StartsWith("ss://",     StringComparison.OrdinalIgnoreCase)) return V2RayProtocol.Shadowsocks;
        if (raw.StartsWith("ssr://",    StringComparison.OrdinalIgnoreCase)) return V2RayProtocol.Shadowsocks;
        return V2RayProtocol.Unknown;
    }

    private static (string? host, int? port) ExtractHostPort(string raw, V2RayProtocol protocol)
    {
        try
        {
            if (protocol == V2RayProtocol.Vmess)
            {
                var b64 = raw["vmess://".Length..].Split('#')[0].Split('?')[0];
                var padded = b64.PadRight(b64.Length + (4 - b64.Length % 4) % 4, '=');
                var json = Encoding.UTF8.GetString(Convert.FromBase64String(padded));
                var hostMatch = Regex.Match(json, @"""add""\s*:\s*""([^""]+)""");
                var portMatch = Regex.Match(json, @"""port""\s*:\s*[""']?(\d+)[""']?");
                return (
                    hostMatch.Success ? hostMatch.Groups[1].Value : null,
                    portMatch.Success ? int.Parse(portMatch.Groups[1].Value) : null
                );
            }
            else
            {
                var uriStr = raw.Split('#')[0];
                var uri = new Uri(uriStr);
                return (uri.Host, uri.Port > 0 ? uri.Port : null);
            }
        }
        catch { return (null, null); }
    }

    public static string ComputeFingerprint(string raw)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(raw));
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }
}

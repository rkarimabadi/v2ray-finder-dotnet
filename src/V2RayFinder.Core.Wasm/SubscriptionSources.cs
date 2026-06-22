namespace V2RayFinder.Core.Wasm;

/// <summary>
/// Public subscription sources. Fetched via CORS-proxy when running in browser.
/// </summary>
public static class SubscriptionSources
{
    // We use a CORS proxy because raw GitHub URLs don't always have CORS headers
    // for browser fetch. We support two modes:
    //   1. Direct   - works if the server sends proper CORS headers
    //   2. Proxied  - routes through a public CORS proxy (fallback)
    private const string CorsProxy = "https://corsproxy.io/?";

    public static readonly IReadOnlyList<SourceEntry> Entries = new SourceEntry[]
    {
        new("barry-far / All",       "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/All_Configs_Sub.txt"),
        new("barry-far / vmess",     "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/vmess.txt"),
        new("barry-far / vless",     "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/vless.txt"),
        new("barry-far / trojan",    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/trojan.txt"),
        new("barry-far / ss",        "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/ss.txt"),
        new("Epodonios / All",       "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/All_Configs_Sub.txt"),
        new("Epodonios / vmess",     "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/Splitted-By-Protocol/vmess.txt"),
        new("Epodonios / vless",     "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/Splitted-By-Protocol/vless.txt"),
        new("ebrasha / all",         "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/main/subs/all.txt"),
        new("mahdibland / Eternity", "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/Eternity"),
        new("mahdibland / merge",    "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/sub/sub_merge.txt"),
        new("yebekhe / mix-b64",     "https://raw.githubusercontent.com/yebekhe/TelegramV2rayCollector/main/sub/base64/mix"),
        new("yebekhe / mix-plain",   "https://raw.githubusercontent.com/yebekhe/TelegramV2rayCollector/main/sub/normal/mix"),
        new("freefq / free",         "https://raw.githubusercontent.com/freefq/free/master/v2"),
        new("peasoft / NoMoreWalls", "https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.txt"),
        new("ermaozi",               "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/v2ray.txt"),
        new("mfuu / v2ray",          "https://raw.githubusercontent.com/mfuu/v2ray/master/v2ray"),
        new("soroushmirzaei",        "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/splitted/mixed"),
    };

    public static string BuildUrl(string directUrl, bool useProxy) =>
        useProxy ? CorsProxy + Uri.EscapeDataString(directUrl) : directUrl;
}

public record SourceEntry(string Name, string Url);

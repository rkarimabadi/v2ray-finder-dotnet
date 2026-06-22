namespace V2RayFinder.Core.Wasm;

/// <summary>
/// Public subscription sources. Fetched via CORS-proxy when running in browser.
/// </summary>
public static class SubscriptionSources
{
    private const string CorsProxy = "https://corsproxy.io/?";

public static readonly IReadOnlyList<SourceEntry> Entries = new SourceEntry[]
{
    // ── سورس‌های اصلی از پایتون ──────────────────────────────────────────────
    new("EbraSha public list", "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/refs/heads/main/V2Ray-Config-By-EbraSha.txt"),
    new("barry-far Sub1", "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Sub1.txt"),
    new("Epodonios all-configs", "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/All_Configs_Sub.txt"),
    new("V2RayAggregator — Eternity", "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/Eternity.txt"),
    new("V2RayAggregator — merged base64", "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/sub/sub_merge_base64.txt"),
    new("mfuu/v2ray clash", "https://raw.githubusercontent.com/mfuu/v2ray/master/clash.yaml"),
    new("NoMoreWalls list", "https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.txt"),
    new("LalatinaHub Mineral nodes", "https://raw.githubusercontent.com/LalatinaHub/Mineral/master/result/nodes"),
    new("freefq/free v2", "https://raw.githubusercontent.com/freefq/free/master/v2"),
    new("tbbatbb Proxy", "https://raw.githubusercontent.com/tbbatbb/Proxy/master/dist/v2ray.config.txt"),
    new("aiboboxx v2rayfree", "https://raw.githubusercontent.com/aiboboxx/v2rayfree/main/v2"),
    new("Pawdroid Free-servers", "https://raw.githubusercontent.com/Pawdroid/Free-servers/main/sub"),
    new("AzadNetCH V2Ray", "https://raw.githubusercontent.com/AzadNetCH/Clash/main/V2Ray.txt"),
    new("Leon406 SubCrawler — vless", "https://raw.githubusercontent.com/Leon406/SubCrawler/master/sub/share/vless"),
    new("Leon406 SubCrawler — ss", "https://raw.githubusercontent.com/Leon406/SubCrawler/master/sub/share/ss"),
    new("awesome-vpn all", "https://raw.githubusercontent.com/awesome-vpn/awesome-vpn/master/all"),
    new("YasserDivaR ShadowSocks2022", "https://raw.githubusercontent.com/YasserDivaR/pr0xy/main/ShadowSocks2022.txt"),
    new("soroushmirzaei telegram-collector — mixed", "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/splitted/mixed"),
    new("soroushmirzaei telegram-collector — vmess", "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/channels/protocols/vmess"),
    new("soroushmirzaei telegram-collector — vless", "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/channels/protocols/vless"),
    new("soroushmirzaei telegram-collector — trojan", "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/channels/protocols/trojan"),
    new("soroushmirzaei telegram-collector — shadowsocks", "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/channels/protocols/shadowsocks"),
    new("MhdiTaheri V2rayCollector — vless", "https://raw.githubusercontent.com/MhdiTaheri/V2rayCollector/main/vless.txt"),
    new("MhdiTaheri V2rayCollector — vmess", "https://raw.githubusercontent.com/MhdiTaheri/V2rayCollector/main/vmess.txt"),
    new("MhdiTaheri V2rayCollector — trojan", "https://raw.githubusercontent.com/MhdiTaheri/V2rayCollector/main/trojan.txt"),
    new("Surfboardv2ray worker-sub Eternity", "https://raw.githubusercontent.com/Surfboardv2ray/v2ray-worker-sub/master/Eternity.txt"),
    new("tbbatbb Proxy — trojan", "https://raw.githubusercontent.com/tbbatbb/Proxy/master/dist/trojan.config.txt"),
    new("resasanian Mirza sub", "https://raw.githubusercontent.com/resasanian/Mirza/main/sub"),
    new("IranianCypherpunks sub", "https://raw.githubusercontent.com/IranianCypherpunks/sub/main/config"),
    new("chromego_merge merged base64", "https://raw.githubusercontent.com/vveg26/chromego_merge/main/sub/merged_base64.txt"),
    new("ALIILAPRO v2rayNG-Config sub", "https://raw.githubusercontent.com/ALIILAPRO/v2rayNG-Config/main/sub.txt"),
    new("Bardiafa Free-V2ray-Config — all", "https://raw.githubusercontent.com/Bardiafa/Free-V2ray-Config/main/ALL_Configs_Sub.txt"),
    new("barry-far / All", "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/All_Configs_Sub.txt"),
    new("barry-far / vmess", "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/vmess.txt"),
    new("barry-far / vless", "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/vless.txt"),
    new("barry-far / trojan", "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/trojan.txt"),
    new("barry-far / ss", "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/Splitted-By-Protocol/ss.txt"),
    new("Epodonios / vmess", "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/Splitted-By-Protocol/vmess.txt"),
    new("Epodonios / vless", "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/Splitted-By-Protocol/vless.txt"),
    new("ebrasha / all", "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/main/subs/all.txt"),
    new("mahdibland / merge", "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/sub/sub_merge.txt"),
    new("yebekhe / mix-b64", "https://raw.githubusercontent.com/yebekhe/TelegramV2rayCollector/main/sub/base64/mix"),
    new("yebekhe / mix-plain", "https://raw.githubusercontent.com/yebekhe/TelegramV2rayCollector/main/sub/normal/mix"),
    new("ermaozi", "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/v2ray.txt"),
    new("mfuu / v2ray", "https://raw.githubusercontent.com/mfuu/v2ray/master/v2ray"),
};

    public static string BuildUrl(string directUrl, bool useProxy) =>
        useProxy ? CorsProxy + Uri.EscapeDataString(directUrl) : directUrl;
}

public record SourceEntry(string Name, string Url);

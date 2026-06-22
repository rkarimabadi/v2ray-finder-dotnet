using FluentAssertions;
using V2RayFinder.Core;
using V2RayFinder.Core.Models;
using Xunit;

namespace V2RayFinder.Tests;

public class ConfigParserTests
{
    [Theory]
    [InlineData("vmess://eyJhZGQiOiIxLjIuMy40IiwicG9ydCI6IjQ0MyIsInR5cGUiOiJub25lIiwiaWQiOiIxMjM0IiwiYWlkIjoiMCIsIm5ldCI6InRjcCIsInBhdGgiOiIiLCJob3N0IjoiIiwidGxzIjoiIn0=", V2RayProtocol.Vmess)]
    [InlineData("vless://uuid@1.2.3.4:443?type=tcp#label", V2RayProtocol.Vless)]
    [InlineData("trojan://password@example.com:443#label", V2RayProtocol.Trojan)]
    [InlineData("ss://Y2hhY2hhMjAtaWV0Zi1wb2x5MTMwNTpwYXNz@1.2.3.4:8388#label", V2RayProtocol.Shadowsocks)]
    public void TryParse_ShouldDetectProtocol(string raw, V2RayProtocol expected)
    {
        var ok = ConfigParser.TryParse(raw, out var config);
        ok.Should().BeTrue();
        config!.Protocol.Should().Be(expected);
    }

    [Fact]
    public void TryParse_InvalidInput_ShouldReturnFalse()
    {
        ConfigParser.TryParse("http://not-a-v2ray-config", out _).Should().BeFalse();
        ConfigParser.TryParse("", out _).Should().BeFalse();
        ConfigParser.TryParse("   ", out _).Should().BeFalse();
    }

    [Fact]
    public void ExtractFromText_ShouldFindMultipleConfigs()
    {
        var text = """
            some random text
            vmess://eyJhZGQiOiIxLjIuMy40IiwicG9ydCI6IjQ0MyIsInR5cGUiOiJub25lIiwiaWQiOiIxMjM0IiwiYWlkIjoiMCIsIm5ldCI6InRjcCIsInBhdGgiOiIiLCJob3N0IjoiIiwidGxzIjoiIn0=
            more text
            vless://uuid@2.3.4.5:443?type=tcp#test
            trojan://pass@3.4.5.6:443#test2
            """;

        var configs = ConfigParser.ExtractFromText(text).ToList();
        configs.Should().HaveCount(3);
        configs.Select(c => c.Protocol).Should().Contain(V2RayProtocol.Vmess);
        configs.Select(c => c.Protocol).Should().Contain(V2RayProtocol.Vless);
        configs.Select(c => c.Protocol).Should().Contain(V2RayProtocol.Trojan);
    }

    [Fact]
    public void Fingerprint_ShouldBeDeterministicAndUnique()
    {
        var raw1 = "vless://uuid@1.1.1.1:443#a";
        var raw2 = "vless://uuid@2.2.2.2:443#b";

        var fp1a = ConfigParser.ComputeFingerprint(raw1);
        var fp1b = ConfigParser.ComputeFingerprint(raw1);
        var fp2  = ConfigParser.ComputeFingerprint(raw2);

        fp1a.Should().Be(fp1b); // deterministic
        fp1a.Should().NotBe(fp2); // unique
    }
}

public class ConfigScorerTests
{
    private static V2RayConfig MakeConfig(string raw, V2RayProtocol proto = V2RayProtocol.Vless) =>
        new(raw, proto, "1.2.3.4", 443, ConfigParser.ComputeFingerprint(raw));

    [Fact]
    public void Score_WithoutHealth_ShouldReturnValidGrade()
    {
        var configs = new[]
        {
            MakeConfig("vless://a@1.1.1.1:443#1", V2RayProtocol.Vless),
            MakeConfig("vmess://abc@2.2.2.2:80#2",  V2RayProtocol.Vmess),
        };

        var scores = ConfigScorer.Score(configs);
        scores.Should().HaveCount(2);
        scores.All(s => s.Grade is "A" or "B" or "C" or "D" or "F").Should().BeTrue();
        scores.All(s => s.Total is >= 0 and <= 1).Should().BeTrue();
    }

    [Fact]
    public void Score_ShouldPreferVlessOverVmess()
    {
        var vless = MakeConfig("vless://a@1.1.1.1:443#1", V2RayProtocol.Vless);
        var vmess = MakeConfig("vmess://b@2.2.2.2:443#2", V2RayProtocol.Vmess);

        var scores = ConfigScorer.Score(new[] { vless, vmess }).ToDictionary(s => s.Protocol);
        scores[V2RayProtocol.Vless].Total.Should().BeGreaterThan(scores[V2RayProtocol.Vmess].Total);
    }

    [Fact]
    public void Score_WithHealthData_HealthyBetterThanUnhealthy()
    {
        var c1 = MakeConfig("vless://a@1.1.1.1:443#1");
        var c2 = MakeConfig("vless://b@2.2.2.2:443#2");

        var healthMap = new Dictionary<string, HealthResult>
        {
            [c1.Fingerprint] = new(c1.Raw, true,  120, HealthLayer.Tcp),
            [c2.Fingerprint] = new(c2.Raw, false, 0,   HealthLayer.None, "timeout"),
        };

        var scores = ConfigScorer.Score(new[] { c1, c2 }, healthMap);
        scores[0].Config.Should().Be(c1.Raw); // healthy comes first
    }

    [Fact]
    public void Score_EmptyInput_ReturnsEmpty()
    {
        ConfigScorer.Score(Array.Empty<V2RayConfig>()).Should().BeEmpty();
    }
}

public class ResultTypeTests
{
    [Fact]
    public void Result_Ok_ShouldUnwrap()
    {
        var r = Result<int, string>.Ok(42);
        r.IsOk.Should().BeTrue();
        r.Unwrap().Should().Be(42);
    }

    [Fact]
    public void Result_Err_ShouldReturnError()
    {
        var r = Result<int, string>.Err("boom");
        r.IsOk.Should().BeFalse();
        r.Error.Should().Be("boom");
    }

    [Fact]
    public void Result_UnwrapOr_ShouldReturnFallbackOnError()
    {
        var r = Result<int, string>.Err("nope");
        r.UnwrapOr(99).Should().Be(99);
    }
}

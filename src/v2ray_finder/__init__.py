"""v2ray-finder: Fetch and aggregate V2Ray server configurations from GitHub."""

from .core import V2RayServerFinder
from .exceptions import (
    AuthenticationError,
    ConfigParseError,
    ErrorType,
    GitHubAPIError,
    RateLimitError,
    V2RayFinderError,
)
from .normalizer import (
    NormalizedServer,
    deduplicate_across_sources,
    deduplicate_servers,
    normalize_server,
)
from .result import Err, Ok, Result
from .source_registry import SourceRegistry, SourceStats
from .sources import KnownSource, SourceConfig, SourceType

__version__ = "0.4.0"

__all__ = [
    "V2RayServerFinder",
    "V2RayFinderError",
    "GitHubAPIError",
    "RateLimitError",
    "AuthenticationError",
    "ConfigParseError",
    "ErrorType",
    "NormalizedServer",
    "normalize_server",
    "deduplicate_servers",
    "deduplicate_across_sources",
    "Ok",
    "Err",
    "Result",
    "SourceRegistry",
    "SourceStats",
    "SourceConfig",
    "SourceType",
    "KnownSource",
]

# xray real-connectivity layer (optional — gracefully absent if aiohttp-socks
# or the xray binary is not installed)
try:
    from .xray_connectivity import (
        RealConnectivityChecker,
        RealHealthResult,
        find_free_port,
    )
    from .xray_runner import XrayBinaryManager
    from .xray_config_adapter import ConfigAdapter
except ImportError:
    pass

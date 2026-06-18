"""Async HTTP fetching module with connection pooling and retry logic."""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

try:
    import aiohttp

    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False

from .exceptions import (
    ErrorType,
    GitHubAPIError,
    NetworkError,
    RateLimitError,
)
from .exceptions import TimeoutError as V2RayTimeoutError
from .exceptions import (
    V2RayFinderError,
)
from .result import Err, Ok, Result

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers: build structured error dicts
# ---------------------------------------------------------------------------


def _structured(exc: V2RayFinderError) -> dict:
    """Return the to_dict() payload of a V2RayFinderError."""
    return exc.to_dict()


def _timeout_error(url: str) -> Tuple[str, dict]:
    exc = V2RayTimeoutError(f"Request timed out: {url}", url=url)
    return exc.message, _structured(exc)


def _network_error(url: str, detail: str) -> Tuple[str, dict]:
    exc = NetworkError(detail, url=url)
    return exc.message, _structured(exc)


def _rate_limit_error(url: str, status: int) -> Tuple[str, dict]:
    exc = RateLimitError(
        message=f"Rate limit (HTTP {status}): {url}",
        remaining=0,
    )
    return exc.message, _structured(exc)


def _http_error(url: str, status: int) -> Tuple[str, dict]:
    exc = GitHubAPIError(f"HTTP {status}: {url}", status_code=status)
    return exc.message, _structured(exc)


def _unknown_error(url: str, detail: str) -> Tuple[str, dict]:
    from .exceptions import ErrorType, V2RayFinderError

    exc = V2RayFinderError(detail, error_type=ErrorType.UNKNOWN_ERROR)
    return exc.message, _structured(exc)


# ---------------------------------------------------------------------------
# FetchResult
# ---------------------------------------------------------------------------


@dataclass
class FetchResult:
    """Result of an async fetch operation.

    Attributes
    ----------
    url           : The URL that was fetched.
    content       : Response body text on success, else None.
    status_code   : HTTP status code if a response was received.
    success       : True only when HTTP 200 and content is available.
    error         : Human-readable error string (preserved for back-compat).
    elapsed_ms    : Wall-clock time from request start to completion.
    structured_error : V1-D2 — machine-readable error payload produced from
                    the V2RayFinderError hierarchy.  Always present when
                    ``success`` is False, None otherwise.  Shape::

                        {
                          "error_type": str,   # ErrorType.value
                          "message":    str,
                          "details":    dict,
                        }
    """

    url: str
    content: Optional[str]
    status_code: Optional[int]
    success: bool
    error: Optional[str]
    elapsed_ms: float
    structured_error: Optional[dict] = field(default=None)


# ---------------------------------------------------------------------------
# AsyncFetcher
# ---------------------------------------------------------------------------


class AsyncFetcher:
    """
    Async HTTP fetcher with connection pooling and retry logic.

    Automatically falls back to httpx if aiohttp is not available,
    and to sync requests if neither is available.
    """

    def __init__(
        self,
        max_concurrent: int = 50,
        timeout: float = 10.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.headers = headers or {}

        if AIOHTTP_AVAILABLE:
            self.backend = "aiohttp"
        elif HTTPX_AVAILABLE:
            self.backend = "httpx"
        else:
            self.backend = "sync"
            logger.warning(
                "Neither aiohttp nor httpx available. "
                "Install with: pip install 'v2ray-finder[async]'"
            )

    # ------------------------------------------------------------------
    # aiohttp backend
    # ------------------------------------------------------------------

    async def _fetch_with_aiohttp(
        self,
        session: "aiohttp.ClientSession",
        url: str,
    ) -> FetchResult:
        start_time = time.time()

        for attempt in range(self.max_retries):
            try:
                async with session.get(url) as response:
                    content = await response.text()
                    elapsed = (time.time() - start_time) * 1000

                    if response.status == 200:
                        return FetchResult(
                            url=url,
                            content=content,
                            status_code=response.status,
                            success=True,
                            error=None,
                            elapsed_ms=elapsed,
                        )
                    elif response.status in (403, 429):
                        msg, se = _rate_limit_error(url, response.status)
                        return FetchResult(
                            url=url,
                            content=None,
                            status_code=response.status,
                            success=False,
                            error=msg,
                            elapsed_ms=elapsed,
                            structured_error=se,
                        )
                    else:
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(self.retry_delay * (2**attempt))
                            continue
                        msg, se = _http_error(url, response.status)
                        return FetchResult(
                            url=url,
                            content=None,
                            status_code=response.status,
                            success=False,
                            error=msg,
                            elapsed_ms=elapsed,
                            structured_error=se,
                        )

            except asyncio.TimeoutError:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (2**attempt))
                    continue
                elapsed = (time.time() - start_time) * 1000
                msg, se = _timeout_error(url)
                return FetchResult(
                    url=url,
                    content=None,
                    status_code=None,
                    success=False,
                    error=msg,
                    elapsed_ms=elapsed,
                    structured_error=se,
                )

            except aiohttp.ClientError as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (2**attempt))
                    continue
                elapsed = (time.time() - start_time) * 1000
                msg, se = _network_error(url, str(e))
                return FetchResult(
                    url=url,
                    content=None,
                    status_code=None,
                    success=False,
                    error=msg,
                    elapsed_ms=elapsed,
                    structured_error=se,
                )

            except Exception as e:
                elapsed = (time.time() - start_time) * 1000
                logger.error("Unexpected error fetching %s: %s", url, e)
                msg, se = _unknown_error(url, str(e))
                return FetchResult(
                    url=url,
                    content=None,
                    status_code=None,
                    success=False,
                    error=msg,
                    elapsed_ms=elapsed,
                    structured_error=se,
                )

        elapsed = (time.time() - start_time) * 1000
        msg, se = _network_error(url, "Max retries exceeded")
        return FetchResult(
            url=url,
            content=None,
            status_code=None,
            success=False,
            error=msg,
            elapsed_ms=elapsed,
            structured_error=se,
        )

    # ------------------------------------------------------------------
    # httpx backend
    # ------------------------------------------------------------------

    async def _fetch_with_httpx(
        self,
        client: "httpx.AsyncClient",
        url: str,
    ) -> FetchResult:
        start_time = time.time()

        for attempt in range(self.max_retries):
            try:
                response = await client.get(url)
                elapsed = (time.time() - start_time) * 1000

                if response.status_code == 200:
                    return FetchResult(
                        url=url,
                        content=response.text,
                        status_code=response.status_code,
                        success=True,
                        error=None,
                        elapsed_ms=elapsed,
                    )
                elif response.status_code in (403, 429):
                    msg, se = _rate_limit_error(url, response.status_code)
                    return FetchResult(
                        url=url,
                        content=None,
                        status_code=response.status_code,
                        success=False,
                        error=msg,
                        elapsed_ms=elapsed,
                        structured_error=se,
                    )
                else:
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.retry_delay * (2**attempt))
                        continue
                    msg, se = _http_error(url, response.status_code)
                    return FetchResult(
                        url=url,
                        content=None,
                        status_code=response.status_code,
                        success=False,
                        error=msg,
                        elapsed_ms=elapsed,
                        structured_error=se,
                    )

            except httpx.TimeoutException:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (2**attempt))
                    continue
                elapsed = (time.time() - start_time) * 1000
                msg, se = _timeout_error(url)
                return FetchResult(
                    url=url,
                    content=None,
                    status_code=None,
                    success=False,
                    error=msg,
                    elapsed_ms=elapsed,
                    structured_error=se,
                )

            except httpx.HTTPError as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (2**attempt))
                    continue
                elapsed = (time.time() - start_time) * 1000
                msg, se = _network_error(url, str(e))
                return FetchResult(
                    url=url,
                    content=None,
                    status_code=None,
                    success=False,
                    error=msg,
                    elapsed_ms=elapsed,
                    structured_error=se,
                )

            except Exception as e:
                elapsed = (time.time() - start_time) * 1000
                logger.error("Unexpected error fetching %s: %s", url, e)
                msg, se = _unknown_error(url, str(e))
                return FetchResult(
                    url=url,
                    content=None,
                    status_code=None,
                    success=False,
                    error=msg,
                    elapsed_ms=elapsed,
                    structured_error=se,
                )

        elapsed = (time.time() - start_time) * 1000
        msg, se = _network_error(url, "Max retries exceeded")
        return FetchResult(
            url=url,
            content=None,
            status_code=None,
            success=False,
            error=msg,
            elapsed_ms=elapsed,
            structured_error=se,
        )

    # ------------------------------------------------------------------
    # fetch_many_async
    # ------------------------------------------------------------------

    async def fetch_many_async(self, urls: List[str]) -> List[FetchResult]:
        if not urls:
            return []

        if self.backend == "aiohttp":
            timeout_obj = aiohttp.ClientTimeout(total=self.timeout)
            connector = aiohttp.TCPConnector(limit=self.max_concurrent)
            async with aiohttp.ClientSession(
                headers=self.headers,
                timeout=timeout_obj,
                connector=connector,
            ) as session:
                tasks = [self._fetch_with_aiohttp(session, url) for url in urls]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                return self._handle_gather_results(urls, results)

        elif self.backend == "httpx":
            limits = httpx.Limits(max_connections=self.max_concurrent)
            async with httpx.AsyncClient(
                headers=self.headers,
                timeout=self.timeout,
                limits=limits,
            ) as client:
                tasks = [self._fetch_with_httpx(client, url) for url in urls]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                return self._handle_gather_results(urls, results)

        else:
            raise RuntimeError(
                "fetch_many_async() requires aiohttp or httpx. "
                "Install with: pip install 'v2ray-finder[async]'. "
                "Use fetch_many() for automatic sync fallback."
            )

    @staticmethod
    def _handle_gather_results(
        urls: List[str],
        results: list,
    ) -> List[FetchResult]:
        """Wrap any bare exceptions from asyncio.gather into FetchResult."""
        out = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                msg, se = _unknown_error(urls[i], str(result))
                out.append(
                    FetchResult(
                        url=urls[i],
                        content=None,
                        status_code=None,
                        success=False,
                        error=msg,
                        elapsed_ms=0,
                        structured_error=se,
                    )
                )
            else:
                out.append(result)
        return out

    # ------------------------------------------------------------------
    # fetch_many (sync wrapper)
    # ------------------------------------------------------------------

    def fetch_many(self, urls: List[str]) -> List[FetchResult]:
        if self.backend == "sync":
            import requests

            results = []
            for url in urls:
                start_time = time.time()
                try:
                    response = requests.get(
                        url,
                        headers=self.headers,
                        timeout=self.timeout,
                    )
                    elapsed = (time.time() - start_time) * 1000
                    if response.status_code == 200:
                        results.append(
                            FetchResult(
                                url=url,
                                content=response.text,
                                status_code=response.status_code,
                                success=True,
                                error=None,
                                elapsed_ms=elapsed,
                            )
                        )
                    elif response.status_code in (403, 429):
                        msg, se = _rate_limit_error(url, response.status_code)
                        results.append(
                            FetchResult(
                                url=url,
                                content=None,
                                status_code=response.status_code,
                                success=False,
                                error=msg,
                                elapsed_ms=elapsed,
                                structured_error=se,
                            )
                        )
                    else:
                        msg, se = _http_error(url, response.status_code)
                        results.append(
                            FetchResult(
                                url=url,
                                content=None,
                                status_code=response.status_code,
                                success=False,
                                error=msg,
                                elapsed_ms=elapsed,
                                structured_error=se,
                            )
                        )
                except requests.exceptions.Timeout:
                    elapsed = (time.time() - start_time) * 1000
                    msg, se = _timeout_error(url)
                    results.append(
                        FetchResult(
                            url=url,
                            content=None,
                            status_code=None,
                            success=False,
                            error=msg,
                            elapsed_ms=elapsed,
                            structured_error=se,
                        )
                    )
                except Exception as e:
                    elapsed = (time.time() - start_time) * 1000
                    msg, se = _network_error(url, str(e))
                    results.append(
                        FetchResult(
                            url=url,
                            content=None,
                            status_code=None,
                            success=False,
                            error=msg,
                            elapsed_ms=elapsed,
                            structured_error=se,
                        )
                    )
            return results

        else:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    import concurrent.futures

                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(
                            asyncio.run, self.fetch_many_async(urls)
                        )
                        return future.result()
                else:
                    return loop.run_until_complete(self.fetch_many_async(urls))
            except RuntimeError:
                return asyncio.run(self.fetch_many_async(urls))


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------


def fetch_urls_concurrently(
    urls: List[str],
    max_concurrent: int = 50,
    timeout: float = 10.0,
    headers: Optional[Dict[str, str]] = None,
) -> List[FetchResult]:
    fetcher = AsyncFetcher(
        max_concurrent=max_concurrent,
        timeout=timeout,
        headers=headers,
    )
    return fetcher.fetch_many(urls)

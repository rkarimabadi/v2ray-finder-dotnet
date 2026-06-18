"""Command-line interface for v2ray-finder.

Non-interactive path (``-o`` / ``--stats-only``) is wired through
:class:`~pipeline.Pipeline` (V1-A1).  Interactive menu still talks
directly to :class:`~core.V2RayServerFinder` and will be migrated
under V1-A2.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
from getpass import getpass
from typing import Any, Dict, List, Optional

from .core import V2RayServerFinder
from .exceptions import AuthenticationError, RateLimitError
from .pipeline import Pipeline
from .pipeline import StopController as PipelineStopController


# ---------------------------------------------------------------------------
# CLI-only StopController (interactive background 'q' listener)
# ---------------------------------------------------------------------------

class _CLIStopController:
    """
    Thread-safe stop controller for **non-interactive** CLI mode only.

    Starts a single daemon thread that blocks on ``input()`` and calls
    ``finder.request_stop()`` when the user types ``q`` + Enter.
    """

    def __init__(self, finder: V2RayServerFinder) -> None:
        self._finder = finder
        self._active = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._finder.reset_stop()
        self._active.set()
        print(
            "\n[i] Press 'q' + Enter at any time to stop and save partial results\n",
            flush=True,
        )
        self._thread = threading.Thread(
            target=self._listen, daemon=True, name="StopListener"
        )
        self._thread.start()

    def _listen(self) -> None:
        if not sys.stdin.isatty():
            self._active.clear()
            return
        while self._active.is_set():
            try:
                key = input().strip().lower()
                if key == "q":
                    print(
                        "\n[!] Stop requested \u2014 finishing current request...",
                        flush=True,
                    )
                    self._finder.request_stop()
                    self._active.clear()
                    break
            except (EOFError, OSError):
                self._active.clear()
                break

    def stop(self) -> None:
        self._active.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def print_stats(
    servers: List,
    show_health: bool = False,
    show_xray: bool = False,
    pipeline_stats: Optional[Dict[str, Any]] = None,
) -> None:
    """Print statistics about fetched servers."""
    if not servers:
        print("No servers found.")
        return

    protocols: dict = {}
    for server in servers:
        if isinstance(server, dict):
            proto = server.get("protocol", "unknown")
        else:
            proto = server.split("://")[0] if "://" in server else "unknown"
        protocols[proto] = protocols.get(proto, 0) + 1

    print(f"\nTotal servers: {len(servers)}")
    print("\nBy protocol:")
    for proto, count in sorted(protocols.items(), key=lambda x: x[1], reverse=True):
        print(f"  {proto}: {count}")

    if pipeline_stats:
        print("\nPipeline stats:")
        for k, v in pipeline_stats.items():
            print(f"  {k}: {v}")

    if show_health and servers and isinstance(servers[0], dict):
        healthy    = sum(1 for s in servers if s.get("health_status") == "healthy")
        degraded   = sum(1 for s in servers if s.get("health_status") == "degraded")
        unreachable= sum(1 for s in servers if s.get("health_status") == "unreachable")
        invalid    = sum(1 for s in servers if s.get("health_status") == "invalid")
        print("\nHealth status:")
        print(f"  Healthy:     {healthy}")
        print(f"  Degraded:    {degraded}")
        print(f"  Unreachable: {unreachable}")
        print(f"  Invalid:     {invalid}")
        if healthy:
            avg_quality = (
                sum(s.get("quality_score", 0) for s in servers
                    if s.get("health_status") == "healthy") / healthy
            )
            avg_latency = (
                sum(s.get("latency_ms", 0) for s in servers
                    if s.get("health_status") == "healthy") / healthy
            )
            print(f"\nAverage quality (healthy): {avg_quality:.1f}/100")
            print(f"Average latency (healthy): {avg_latency:.1f}ms")

    if show_xray and servers and isinstance(servers[0], dict):
        reachable = sum(1 for s in servers if s.get("reachable"))
        g204      = sum(1 for s in servers if s.get("google_204_ok"))
        print("\nxray real-connectivity results:")
        print(f"  Reachable (proxy): {reachable}/{len(servers)}")
        print(f"  Google 204 OK:     {g204}/{len(servers)}")
        if reachable:
            avg_lat = (
                sum(s.get("latency_ms") or 0 for s in servers if s.get("reachable"))
                / reachable
            )
            print(f"  Avg real latency:  {avg_lat:.1f}ms")


def prompt_for_token() -> Optional[str]:
    """Prompt user for GitHub token via masked input."""
    print("\n=== GitHub Token Setup ===")
    print("A GitHub token increases rate limits from 60 to 5000 requests/hour.")
    print("Your token will NOT be stored and is only used for this session.\n")
    use_token = input("Do you want to provide a GitHub token? (y/n): ").strip().lower()
    if use_token == "y":
        print("\nPaste your GitHub token (input will be hidden):")
        token = getpass("Token: ").strip()
        if token:
            print("[\u2713] Token received\n")
            return token
        print("[!] No token provided, continuing without authentication\n")
        return None
    print("[i] Continuing without authentication\n")
    return None


def save_partial_results(
    servers: List, filename: str = "v2ray_servers_partial.txt"
) -> None:
    """Write partial results to *filename* and print a confirmation."""
    if not servers:
        print("No servers to save.")
        return
    try:
        configs: List[str]
        if servers and isinstance(servers[0], dict):
            configs = [s.get("config", "") for s in servers if s.get("config")]
        else:
            configs = list(servers)
        with open(filename, "w", encoding="utf-8") as fh:
            for server in configs:
                fh.write(f"{server}\n")
        print(f"\n[\u2713] Saved {len(configs)} servers to {filename}")
        print("    You can resume or use these servers.\n")
    except OSError as exc:
        print(f"\n[!] Failed to save partial results: {exc}\n")


# ---------------------------------------------------------------------------
# Interactive menu (still uses V2RayServerFinder — V1-A2)
# ---------------------------------------------------------------------------

def interactive_menu(finder: V2RayServerFinder) -> None:
    """Display interactive terminal menu (V1-A2: not yet migrated to Pipeline)."""
    partial_servers: List = []

    while True:
        print("\n=== V2Ray Server Finder ===")
        print("1. Fetch from known sources")
        print("2. Fetch with GitHub search")
        print("3. Fetch with health checking (TCP/HTTP)")
        print("4. Save to file")
        print("5. Show statistics only")
        print("6. Check rate limit info")
        print("7. Real connectivity check via xray (ground-truth)")
        print("0. Exit")

        try:
            choice = input("\nSelect option: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n\nGoodbye!")
            break

        if choice == "0":
            print("Goodbye!")
            break

        elif choice == "1":
            print("\nFetching from known sources...")
            print("(Press Ctrl+C to stop and save partial results)")
            finder.reset_stop()
            try:
                servers = finder.get_all_servers(use_github_search=False)
            except KeyboardInterrupt:
                finder.request_stop()
                servers = []
            partial_servers = servers
            if finder.should_stop() and servers:
                print(f"\n[!] Stopped early \u2014 {len(servers)} partial results")
                save_partial_results(servers)
            print_stats(servers)

        elif choice == "2":
            print("\nFetching with GitHub search (slower)...")
            print("(Press Ctrl+C to stop and save partial results)")
            finder.reset_stop()
            try:
                servers = finder.get_all_servers(use_github_search=True)
            except KeyboardInterrupt:
                finder.request_stop()
                servers = []
            partial_servers = servers
            if finder.should_stop() and servers:
                print(f"\n[!] Stopped early \u2014 {len(servers)} partial results")
                save_partial_results(servers)
            print_stats(servers)
            rate_info = finder.get_rate_limit_info()
            if rate_info:
                print(
                    f"\nAPI calls remaining: "
                    f"{rate_info['remaining']}/{rate_info['limit']}"
                )

        elif choice == "3":
            try:
                use_search = input("Use GitHub search? (y/n): ").strip().lower() == "y"
            except (KeyboardInterrupt, EOFError):
                continue
            print("\nFetching and checking server health (TCP/HTTP)...")
            print("(Press Ctrl+C to stop)")
            finder.reset_stop()
            try:
                servers = finder.get_servers_with_health(
                    use_github_search=use_search,
                    check_health=True,
                    health_timeout=5.0,
                    min_quality_score=0,
                    filter_unhealthy=False,
                )
            except KeyboardInterrupt:
                finder.request_stop()
                servers = []
            partial_servers = servers
            if finder.should_stop() and servers:
                print(f"\n[!] Stopped early \u2014 {len(servers)} partial results")
                save_partial_results(servers, "v2ray_servers_partial_health.txt")
            print_stats(servers, show_health=True)
            if servers:
                try:
                    show_top = input("\nShow top 10 by quality? (y/n): ").strip().lower()
                except (KeyboardInterrupt, EOFError):
                    show_top = "n"
                if show_top == "y":
                    print("\nTop 10 servers by quality:")
                    for i, s in enumerate(servers[:10], 1):
                        status  = s.get("health_status", "unknown")
                        quality = s.get("quality_score", 0)
                        latency = s.get("latency_ms", 0)
                        proto   = s.get("protocol", "?")
                        print(
                            f"{i:2d}. [{proto:8s}] Quality: {quality:5.1f} "
                            f"| Latency: {latency:6.1f}ms | Status: {status}"
                        )

        elif choice == "4":
            try:
                filename = (
                    input("Enter filename (default: v2ray_servers.txt): ").strip()
                    or "v2ray_servers.txt"
                )
                use_search   = input("Use GitHub search? (y/n): ").strip().lower() == "y"
                check_health = input("Check server health? (y/n): ").strip().lower() == "y"
                limit_str    = input("Limit (0 for all): ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\n[!] Cancelled")
                continue
            limit = int(limit_str) if limit_str and limit_str != "0" else None
            print(f"\nSaving to {filename}...")
            finder.reset_stop()
            try:
                if check_health:
                    print("(Health checking enabled \u2014 this will take longer)")
                    health_data = finder.get_servers_with_health(
                        use_github_search=use_search,
                        check_health=True,
                        health_timeout=5.0,
                        min_quality_score=50.0,
                        filter_unhealthy=True,
                    )
                    output: List[str] = [s["config"] for s in health_data]
                else:
                    raw    = finder.get_all_servers(use_github_search=use_search)
                    output = list(raw)
            except KeyboardInterrupt:
                finder.request_stop()
                output = []
            if limit:
                output = output[:limit]
            partial_servers = output
            if finder.should_stop():
                save_partial_results(output, filename)
            else:
                try:
                    with open(filename, "w", encoding="utf-8") as fh:
                        for server in output:
                            fh.write(f"{server}\n")
                    print(f"Saved {len(output)} servers to {filename}")
                except OSError as exc:
                    print(f"[!] Could not write file: {exc}")

        elif choice == "5":
            try:
                use_search   = input("Use GitHub search? (y/n): ").strip().lower() == "y"
                check_health = input("Check server health? (y/n): ").strip().lower() == "y"
            except (KeyboardInterrupt, EOFError):
                continue
            print("\nFetching servers for statistics...")
            print("(Press Ctrl+C to stop and save partial results)")
            finder.reset_stop()
            try:
                if check_health:
                    servers = finder.get_servers_with_health(
                        use_github_search=use_search,
                        check_health=True,
                        health_timeout=5.0,
                    )
                else:
                    servers = finder.get_all_servers(use_github_search=use_search)
            except KeyboardInterrupt:
                finder.request_stop()
                servers = []
            partial_servers = servers
            if finder.should_stop() and servers:
                print(f"\n[!] Stopped early \u2014 {len(servers)} partial results")
                save_partial_results(servers)
            print_stats(servers, show_health=check_health)

        elif choice == "6":
            rate_info = finder.get_rate_limit_info()
            if rate_info:
                print("\nGitHub API Rate Limit:")
                print(f"  Limit:     {rate_info['limit']}")
                print(f"  Remaining: {rate_info['remaining']}")
                if rate_info["reset"]:
                    from datetime import datetime
                    reset_time = datetime.fromtimestamp(rate_info["reset"])
                    print(f"  Resets at: {reset_time}")
            else:
                print(
                    "\nNo rate limit info available yet. "
                    "Make a GitHub API call first."
                )

        elif choice == "7":
            try:
                use_search = input("Use GitHub search? (y/n): ").strip().lower() == "y"
                limit_str  = input("Limit servers to check (0 for all): ").strip()
            except (KeyboardInterrupt, EOFError):
                continue
            limit = int(limit_str) if limit_str and limit_str != "0" else None
            print("\nFetching servers and running xray real-connectivity checks...")
            print("(This requires xray binary; may take several minutes)")
            finder.reset_stop()
            try:
                servers = finder.get_all_servers(use_github_search=use_search)
                if limit:
                    servers = list(servers)[:limit]
                results = finder.get_servers_with_real_health(servers)
            except KeyboardInterrupt:
                finder.request_stop()
                results = []
            except Exception as exc:
                print(f"[!] xray check failed: {exc}")
                results = []
            partial_servers = results
            print_stats(results, show_xray=True)

        else:
            print("Invalid option. Please try again.")


# ---------------------------------------------------------------------------
# Non-interactive pipeline runner (V1-A1)
# ---------------------------------------------------------------------------

def _run_pipeline(
    args: argparse.Namespace,
    token: Optional[str],
) -> int:
    """Execute the non-interactive path via :class:`~pipeline.Pipeline`.

    Returns the process exit code (0 = success, 1 = error, 130 = stopped).
    """
    stop_ctrl = PipelineStopController()

    # Wire Ctrl+C to the pipeline stop event
    import signal
    original_sigint = signal.getsignal(signal.SIGINT)

    def _on_sigint(sig: int, frame: Any) -> None:  # type: ignore[type-arg]
        stop_ctrl.stop()

    signal.signal(signal.SIGINT, _on_sigint)

    if not args.quiet:
        action     = "GitHub search" if args.search else "known sources"
        health_note= " with health checking" if args.check_health else ""
        xray_note  = " with xray real-check" if args.xray_check else ""
        print(f"Fetching servers from {action}{health_note}{xray_note}...")
        print("[i] Press Ctrl+C at any time to stop and save partial results\n")

    pipeline = Pipeline(
        check_health     = args.check_health or args.xray_check,
        check_http_probe = False,
        check_google_204 = args.xray_check,
        timeout          = args.health_timeout,
        min_quality_score= args.min_quality,
        limit            = args.limit,
        binary_path      = getattr(args, "xray_binary", None),
        github_token     = token,
    )

    result = pipeline.run(stop_event=stop_ctrl.event)

    # Restore original SIGINT handler
    signal.signal(signal.SIGINT, original_sigint)

    if stop_ctrl.is_set():
        print("\n[!] Operation stopped by user")
        out_file = args.output if args.output else "v2ray_servers_partial.txt"
        save_partial_results(
            [{"config": c} for c in result.top_configs] if result.scores
            else [{"config": c} for c in result.configs],
            out_file,
        )
        print_stats(
            result.scores or result.configs,  # type: ignore[arg-type]
            show_health=args.check_health,
            show_xray=args.xray_check,
            pipeline_stats=result.stats,
        )
        return 130

    # Determine output list
    if result.scores:
        output_configs = result.top_configs
    else:
        output_configs = result.configs

    if args.limit:
        output_configs = output_configs[: args.limit]

    if args.stats_only:
        print_stats(
            result.health_dicts or [{"config": c} for c in output_configs],
            show_health=args.check_health,
            show_xray=args.xray_check,
            pipeline_stats=result.stats,
        )
        return 0

    if args.output:
        try:
            with open(args.output, "w", encoding="utf-8") as fh:
                for cfg in output_configs:
                    fh.write(f"{cfg}\n")
        except OSError as exc:
            print(f"\nFailed to write {args.output}: {exc}", file=sys.stderr)
            return 1
        if not args.quiet:
            print(f"\n[\u2713] Saved {len(output_configs)} servers to {args.output}")
            if result.stats:
                print(
                    f"    (fetched {result.stats.get('fetched', '?')}, "
                    f"deduped {result.stats.get('deduped', '?')}, "
                    f"healthy {result.stats.get('healthy', '?')})"
                )

    return 0


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch and aggregate V2Ray server configs from GitHub",
        epilog="For security, use GITHUB_TOKEN environment variable instead of -t flag.",
    )
    parser.add_argument(
        "-t", "--token",
        help="GitHub token (DEPRECATED: use GITHUB_TOKEN env var instead)",
    )
    parser.add_argument(
        "--prompt-token", action="store_true",
        help="Prompt for GitHub token interactively (secure input)",
    )
    parser.add_argument("-o", "--output", help="Output filename for saving servers")
    parser.add_argument(
        "-s", "--search", action="store_true",
        help="Include GitHub repository search",
    )
    parser.add_argument("-l", "--limit", type=int, help="Limit number of servers")
    parser.add_argument(
        "--stats-only", action="store_true", help="Only show statistics"
    )
    parser.add_argument("-q", "--quiet", action="store_true", help="Minimal output")
    parser.add_argument(
        "-c", "--check-health", action="store_true",
        help="Check server health (TCP connectivity and latency)",
    )
    parser.add_argument(
        "--min-quality", type=float, default=0.0,
        help="Minimum quality score (0-100, default: 0)",
    )
    parser.add_argument(
        "--health-timeout", type=float, default=5.0,
        help="Health check timeout in seconds (default: 5.0)",
    )
    parser.add_argument(
        "--xray-check", action="store_true",
        help="Run real connectivity check via xray proxy (ground-truth)",
    )
    parser.add_argument(
        "--xray-binary",
        help="Path to xray binary (auto-downloaded if not found)",
    )
    parser.add_argument(
        "--xray-no-download", action="store_true",
        help="Disable automatic xray binary download",
    )
    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Main CLI entry point."""
    parser = _build_parser()
    args   = parser.parse_args()

    # --- Token resolution ---
    token: Optional[str] = None
    if args.token:
        token = args.token
        print(
            "WARNING: Passing tokens via command line is insecure!\n"
            "         Token may appear in shell history and process listings.\n"
            f"         Use environment variable: export GITHUB_TOKEN='your_token'\n",
            file=sys.stderr,
        )
    elif args.prompt_token:
        token = prompt_for_token()

    token_from_env = os.environ.get("GITHUB_TOKEN")
    if not token and token_from_env:
        token = token_from_env
        if not args.quiet:
            print("[i] Using GitHub token from GITHUB_TOKEN environment variable")
    elif not token and not args.prompt_token and not any([args.output, args.stats_only]):
        token = prompt_for_token()

    # --- Interactive mode (V1-A2: still uses V2RayServerFinder) ---
    if not any([args.output, args.stats_only]):
        finder = V2RayServerFinder(token=token)
        interactive_menu(finder)
        return

    # --- Non-interactive mode via Pipeline (V1-A1) ---
    sys.exit(_run_pipeline(args, token))


if __name__ == "__main__":
    main()

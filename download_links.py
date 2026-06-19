from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import secrets
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import quote, unquote, urlparse
from urllib.request import Request, urlopen

import aiohttp
import yaml
from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)


DEFAULT_INPUT_FOLDER = Path(r"G:\Download-IDM")
DEFAULT_OUTPUT_FOLDER = Path(r"K:\0_worldpop_Nie")
DEFAULT_CONFIG_DIR = Path(os.environ.get("APPDATA", "")) / "io.github.clash-verge-rev.clash-verge-rev"
DEFAULT_TARGET_MBPS = 100.0
DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_METADATA_CONCURRENCY = 8
DEFAULT_START_CONCURRENCY = 2
DEFAULT_CONTROLLER_SAMPLE_SECONDS = 12.0
DEFAULT_RETRIES = 4
MAX_CONCURRENCY_CAP = 16
DEFAULT_BENCHMARK_BYTES_MB = 24
DEFAULT_BENCHMARK_CANDIDATES = 0
DEFAULT_ROUTE_RECHECK_MINUTES = 8.0
DEFAULT_ROUTE_SWITCH_GAIN = 1.18
DEFAULT_ADJUST_COOLDOWN_SECONDS = 36.0
DEFAULT_SETTLE_SECONDS = 24.0
DEFAULT_MIN_SPEED_FOR_GROWTH_MBPS = 1.0
DEFAULT_ARIA2_SPLIT = 1
DEFAULT_ARIA2_MIN_SPLIT_SIZE = "16M"
DEFAULT_ARIA2_CONCURRENT_FILES = 8
DEFAULT_BENCHMARK_SECONDS = 6.0
PREFERRED_PROXY_TOKENS = [
    "uk",
    "gb",
    "london",
    "england",
    "britain",
    "europe",
    "netherlands",
    "germany",
    "france",
    "united kingdom",
    "英国",
    "伦敦",
    "欧洲",
    "荷兰",
    "德国",
    "法国",
]
MANUAL_GROUP_CANDIDATES = [
    "✈️ 手动选择",
    "节点选择",
    "Proxy",
    "PROXY",
    "Manual",
]
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PythonDownloader/1.0"

console = Console()


@dataclass(slots=True)
class LinkItem:
    url: str
    source_file: Path
    output_path: Path
    temp_path: Path
    remote_size: int | None = None
    accept_ranges: bool = False
    skip_reason: str | None = None
    resumed_bytes: int = 0
    failed_reason: str | None = None

    @property
    def display_name(self) -> str:
        return self.output_path.name

    @property
    def remaining_bytes(self) -> int | None:
        if self.remote_size is None:
            return None
        return max(self.remote_size - self.resumed_bytes, 0)


@dataclass
class RunStats:
    downloaded_bytes: int = 0
    completed: int = 0
    skipped: int = 0
    failed: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RouteChoice:
    label: str
    proxy: str | None
    node_name: str | None
    latency_ms: int | None = None
    throughput_mbps: float = 0.0


@dataclass
class RouteRuntimeState:
    active_route: RouteChoice
    proxy_url: str | None
    controller_url: str | None
    secret: str | None
    group_name: str | None
    proxy_node_name: str | None
    benchmark_bytes_mb: int
    recheck_seconds: float
    switch_gain: float


class PauseDownload(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download all URLs from txt files in a folder with progress bars and adaptive concurrency."
    )
    parser.add_argument(
        "--input-folder",
        type=Path,
        default=DEFAULT_INPUT_FOLDER,
        help="Folder that contains the txt files with download URLs.",
    )
    parser.add_argument(
        "--output-folder",
        type=Path,
        default=DEFAULT_OUTPUT_FOLDER,
        help="Folder where downloaded files are stored.",
    )
    parser.add_argument(
        "--folder",
        type=Path,
        default=None,
        help="Backward-compatible alias. If set, it is used as both input and output folder.",
    )
    parser.add_argument("--proxy", default="auto", help='Proxy URL. Use "auto" to detect Clash, or "direct" to disable proxy.')
    parser.add_argument(
        "--controller",
        default="auto",
        help='Clash controller URL. Use "auto" to detect it, or "off" to disable controller-based node switching.',
    )
    parser.add_argument("--secret", default=None, help="Clash controller secret. Defaults to the value found in config.yaml.")
    parser.add_argument(
        "--prefer",
        default="uk,gb,london,united kingdom,europe,netherlands,germany,france,英国,伦敦,欧洲",
        help="Comma-separated node keywords used only to shortlist proxy candidates before measurement.",
    )
    parser.add_argument("--start-concurrency", type=int, default=DEFAULT_START_CONCURRENCY, help="Initial concurrent downloads.")
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=0,
        help="Maximum concurrent downloads. Default is auto-derived from target bandwidth.",
    )
    parser.add_argument("--target-mbps", type=float, default=DEFAULT_TARGET_MBPS, help="Expected peak throughput in MB/s.")
    parser.add_argument("--metadata-concurrency", type=int, default=DEFAULT_METADATA_CONCURRENCY, help="Concurrent HEAD probes.")
    parser.add_argument("--chunk-size-mb", type=int, default=1, help="Chunk size per read in MB.")
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="Retries per file.")
    parser.add_argument(
        "--backend",
        choices=["auto", "python", "aria2c", "arcgis"],
        default="auto",
        help='Download backend. "arcgis" uses the resumable WorldPop ArcGIS ImageServer and local mosaicking.',
    )
    parser.add_argument("--aria2c-path", default=None, help="Path to aria2c executable. Defaults to PATH lookup.")
    parser.add_argument(
        "--aria2-split",
        type=int,
        default=DEFAULT_ARIA2_SPLIT,
        help="aria2c per-file split count and max connections per server.",
    )
    parser.add_argument(
        "--aria2-concurrent-files",
        type=int,
        default=DEFAULT_ARIA2_CONCURRENT_FILES,
        help="Number of files aria2c downloads at the same time.",
    )
    parser.add_argument(
        "--aria2-min-split-size",
        default=DEFAULT_ARIA2_MIN_SPLIT_SIZE,
        help="aria2c minimum split size, for example 16M.",
    )
    parser.add_argument(
        "--arcgis-tile-size",
        type=int,
        default=4096,
        help="ArcGIS export tile width/height in pixels (256-4096).",
    )
    parser.add_argument(
        "--arcgis-split",
        type=int,
        default=8,
        help="aria2c connections per ArcGIS tile.",
    )
    parser.add_argument(
        "--arcgis-concurrent-tiles",
        type=int,
        default=3,
        help="ArcGIS tiles exported/downloaded concurrently.",
    )
    parser.add_argument(
        "--arcgis-keep-tiles",
        action="store_true",
        help="Keep temporary ArcGIS tiles after a final GeoTIFF passes validation.",
    )
    parser.add_argument(
        "--benchmark-bytes-mb",
        type=int,
        default=DEFAULT_BENCHMARK_BYTES_MB,
        help="Sample size per route benchmark before downloads start.",
    )
    parser.add_argument(
        "--benchmark-candidates",
        type=int,
        default=DEFAULT_BENCHMARK_CANDIDATES,
        help="Maximum proxy nodes to throughput-test before download; 0 tests every real node (default).",
    )
    parser.add_argument(
        "--route-recheck-minutes",
        type=float,
        default=DEFAULT_ROUTE_RECHECK_MINUTES,
        help="Minutes between mid-download route rechecks of direct versus the selected proxy node.",
    )
    parser.add_argument(
        "--route-switch-gain",
        type=float,
        default=DEFAULT_ROUTE_SWITCH_GAIN,
        help="Minimum throughput multiplier required before switching routes mid-download.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Only download the first N URLs after de-duplication.")
    parser.add_argument("--dry-run", action="store_true", help="Inspect the folder and URLs without downloading files.")
    return parser.parse_args()


def clamp(value: int, lower: int, upper: int) -> int:
    return max(lower, min(value, upper))


def derive_max_concurrency(target_mbps: float, requested: int) -> int:
    if requested > 0:
        return clamp(requested, 1, MAX_CONCURRENCY_CAP)
    estimate = round(target_mbps / 8.0)
    return clamp(int(estimate), 6, MAX_CONCURRENCY_CAP)


def find_aria2c(path_hint: str | None) -> str | None:
    if path_hint:
        candidate = Path(path_hint)
        if candidate.exists():
            return str(candidate)
    path_match = shutil.which("aria2c")
    if path_match:
        return path_match

    winget_root = Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "WinGet" / "Packages"
    candidates = sorted(
        winget_root.glob("aria2.aria2_*/*/aria2c.exe"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return str(candidates[0]) if candidates else None


def is_listening(host: str, port: int, timeout: float = 0.4) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    content = path.read_text(encoding="utf-8")
    data = yaml.safe_load(content) or {}
    return data if isinstance(data, dict) else {}


def detect_clash(config_dir: Path) -> dict[str, str | None]:
    config = load_yaml(config_dir / "config.yaml")
    mixed_port = config.get("mixed-port")
    controller = config.get("external-controller")
    secret = config.get("secret")

    proxy_url = None
    if isinstance(mixed_port, int) and is_listening("127.0.0.1", mixed_port):
        proxy_url = f"http://127.0.0.1:{mixed_port}"

    controller_url = None
    if isinstance(controller, str) and controller.strip():
        candidate = controller if controller.startswith("http") else f"http://{controller}"
        parsed = urlparse(candidate)
        if parsed.hostname and parsed.port and is_listening(parsed.hostname, parsed.port):
            controller_url = candidate

    return {
        "proxy_url": proxy_url,
        "controller_url": controller_url,
        "secret": secret if isinstance(secret, str) else None,
    }


def detect_current_clash_node(config_dir: Path) -> dict[str, str]:
    profiles = load_yaml(config_dir / "profiles.yaml")
    current_uid = profiles.get("current")
    items = profiles.get("items", [])
    if not isinstance(items, list):
        return {}

    for item in items:
        if not isinstance(item, dict) or item.get("uid") != current_uid:
            continue
        selected = item.get("selected", [])
        result: dict[str, str] = {}
        if isinstance(selected, list):
            for entry in selected:
                if isinstance(entry, dict) and entry.get("name") and entry.get("now"):
                    result[str(entry["name"])] = str(entry["now"])
        return result
    return {}


def get_preferred_tokens(raw: str) -> list[str]:
    return [token.strip() for token in raw.split(",") if token.strip()]


def summarize_route_measurement(active_route: RouteChoice) -> str:
    if active_route.throughput_mbps <= 0:
        if active_route.proxy:
            return "Route measurement failed; using the current Clash proxy without a measured speed result."
        return "Route measurement failed; using a direct connection without a measured speed result."
    if active_route.node_name == "DIRECT" or active_route.proxy is None:
        return "Startup route selection used measured latency/throughput to the actual download host and chose direct."
    return (
        f'Startup route selection used measured latency/throughput to the actual download host and chose '
        f'"{active_route.node_name}".'
    )


def read_urls(input_folder: Path, output_folder: Path, limit: int) -> list[LinkItem]:
    txt_files = sorted(path for path in input_folder.glob("*.txt") if path.is_file())
    if not txt_files:
        raise FileNotFoundError(f"No .txt files were found in {input_folder}")

    seen_urls: set[str] = set()
    filename_counts: dict[str, int] = {}
    items: list[LinkItem] = []

    for txt_file in txt_files:
        for raw_line in txt_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            url = raw_line.strip()
            if not url or not url.startswith(("http://", "https://")) or url in seen_urls:
                continue
            seen_urls.add(url)
            parsed = urlparse(url)
            filename = unquote(Path(parsed.path).name) or f"download_{len(items) + 1}.bin"

            name_count = filename_counts.get(filename, 0)
            filename_counts[filename] = name_count + 1
            if name_count:
                stem = Path(filename).stem
                suffix = Path(filename).suffix
                filename = f"{stem}_{name_count + 1}{suffix}"

            output_path = output_folder / filename
            temp_path = output_path.with_name(output_path.name + ".part")
            items.append(LinkItem(url=url, source_file=txt_file, output_path=output_path, temp_path=temp_path))

            if limit > 0 and len(items) >= limit:
                return items

    return items


def parse_remote_size(headers: aiohttp.typedefs.LooseHeaders) -> int | None:
    length = headers.get("Content-Length")
    if length and str(length).isdigit():
        return int(str(length))
    content_range = headers.get("Content-Range")
    if isinstance(content_range, str) and "/" in content_range:
        tail = content_range.rsplit("/", 1)[-1]
        if tail.isdigit():
            return int(tail)
    return None


async def inspect_item(session: aiohttp.ClientSession, item: LinkItem, proxy: str | None) -> None:
    headers = {"User-Agent": USER_AGENT}
    try:
        async with session.head(item.url, allow_redirects=True, proxy=proxy, headers=headers) as response:
            if response.status >= 400 or response.status == 405:
                raise aiohttp.ClientResponseError(
                    response.request_info,
                    response.history,
                    status=response.status,
                    message="HEAD probe failed",
                    headers=response.headers,
                )
            item.remote_size = parse_remote_size(response.headers)
            item.accept_ranges = False
    except Exception:
        fallback_headers = headers | {"Range": "bytes=0-0"}
        async with session.get(item.url, allow_redirects=True, proxy=proxy, headers=fallback_headers) as response:
            response.raise_for_status()
            item.remote_size = parse_remote_size(response.headers)
            item.accept_ranges = response.status == 206 and response.headers.get("Content-Range", "").startswith("bytes 0-0/")
            response.release()

    # Some servers advertise Accept-Ranges but ignore Range and return HTTP 200.
    # Only a real 206 response is safe for segmented downloads or resume.
    if not item.accept_ranges:
        range_headers = headers | {"Range": "bytes=0-0"}
        async with session.get(item.url, allow_redirects=True, proxy=proxy, headers=range_headers) as response:
            response.raise_for_status()
            item.accept_ranges = (
                response.status == 206
                and response.headers.get("Content-Range", "").startswith("bytes 0-0/")
            )
            await response.content.read(1)

    if item.output_path.exists():
        local_size = item.output_path.stat().st_size
        if item.remote_size is None or local_size == item.remote_size:
            item.skip_reason = "already exists"
            return

    if item.temp_path.exists():
        part_size = item.temp_path.stat().st_size
        if item.remote_size is not None and part_size >= item.remote_size:
            item.temp_path.replace(item.output_path)
            item.skip_reason = "completed from .part"
            return
        if item.accept_ranges and part_size > 0:
            item.resumed_bytes = part_size


async def inspect_items(
    items: list[LinkItem],
    proxy: str | None,
    metadata_concurrency: int,
) -> tuple[int, int]:
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=20, sock_read=30)
    connector = aiohttp.TCPConnector(limit=metadata_concurrency, ttl_dns_cache=300)
    semaphore = asyncio.Semaphore(metadata_concurrency)
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=28),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    )

    with progress:
        task_id = progress.add_task("Inspecting URLs", total=len(items))

        async with aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=False) as session:
            async def wrapped(item: LinkItem) -> None:
                async with semaphore:
                    try:
                        await inspect_item(session, item, proxy)
                    finally:
                        progress.advance(task_id, 1)

            results = await asyncio.gather(*(wrapped(item) for item in items), return_exceptions=True)
            for item, result in zip(items, results):
                if isinstance(result, Exception):
                    console.print(f"[yellow]Metadata probe failed for {item.display_name}:[/yellow] {result}")

    total_bytes = 0
    unknown_count = 0
    for item in items:
        if item.skip_reason:
            continue
        if item.remaining_bytes is None:
            unknown_count += 1
            continue
        total_bytes += item.remaining_bytes
    return total_bytes, unknown_count


async def controller_request(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    secret: str | None,
    **kwargs: Any,
) -> Any:
    headers = dict(kwargs.pop("headers", {}))
    if secret and secret != "set-your-secret":
        headers["Authorization"] = f"Bearer {secret}"
    async with session.request(method, url, headers=headers, **kwargs) as response:
        response.raise_for_status()
        if response.content_type == "application/json":
            return await response.json()
        return await response.text()


async def get_proxy_group_candidates(
    controller_url: str | None,
    secret: str | None,
    preferred_tokens: list[str],
    max_candidates: int,
) -> tuple[str | None, str | None, list[tuple[str, int | None]]]:
    if not controller_url:
        return None, None, []

    timeout = aiohttp.ClientTimeout(total=20, sock_connect=5, sock_read=12)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
        data = await controller_request(session, "GET", f"{controller_url}/proxies", secret)
        if not isinstance(data, dict):
            return None, None, []

        proxies = data.get("proxies", {})
        if not isinstance(proxies, dict):
            return None, None, []

        group_name = next((name for name in MANUAL_GROUP_CANDIDATES if name in proxies), None)
        if not group_name:
            return None, None, []

        group = proxies.get(group_name, {})
        if not isinstance(group, dict):
            return group_name, None, []

        candidates = group.get("all", [])
        current_name = group.get("now")
        if not isinstance(candidates, list):
            return group_name, current_name if isinstance(current_name, str) else None, []

        all_names = [
            name
            for name in candidates
            if isinstance(name, str)
            and isinstance(proxies.get(name), dict)
            and proxies[name].get("type") not in {"Direct", "Reject", "Selector", "URLTest", "Fallback", "LoadBalance"}
        ]

        def cached_delay(name: str) -> int | None:
            history = proxies.get(name, {}).get("history", [])
            if not isinstance(history, list):
                return None
            for entry in reversed(history):
                delay = entry.get("delay") if isinstance(entry, dict) else None
                if isinstance(delay, int) and delay > 0:
                    return delay
            return None

        target_count = len(all_names) if max_candidates <= 0 else min(max_candidates, len(all_names))
        probe_list: list[str] = []
        if isinstance(current_name, str) and current_name in all_names:
            probe_list.append(current_name)
        for name in all_names:
            if name not in probe_list:
                probe_list.append(name)
            if len(probe_list) >= target_count:
                break

        shortlist = [(name, cached_delay(name)) for name in probe_list]

        return group_name, current_name if isinstance(current_name, str) else None, shortlist


async def set_proxy_group_node(
    controller_url: str | None,
    secret: str | None,
    group_name: str | None,
    node_name: str | None,
) -> None:
    if not controller_url or not group_name or not node_name:
        return

    timeout = aiohttp.ClientTimeout(total=12, sock_connect=5, sock_read=8)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
        await controller_request(
            session,
            "PUT",
            f"{controller_url}/proxies/{quote(group_name, safe='')}",
            secret,
            json={"name": node_name},
        )


async def benchmark_route(
    url: str,
    proxy: str | None,
    benchmark_bytes_mb: int,
) -> float:
    limit_bytes = max(4, benchmark_bytes_mb) * 1024 * 1024
    timeout = aiohttp.ClientTimeout(total=18, sock_connect=8, sock_read=10)
    connector = aiohttp.TCPConnector(limit=1, ttl_dns_cache=300)
    headers = {
        "User-Agent": USER_AGENT,
        "Range": f"bytes=0-{limit_bytes - 1}",
    }

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=False) as session:
        started = time.monotonic()
        total = 0
        async with session.get(url, allow_redirects=True, proxy=proxy, headers=headers) as response:
            response.raise_for_status()
            async for chunk in response.content.iter_chunked(512 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if total >= limit_bytes or time.monotonic() - started >= DEFAULT_BENCHMARK_SECONDS:
                    break
        elapsed = max(time.monotonic() - started, 1e-6)
    if total <= 0:
        raise RuntimeError("route returned no download data")
    return total / elapsed / 1024 / 1024


async def benchmark_latency(
    url: str,
    proxy: str | None,
) -> int | None:
    timeout = aiohttp.ClientTimeout(total=20, sock_connect=10, sock_read=20)
    connector = aiohttp.TCPConnector(limit=1, ttl_dns_cache=300)
    headers = {"User-Agent": USER_AGENT}

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=False) as session:
        started = time.monotonic()
        try:
            async with session.head(url, allow_redirects=True, proxy=proxy, headers=headers) as response:
                response.raise_for_status()
        except Exception:
            fallback_headers = headers | {"Range": "bytes=0-0"}
            async with session.get(url, allow_redirects=True, proxy=proxy, headers=fallback_headers) as response:
                response.raise_for_status()
                await response.content.read(1)
        elapsed = max(time.monotonic() - started, 1e-6)
    return int(elapsed * 1000)


def format_exception(exc: Exception) -> str:
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


async def auto_select_download_route(
    sample_url: str,
    proxy_url: str | None,
    controller_url: str | None,
    secret: str | None,
    preferred_tokens: list[str],
    benchmark_bytes_mb: int,
    benchmark_candidates: int,
) -> tuple[RouteChoice, list[RouteChoice], str | None, str | None]:
    candidates: list[RouteChoice] = [RouteChoice(label="direct", proxy=None, node_name="DIRECT")]

    group_name = None
    original_node = None
    if proxy_url:
        if controller_url:
            group_name, original_node, ranked_nodes = await get_proxy_group_candidates(
                controller_url, secret, preferred_tokens, benchmark_candidates
            )
            for node_name, delay in ranked_nodes:
                candidates.append(
                    RouteChoice(label=f"proxy:{node_name}", proxy=proxy_url, node_name=node_name, latency_ms=delay)
                )
        else:
            candidates.append(RouteChoice(label="proxy:current", proxy=proxy_url, node_name=None))

    if len(candidates) == 1:
        candidates[0].throughput_mbps = await benchmark_route(sample_url, None, benchmark_bytes_mb)
        return candidates[0], candidates, group_name, original_node

    console.print("Benchmarking direct and proxy paths against the real download URL before download...")
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=28),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    )

    with progress:
        task_id = progress.add_task("Benchmarking routes", total=len(candidates))
        for candidate in candidates:
            try:
                progress.update(task_id, description=f"Testing {candidate.label}")
                if candidate.node_name and candidate.node_name != "DIRECT" and group_name:
                    await set_proxy_group_node(controller_url, secret, group_name, candidate.node_name)
                    await asyncio.sleep(1.0)
                candidate.throughput_mbps = await benchmark_route(sample_url, candidate.proxy, benchmark_bytes_mb)
            except Exception as exc:
                candidate.latency_ms = None
                candidate.throughput_mbps = 0.0
                console.print(f"[yellow]Route benchmark failed for {candidate.label}:[/yellow] {format_exception(exc)}")
            finally:
                progress.advance(task_id, 1)

    successful = [candidate for candidate in candidates if candidate.throughput_mbps > 0]
    if not successful:
        if group_name and original_node:
            await set_proxy_group_node(controller_url, secret, group_name, original_node)
        raise RuntimeError("all route benchmarks failed")

    best_choice = max(successful, key=lambda item: item.throughput_mbps)
    if group_name and original_node:
        if best_choice.node_name and best_choice.node_name != "DIRECT":
            await set_proxy_group_node(controller_url, secret, group_name, best_choice.node_name)
        else:
            await set_proxy_group_node(controller_url, secret, group_name, original_node)

    best_proxy_node = max(
        (item for item in successful if item.proxy is not None and item.node_name),
        key=lambda item: item.throughput_mbps,
        default=None,
    )
    return best_choice, candidates, group_name, (best_proxy_node.node_name if best_proxy_node else original_node)


async def benchmark_current_routes(
    sample_url: str,
    route_state: RouteRuntimeState,
) -> list[RouteChoice]:
    choices = [RouteChoice(label="direct", proxy=None, node_name="DIRECT")]
    try:
        choices[0].latency_ms = await benchmark_latency(sample_url, None)
        choices[0].throughput_mbps = await benchmark_route(sample_url, None, route_state.benchmark_bytes_mb)
    except Exception as exc:
        console.print(f"[yellow]Route recheck failed for direct:[/yellow] {format_exception(exc)}")

    if route_state.proxy_url and route_state.proxy_node_name and route_state.controller_url and route_state.group_name:
        await set_proxy_group_node(
            route_state.controller_url,
            route_state.secret,
            route_state.group_name,
            route_state.proxy_node_name,
        )
        await asyncio.sleep(1.0)
        proxy_choice = RouteChoice(
            label=f"proxy:{route_state.proxy_node_name}",
            proxy=route_state.proxy_url,
            node_name=route_state.proxy_node_name,
        )
        try:
            proxy_choice.latency_ms = await benchmark_latency(sample_url, route_state.proxy_url)
            proxy_choice.throughput_mbps = await benchmark_route(
                sample_url,
                route_state.proxy_url,
                route_state.benchmark_bytes_mb,
            )
        except Exception as exc:
            console.print(
                f"[yellow]Route recheck failed for {proxy_choice.label}:[/yellow] {format_exception(exc)}"
            )
        choices.append(proxy_choice)

    active = route_state.active_route
    if active.node_name and active.node_name != "DIRECT" and route_state.controller_url and route_state.group_name:
        await set_proxy_group_node(route_state.controller_url, route_state.secret, route_state.group_name, active.node_name)
    elif route_state.proxy_node_name and route_state.controller_url and route_state.group_name:
        await set_proxy_group_node(
            route_state.controller_url,
            route_state.secret,
            route_state.group_name,
            route_state.proxy_node_name,
        )

    return choices


async def try_switch_best_proxy(
    controller_url: str | None,
    secret: str | None,
    preferred_tokens: list[str],
) -> str | None:
    if not controller_url:
        return None

    timeout = aiohttp.ClientTimeout(total=12, sock_connect=5, sock_read=8)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
        data = await controller_request(session, "GET", f"{controller_url}/proxies", secret)
        if not isinstance(data, dict):
            return None
        proxies = data.get("proxies", {})
        if not isinstance(proxies, dict):
            return None

        group_name = next((name for name in MANUAL_GROUP_CANDIDATES if name in proxies), None)
        if not group_name:
            return None

        group = proxies.get(group_name, {})
        candidates = group.get("all", []) if isinstance(group, dict) else []
        if not isinstance(candidates, list) or not candidates:
            return None

        preferred = [
            name
            for name in candidates
            if isinstance(name, str) and any(token.lower() in name.lower() for token in preferred_tokens)
        ]
        probe_list = preferred or [name for name in candidates if isinstance(name, str)]
        if not probe_list:
            return None

        best_name = None
        best_delay = None
        for name in probe_list:
            encoded = quote(name, safe="")
            try:
                result = await controller_request(
                    session,
                    "GET",
                    f"{controller_url}/proxies/{encoded}/delay",
                    secret,
                    params={"url": "https://data.worldpop.org", "timeout": 5000},
                )
                delay = result.get("delay") if isinstance(result, dict) else None
                if isinstance(delay, int) and delay >= 0 and (best_delay is None or delay < best_delay):
                    best_name = name
                    best_delay = delay
            except Exception:
                continue

        if not best_name:
            return None

        await controller_request(
            session,
            "PUT",
            f"{controller_url}/proxies/{quote(group_name, safe='')}",
            secret,
            json={"name": best_name},
        )
        return best_name


async def download_one(
    session: aiohttp.ClientSession,
    item: LinkItem,
    proxy_getter: Callable[[], str | None],
    progress: Progress,
    total_task_id: int,
    stats: RunStats,
    chunk_size: int,
    retries: int,
    should_pause: Callable[[], str | None] | None = None,
) -> None:
    file_task_id = progress.add_task(item.display_name, total=item.remote_size, completed=item.resumed_bytes)
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(1, retries + 1):
        try:
            resume_from = 0
            write_mode = "wb"
            request_headers = dict(headers)

            if item.temp_path.exists() and item.accept_ranges:
                part_size = item.temp_path.stat().st_size
                if item.remote_size is not None and part_size < item.remote_size:
                    resume_from = part_size
                    write_mode = "ab"
                    request_headers["Range"] = f"bytes={part_size}-"
                    progress.update(file_task_id, completed=part_size)
                elif item.remote_size is not None and part_size >= item.remote_size:
                    item.temp_path.replace(item.output_path)
                    item.skip_reason = "completed from .part"
                    progress.remove_task(file_task_id)
                    stats.skipped += 1
                    return

            async with session.get(item.url, allow_redirects=True, proxy=proxy_getter(), headers=request_headers) as response:
                response.raise_for_status()
                if resume_from and response.status != 206:
                    resume_from = 0
                    write_mode = "wb"
                    progress.update(file_task_id, completed=0)

                if item.remote_size is None:
                    item.remote_size = parse_remote_size(response.headers)
                    progress.update(file_task_id, total=item.remote_size)

                with open(item.temp_path, write_mode) as handle:
                    async for chunk in response.content.iter_chunked(chunk_size):
                        if not chunk:
                            continue
                        handle.write(chunk)
                        size = len(chunk)
                        stats.downloaded_bytes += size
                        progress.update(file_task_id, advance=size)
                        progress.update(total_task_id, advance=size)
                        if should_pause and item.accept_ranges:
                            pause_reason = should_pause()
                            if pause_reason:
                                raise PauseDownload(pause_reason)

            item.temp_path.replace(item.output_path)
            stats.completed += 1
            progress.remove_task(file_task_id)
            return
        except PauseDownload:
            progress.remove_task(file_task_id)
            raise
        except Exception as exc:
            if attempt >= retries:
                item.failed_reason = str(exc)
                progress.remove_task(file_task_id)
                raise
            wait_seconds = min(2 ** attempt, 15)
            progress.update(file_task_id, description=f"retry {attempt}/{retries} {item.display_name}")
            await asyncio.sleep(wait_seconds)


def find_free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


def aria2_rpc(port: int, secret: str, method: str, params: list[Any] | None = None) -> Any:
    payload = {
        "jsonrpc": "2.0",
        "id": "download-links",
        "method": f"aria2.{method}",
        "params": [f"token:{secret}", *(params or [])],
    }
    request = Request(
        f"http://127.0.0.1:{port}/jsonrpc",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=4) as response:
        result = json.loads(response.read().decode("utf-8"))
    if "error" in result:
        raise RuntimeError(result["error"].get("message", "aria2 RPC error"))
    return result.get("result")


def aria2_snapshot(port: int, secret: str) -> tuple[int, int, int, int, list[str]]:
    keys = ["gid", "status", "totalLength", "completedLength", "files"]
    active = aria2_rpc(port, secret, "tellActive", [keys])
    waiting = aria2_rpc(port, secret, "tellWaiting", [0, 1000, keys])
    stopped = aria2_rpc(port, secret, "tellStopped", [0, 1000, keys])
    entries = {entry["gid"]: entry for entry in [*active, *waiting, *stopped]}

    completed = sum(int(entry.get("completedLength", 0)) for entry in entries.values())
    total = sum(int(entry.get("totalLength", 0)) for entry in entries.values())
    active_names: list[str] = []
    for entry in active[:3]:
        files = entry.get("files") or []
        if files and files[0].get("path"):
            active_names.append(Path(files[0]["path"]).name)
    return completed, total, len(active), len(waiting), active_names


def run_aria2c_downloads(
    items: list[LinkItem],
    output_folder: Path,
    proxy: str | None,
    concurrent_files: int,
    retries: int,
    aria2c_path: str,
    split: int,
    min_split_size: str,
) -> RunStats:
    pending_items = [item for item in items if not item.skip_reason]
    skipped_items = [item for item in items if item.skip_reason]
    stats = RunStats(skipped=len(skipped_items))

    if not pending_items:
        return stats

    manifest_path = output_folder / "_aria2_input.txt"
    log_path = output_folder / "_aria2.log"
    log_path.unlink(missing_ok=True)
    range_unsupported = [item for item in pending_items if not item.accept_ranges]
    effective_split = 1 if range_unsupported else max(1, split)
    if range_unsupported:
        console.print(
            f"[yellow]Server does not honor HTTP Range for {len(range_unsupported)} pending files; "
            "using one connection per file. Incomplete files cannot be resumed and will restart.[/yellow]"
        )

    initial_sizes: dict[Path, int] = {}
    lines: list[str] = []
    for item in pending_items:
        if item.temp_path.exists() and not item.output_path.exists():
            item.temp_path.replace(item.output_path)
        can_resume = item.accept_ranges
        initial_sizes[item.output_path] = (
            item.output_path.stat().st_size if can_resume and item.output_path.exists() else 0
        )
        if not can_resume:
            item.output_path.with_name(item.output_path.name + ".aria2").unlink(missing_ok=True)
        lines.append(item.url)
        lines.append(f" dir={output_folder.as_posix()}")
        lines.append(f" out={item.output_path.name}")
        lines.append(f" continue={'true' if can_resume else 'false'}")

    manifest_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    rpc_port = find_free_local_port()
    rpc_secret = secrets.token_urlsafe(24)
    command = [
        aria2c_path,
        f"--input-file={manifest_path}",
        "--enable-rpc=true",
        "--rpc-listen-all=false",
        f"--rpc-listen-port={rpc_port}",
        f"--rpc-secret={rpc_secret}",
        "--continue=true",
        "--auto-file-renaming=false",
        "--allow-overwrite=true",
        "--file-allocation=none",
        "--show-console-readout=false",
        "--summary-interval=0",
        "--console-log-level=warn",
        f"--log={log_path}",
        "--log-level=notice",
        "--download-result=hide",
        "--max-download-result=1000",
        "--check-certificate=true",
        "--disable-ipv6=true",
        "--connect-timeout=30",
        "--timeout=90",
        f"--max-concurrent-downloads={max(1, concurrent_files)}",
        f"--split={effective_split}",
        f"--max-connection-per-server={effective_split}",
        f"--min-split-size={min_split_size}",
        f"--max-tries={max(1, retries)}",
        "--retry-wait=3",
        f"--user-agent={USER_AGENT}",
    ]
    if proxy:
        command.append(f"--all-proxy={proxy}")
    else:
        command.append("--all-proxy=")

    console.print("Handing off downloads to aria2c...")

    env = os.environ.copy()
    for key in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ]:
        env.pop(key, None)

    creation_flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    process = subprocess.Popen(
        command,
        cwd=str(output_folder),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creation_flags,
    )

    rpc_ready = False
    for _ in range(30):
        if process.poll() is not None:
            break
        try:
            aria2_rpc(rpc_port, rpc_secret, "getVersion")
            rpc_ready = True
            break
        except (OSError, URLError, RuntimeError):
            time.sleep(0.25)

    if not rpc_ready:
        exit_code = process.wait(timeout=10)
        log_tail = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-12:] if log_path.exists() else []
        raise RuntimeError(f"aria2c RPC did not start (exit {exit_code}): {' | '.join(log_tail)}")

    known_total = sum(item.remote_size or 0 for item in pending_items)
    initial_completed = sum(initial_sizes.values())
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=28),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    )

    try:
        with progress:
            task_id = progress.add_task("ARIA2 starting", total=known_total or None, completed=initial_completed)
            idle_polls = 0
            while process.poll() is None:
                try:
                    completed, rpc_total, active_count, waiting_count, active_names = aria2_snapshot(
                        rpc_port, rpc_secret
                    )
                    description = f"ARIA2 active={active_count}, queued={waiting_count}"
                    if active_names:
                        description += f" | {', '.join(active_names)}"
                    progress.update(
                        task_id,
                        description=description,
                        total=known_total or rpc_total or None,
                        completed=completed,
                    )
                    idle_polls = idle_polls + 1 if active_count == 0 and waiting_count == 0 else 0
                    if idle_polls >= 2:
                        aria2_rpc(rpc_port, rpc_secret, "shutdown")
                        break
                except (OSError, URLError, RuntimeError):
                    pass
                time.sleep(1)
    except KeyboardInterrupt:
        with contextlib.suppress(Exception):
            aria2_rpc(rpc_port, rpc_secret, "shutdown")
        process.terminate()
        process.wait(timeout=10)
        raise

    exit_code = process.wait(timeout=15)

    for item in pending_items:
        if item.output_path.exists():
            if item.remote_size is None or item.output_path.stat().st_size == item.remote_size:
                stats.completed += 1
                stats.downloaded_bytes += max(0, item.output_path.stat().st_size - initial_sizes[item.output_path])
                continue
        reason = f"aria2c exit {exit_code}"
        if item.temp_path.exists():
            reason += " (python .part present)"
        item.failed_reason = reason
        stats.failed.append(f"{item.display_name}: {reason}")

    if not stats.failed and exit_code == 0:
        with contextlib.suppress(OSError):
            manifest_path.unlink()
        with contextlib.suppress(OSError):
            log_path.unlink()
    elif log_path.exists():
        console.print(f"[yellow]aria2 diagnostic log:[/yellow] {log_path}")
        console.print(f"[yellow]aria2 task manifest:[/yellow] {manifest_path}")

    return stats


async def run_downloads(
    items: list[LinkItem],
    start_concurrency: int,
    max_concurrency: int,
    chunk_size: int,
    retries: int,
    total_bytes: int,
    target_mbps: float,
    sample_url: str,
    route_state: RouteRuntimeState,
) -> RunStats:
    pending_items = [item for item in items if not item.skip_reason]
    skipped_items = [item for item in items if item.skip_reason]
    stats = RunStats(skipped=len(skipped_items))

    queue: asyncio.Queue[LinkItem | None] = asyncio.Queue()
    for item in pending_items:
        await queue.put(item)

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=120)
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    workers: list[asyncio.Task[None]] = []
    stop_controller = asyncio.Event()

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=28),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    )

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=False) as session:
        with progress:
            total_task_id = progress.add_task("TOTAL", total=total_bytes if total_bytes > 0 else None)
            desired_workers = start_concurrency
            live_workers = 0
            reduce_budget = 0
            switch_budget = 0
            worker_lock = asyncio.Lock()

            def current_proxy() -> str | None:
                return route_state.active_route.proxy

            def consume_pause_budget() -> str | None:
                nonlocal live_workers, reduce_budget, switch_budget
                if switch_budget > 0:
                    switch_budget -= 1
                    return "switch"
                if reduce_budget > 0 and live_workers > desired_workers:
                    reduce_budget -= 1
                    live_workers -= 1
                    return "reduce"
                return None

            async def worker(label: str) -> None:
                nonlocal live_workers
                while True:
                    async with worker_lock:
                        if stop_controller.is_set() and queue.empty():
                            live_workers -= 1
                            return
                        if live_workers > desired_workers:
                            live_workers -= 1
                            return
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    try:
                        await download_one(
                            session,
                            item,
                            current_proxy,
                            progress,
                            total_task_id,
                            stats,
                            chunk_size,
                            retries,
                            should_pause=consume_pause_budget,
                        )
                    except PauseDownload as pause_exc:
                        await queue.put(item)
                        if pause_exc.reason == "reduce":
                            return
                        continue
                    except Exception as exc:
                        message = f"{item.display_name}: {exc}"
                        stats.failed.append(message)
                    finally:
                        queue.task_done()

            def spawn_workers(count: int) -> None:
                nonlocal live_workers
                start_index = len(workers)
                for index in range(count):
                    worker_name = f"worker-{start_index + index + 1}"
                    live_workers += 1
                    workers.append(asyncio.create_task(worker(worker_name)))

            async def adaptive_controller() -> None:
                nonlocal desired_workers, reduce_budget
                last_time = time.monotonic()
                last_bytes = 0
                last_speed = None
                best_speed = 0.0
                best_workers = desired_workers
                min_workers = max(1, start_concurrency)
                last_adjust_at = time.monotonic()
                settle_until = last_adjust_at + DEFAULT_SETTLE_SECONDS
                grow_streak = 0
                reduce_streak = 0
                while not stop_controller.is_set():
                    await asyncio.sleep(DEFAULT_CONTROLLER_SAMPLE_SECONDS)
                    if queue.empty():
                        continue
                    now = time.monotonic()
                    elapsed = now - last_time
                    current_bytes = stats.downloaded_bytes
                    delta_bytes = current_bytes - last_bytes
                    speed = delta_bytes / elapsed if elapsed > 0 else 0.0
                    speed_mbps = speed / 1024 / 1024
                    current_workers = live_workers

                     # Ignore transient samples right after changing concurrency.
                    if now < settle_until:
                        last_time = now
                        last_bytes = current_bytes
                        last_speed = speed
                        continue

                    if speed > best_speed * 1.03:
                        best_speed = speed
                        best_workers = current_workers

                    cooldown_ready = (now - last_adjust_at) >= DEFAULT_ADJUST_COOLDOWN_SECONDS
                    new_desired = desired_workers
                    if last_speed is not None and current_workers > 1:
                        grew_vs_last = speed >= last_speed * 1.08 and speed_mbps >= DEFAULT_MIN_SPEED_FOR_GROWTH_MBPS
                        dropped_vs_last = speed <= last_speed * 0.82
                        under_best = best_speed > 0 and speed < best_speed * 0.78 and current_workers > best_workers

                        grow_streak = grow_streak + 1 if grew_vs_last else 0
                        reduce_streak = reduce_streak + 1 if (dropped_vs_last or under_best) else 0
                    else:
                        grow_streak = 0
                        reduce_streak = 0

                    if cooldown_ready and reduce_streak >= 2 and current_workers > min_workers:
                        new_desired = max(min_workers, current_workers - 1)
                    elif cooldown_ready and grow_streak >= 2 and current_workers < max_concurrency:
                        new_desired = min(max_concurrency, current_workers + 1)

                    if new_desired > desired_workers:
                        desired_workers = new_desired
                        spawn_workers(new_desired - current_workers)
                        last_adjust_at = now
                        settle_until = now + DEFAULT_SETTLE_SECONDS
                        grow_streak = 0
                        reduce_streak = 0
                        console.print(
                            f"[cyan]Adaptive concurrency -> {desired_workers} ({speed_mbps:.1f} MB/s)[/cyan]"
                        )
                    elif new_desired < desired_workers:
                        reduce_budget += current_workers - new_desired
                        desired_workers = new_desired
                        last_adjust_at = now
                        settle_until = now + DEFAULT_SETTLE_SECONDS
                        grow_streak = 0
                        reduce_streak = 0
                        console.print(
                            f"[magenta]Adaptive concurrency -> {desired_workers} ({speed_mbps:.1f} MB/s, reduced)[/magenta]"
                        )

                    last_time = now
                    last_bytes = current_bytes
                    last_speed = speed

            async def route_monitor() -> None:
                nonlocal switch_budget
                if route_state.recheck_seconds <= 0:
                    return
                last_route_switch_at = 0.0
                while not stop_controller.is_set():
                    await asyncio.sleep(route_state.recheck_seconds)
                    if queue.empty():
                        continue
                    try:
                        choices = await benchmark_current_routes(sample_url, route_state)
                    except Exception as exc:
                        console.print(f"[yellow]Route recheck skipped:[/yellow] {exc}")
                        continue

                    best_choice = max(choices, key=lambda item: item.throughput_mbps)
                    active_choice = next(
                        (
                            item
                            for item in choices
                            if item.node_name == route_state.active_route.node_name and item.proxy == route_state.active_route.proxy
                        ),
                        None,
                    )
                    active_speed = active_choice.throughput_mbps if active_choice else route_state.active_route.throughput_mbps
                    route_state.active_route.throughput_mbps = active_speed

                    if active_speed <= 0:
                        should_switch = best_choice.throughput_mbps > 0
                    else:
                        should_switch = best_choice.throughput_mbps >= active_speed * route_state.switch_gain

                    changed_route = (
                        best_choice.proxy != route_state.active_route.proxy
                        or best_choice.node_name != route_state.active_route.node_name
                    )
                    switch_cooldown_ready = (time.monotonic() - last_route_switch_at) >= max(
                        route_state.recheck_seconds * 1.5, 15 * 60
                    )

                    if changed_route and should_switch and switch_cooldown_ready:
                        if (
                            best_choice.node_name
                            and best_choice.node_name != "DIRECT"
                            and route_state.controller_url
                            and route_state.group_name
                        ):
                            await set_proxy_group_node(
                                route_state.controller_url,
                                route_state.secret,
                                route_state.group_name,
                                best_choice.node_name,
                            )
                        route_state.active_route = RouteChoice(
                            label=best_choice.label,
                            proxy=best_choice.proxy,
                            node_name=best_choice.node_name,
                            latency_ms=best_choice.latency_ms,
                            throughput_mbps=best_choice.throughput_mbps,
                        )
                        if best_choice.node_name and best_choice.node_name != "DIRECT":
                            route_state.proxy_node_name = best_choice.node_name
                        switch_budget += max(1, live_workers)
                        last_route_switch_at = time.monotonic()
                        console.print(
                            f"[green]Route switch -> {best_choice.label} ({best_choice.throughput_mbps:.1f} MB/s vs {active_speed:.1f} MB/s)[/green]"
                        )
                    else:
                        summary = ", ".join(f"{choice.label}={choice.throughput_mbps:.1f} MB/s" for choice in choices)
                        console.print(f"[blue]Route recheck kept current path:[/blue] {summary}")

            spawn_workers(start_concurrency)
            controller_task = asyncio.create_task(adaptive_controller())
            route_task = asyncio.create_task(route_monitor())

            await queue.join()
            stop_controller.set()
            controller_task.cancel()
            route_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await controller_task
            with contextlib.suppress(asyncio.CancelledError):
                await route_task
            await asyncio.gather(*workers, return_exceptions=True)

    return stats


def write_failure_log(output_folder: Path, failures: list[str]) -> Path | None:
    path = output_folder / "download_failures.txt"
    if not failures:
        path.unlink(missing_ok=True)
        return None
    path.write_text("\n".join(failures) + "\n", encoding="utf-8")
    return path


def print_summary(
    input_folder: Path,
    output_folder: Path,
    items: list[LinkItem],
    proxy: str | None,
    controller_url: str | None,
    current_node: str | None,
    selected_node: str | None,
    total_bytes: int,
    unknown_count: int,
) -> None:
    txt_files = sorted(path.name for path in input_folder.glob("*.txt"))
    console.print(f"[bold]Input folder:[/bold] {input_folder}")
    console.print(f"[bold]Output folder:[/bold] {output_folder}")
    console.print(f"[bold]Txt files:[/bold] {', '.join(txt_files)}")
    console.print(f"[bold]URLs found:[/bold] {len(items)}")
    console.print(f"[bold]Proxy:[/bold] {proxy or 'direct'}")
    console.print(f"[bold]Controller:[/bold] {controller_url or 'not available'}")
    if current_node:
        console.print(f"[bold]Current Clash node:[/bold] {current_node}")
    if selected_node and selected_node != current_node:
        console.print(f"[bold]Auto-selected node:[/bold] {selected_node}")
    if total_bytes > 0:
        console.print(f"[bold]Known remaining size:[/bold] {total_bytes / 1024 / 1024 / 1024:.2f} GiB")
    if unknown_count:
        console.print(f"[bold]Unknown-size files:[/bold] {unknown_count}")


async def async_main() -> int:
    args = parse_args()
    if args.folder is not None:
        input_folder = args.folder.resolve()
        output_folder = args.folder.resolve()
    else:
        input_folder = args.input_folder.resolve()
        output_folder = args.output_folder.resolve()

    if not input_folder.exists():
        console.print(f"[red]Input folder does not exist:[/red] {input_folder}")
        return 1

    output_folder.mkdir(parents=True, exist_ok=True)

    derived_max_concurrency = derive_max_concurrency(args.target_mbps, args.max_concurrency)
    chunk_size = max(1, args.chunk_size_mb) * 1024 * 1024
    start_concurrency = clamp(args.start_concurrency, 1, derived_max_concurrency)
    preferred_tokens = get_preferred_tokens(args.prefer)
    aria2c_path = find_aria2c(args.aria2c_path)

    backend = args.backend
    if backend == "auto":
        backend = "aria2c" if aria2c_path else "python"
    if backend == "aria2c" and not aria2c_path:
        console.print("[yellow]aria2c was requested but not found on PATH. Falling back to the Python downloader.[/yellow]")
        backend = "python"
    if backend == "arcgis":
        if not aria2c_path:
            console.print("[red]The ArcGIS backend requires aria2c, but aria2c was not found.[/red]")
            return 1
        try:
            import rasterio  # noqa: F401
        except ImportError:
            console.print("[red]The ArcGIS backend requires rasterio/GDAL from the GEO conda environment.[/red]")
            console.print(
                'Run: conda run -n GEO python -u "C:\\Users\\MSi\\Documents\\AI workflow\\download_links.py" '
                "--backend arcgis"
            )
            return 1

    console.print("[bold]Starting downloader...[/bold]")
    console.print(f"Input: {input_folder}")
    console.print(f"Output: {output_folder}")
    console.print(f"Backend: {backend}")

    clash = detect_clash(DEFAULT_CONFIG_DIR)
    current_selection = detect_current_clash_node(DEFAULT_CONFIG_DIR)
    current_node = current_selection.get("✈️ 手动选择") or current_selection.get("节点选择")

    proxy = None
    if args.proxy.lower() == "auto":
        proxy = clash["proxy_url"]
    elif args.proxy.lower() not in {"direct", "off", "none"}:
        proxy = args.proxy

    controller_url = None
    if args.controller.lower() == "auto":
        controller_url = clash["controller_url"]
    elif args.controller.lower() not in {"off", "none"}:
        controller_url = args.controller

    secret = args.secret if args.secret is not None else clash["secret"]

    items = read_urls(input_folder, output_folder, args.limit)
    if not items:
        console.print("[yellow]No valid URLs were found in the txt files.[/yellow]")
        return 0

    route_sample_url = items[0].url
    if backend == "arcgis":
        from arcgis_backend import create_benchmark_url

        console.print("Preparing an ArcGIS range-enabled sample for route benchmarking...")
        try:
            route_sample_url = await create_benchmark_url(proxy)
        except Exception:
            if proxy is None:
                raise
            console.print("[yellow]Proxy sample export failed; retrying the ArcGIS export directly.[/yellow]")
            route_sample_url = await create_benchmark_url(None)

    selected_node = current_node
    route_results: list[RouteChoice] = []
    route_group_name = None
    selected_proxy_node = current_node
    active_route = RouteChoice(label="direct", proxy=proxy, node_name=selected_node)
    if args.proxy.lower() == "auto":
        try:
            chosen_route, route_results, route_group_name, selected_proxy_node = await auto_select_download_route(
                sample_url=route_sample_url,
                proxy_url=clash["proxy_url"],
                controller_url=controller_url,
                secret=secret,
                preferred_tokens=preferred_tokens,
                benchmark_bytes_mb=args.benchmark_bytes_mb,
                benchmark_candidates=args.benchmark_candidates,
            )
            proxy = chosen_route.proxy
            selected_node = chosen_route.node_name or current_node
            active_route = RouteChoice(
                label=chosen_route.label,
                proxy=chosen_route.proxy,
                node_name=chosen_route.node_name,
                latency_ms=chosen_route.latency_ms,
                throughput_mbps=chosen_route.throughput_mbps,
            )
            console.print(
                f'Auto-selected path: {chosen_route.label} at {chosen_route.throughput_mbps:.1f} MB/s'
            )
        except Exception as exc:
            console.print(f"[yellow]Automatic route benchmark skipped:[/yellow] {format_exception(exc)}")
            active_route = RouteChoice(
                label="proxy:fallback" if proxy else "direct",
                proxy=proxy,
                node_name=selected_node if proxy else "DIRECT",
            )
            if controller_url:
                console.print("Falling back to the current Clash node and proxy settings.")
            else:
                console.print("Clash controller not available. Continuing with the detected proxy settings.")
    elif controller_url and proxy:
        console.print("Using the explicitly requested proxy mode; startup route benchmark was skipped.")
        active_route = RouteChoice(label="proxy:explicit", proxy=proxy, node_name=selected_node)
    else:
        console.print("Using the explicitly requested connection mode; startup route benchmark was skipped.")
        active_route = RouteChoice(label="direct", proxy=None, node_name="DIRECT")

    if route_results:
        console.print("Route benchmark results:")
        for route in route_results:
            latency = f", delay={route.latency_ms} ms" if route.latency_ms is not None else ""
            console.print(f"  {route.label}: {route.throughput_mbps:.1f} MB/s{latency}")

    if backend == "arcgis":
        total_bytes, unknown_count = 0, 0
        console.print("ArcGIS output sizes are determined as each range-enabled tile is exported.")
    else:
        console.print("Probing download URLs and resume support...")
        total_bytes, unknown_count = await inspect_items(items, proxy, args.metadata_concurrency)
    print_summary(input_folder, output_folder, items, proxy, controller_url, current_node, selected_node, total_bytes, unknown_count)
    console.print(summarize_route_measurement(active_route))
    if backend == "python":
        console.print("Concurrency now adapts in both directions during the run; route checks can also switch between direct and proxy mid-download.")
        console.print(
            f"Concurrency plan: start={start_concurrency}, max={derived_max_concurrency}, target={args.target_mbps:.1f} MB/s"
        )
        console.print(
            f"Route recheck plan: every {args.route_recheck_minutes:.1f} min, switch gain threshold {args.route_switch_gain:.2f}x"
        )
    elif backend == "aria2c":
        aria2_pending = [item for item in items if not item.skip_reason]
        aria2_effective_split = (
            max(1, args.aria2_split)
            if aria2_pending and all(item.accept_ranges for item in aria2_pending)
            else 1
        )
        console.print(
            f"aria2c plan: concurrent files={max(1, args.aria2_concurrent_files)}, "
            f"split per file={aria2_effective_split}, "
            f"max connections={max(1, args.aria2_concurrent_files) * aria2_effective_split}, "
            f"min split size={args.aria2_min_split_size}"
        )
        console.print("Mid-download route switching is disabled for aria2c runs; the selected path stays fixed for this launch.")
    else:
        tile_size = clamp(args.arcgis_tile_size, 256, 4096)
        console.print(
            f"ArcGIS plan: tile={tile_size}x{tile_size}, concurrent tiles={max(1, args.arcgis_concurrent_tiles)}, "
            f"split per tile={clamp(args.arcgis_split, 1, 16)}, "
            f"max connections={max(1, args.arcgis_concurrent_tiles) * clamp(args.arcgis_split, 1, 16)}"
        )
        console.print("Each completed raster is merged and validated locally before its temporary tiles are removed.")

    if args.dry_run:
        return 0

    console.print("Starting file downloads...")
    if backend == "arcgis" and aria2c_path:
        from arcgis_backend import run_arcgis_downloads

        stats = await run_arcgis_downloads(
            items=items,
            output_folder=output_folder,
            proxy=proxy,
            aria2c_path=aria2c_path,
            download_batch=run_aria2c_downloads,
            item_factory=LinkItem,
            tile_size=clamp(args.arcgis_tile_size, 256, 4096),
            concurrent_tiles=max(1, args.arcgis_concurrent_tiles),
            split=clamp(args.arcgis_split, 1, 16),
            retries=args.retries,
            keep_tiles=args.arcgis_keep_tiles,
            console=console,
        )
    elif backend == "aria2c" and aria2c_path:
        stats = run_aria2c_downloads(
            items=items,
            output_folder=output_folder,
            proxy=proxy,
            concurrent_files=args.aria2_concurrent_files,
            retries=args.retries,
            aria2c_path=aria2c_path,
            split=args.aria2_split,
            min_split_size=args.aria2_min_split_size,
        )
    else:
        route_state = RouteRuntimeState(
            active_route=active_route,
            proxy_url=clash["proxy_url"],
            controller_url=controller_url,
            secret=secret,
            group_name=route_group_name,
            proxy_node_name=selected_proxy_node,
            benchmark_bytes_mb=args.benchmark_bytes_mb,
            recheck_seconds=max(0.0, args.route_recheck_minutes * 60.0),
            switch_gain=max(1.01, args.route_switch_gain),
        )
        stats = await run_downloads(
            items=items,
            start_concurrency=start_concurrency,
            max_concurrency=derived_max_concurrency,
            chunk_size=chunk_size,
            retries=args.retries,
            total_bytes=total_bytes,
            target_mbps=args.target_mbps,
            sample_url=items[0].url,
            route_state=route_state,
        )

    failure_log = write_failure_log(output_folder, stats.failed)
    console.print("")
    console.print(
        f"[bold green]Finished.[/bold green] completed={stats.completed}, skipped={stats.skipped}, failed={len(stats.failed)}"
    )
    console.print(f"[bold]Downloaded:[/bold] {stats.downloaded_bytes / 1024 / 1024 / 1024:.2f} GiB")
    if failure_log:
        console.print(f"[bold yellow]Failure log:[/bold yellow] {failure_log}")
        return 2
    return 0


def main() -> int:
    try:
        return asyncio.run(async_main())
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user.[/yellow]")
        return 130


if __name__ == "__main__":
    sys.exit(main())

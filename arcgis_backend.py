from __future__ import annotations

import asyncio
import contextlib
import json
import math
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import aiohttp
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn


TOTAL_SERVICE = (
    "https://worldpop.arcgis.com/arcgis/rest/services/"
    "WorldPop_Total_Population_1km/ImageServer"
)
COHORT_SERVICE = (
    "https://worldpop.arcgis.com/arcgis/rest/services/"
    "WorldPop_Population_Cohorts_1km/ImageServer"
)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) WorldPop ArcGIS Downloader/1.0"


@dataclass(slots=True)
class ArcGISStats:
    downloaded_bytes: int = 0
    completed: int = 0
    skipped: int = 0
    failed: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ServiceMeta:
    url: str
    xmin: float
    ymin: float
    xmax: float
    ymax: float
    pixel_x: float
    pixel_y: float
    width: int
    height: int
    nodata: float | None


@dataclass(slots=True)
class ArcGISJob:
    source_item: Any
    service_url: str
    variable: str
    year: int
    timestamp_ms: int
    meta: ServiceMeta


@dataclass(slots=True)
class TileSpec:
    row: int
    col: int
    width: int
    height: int
    bbox: tuple[float, float, float, float]
    path: Path
    url: str | None = None
    remote_size: int | None = None


def _timestamp_ms(year: int) -> int:
    return int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


def _cohort_variable(sex: str, age: int) -> str:
    prefix = "Males" if sex.lower() == "m" else "Females"
    if age == 80:
        return f"{prefix} over 80 years of age"
    return f"{prefix} age {age:.1f} to {age + 4.99:.2f} years"


def parse_job(item: Any, services: dict[str, ServiceMeta]) -> ArcGISJob:
    name = item.output_path.name
    total_match = re.fullmatch(r"ppp_(\d{4})_1km_Aggregated\.tif", name, flags=re.IGNORECASE)
    if total_match:
        year = int(total_match.group(1))
        return ArcGISJob(
            source_item=item,
            service_url=TOTAL_SERVICE,
            variable="Total Population",
            year=year,
            timestamp_ms=_timestamp_ms(year),
            meta=services[TOTAL_SERVICE],
        )

    cohort_match = re.fullmatch(
        r"global_([mf])_(60|65|70|75|80)_(\d{4})_1km\.tif",
        name,
        flags=re.IGNORECASE,
    )
    if cohort_match:
        sex, age_text, year_text = cohort_match.groups()
        year = int(year_text)
        return ArcGISJob(
            source_item=item,
            service_url=COHORT_SERVICE,
            variable=_cohort_variable(sex, int(age_text)),
            year=year,
            timestamp_ms=_timestamp_ms(year),
            meta=services[COHORT_SERVICE],
        )

    raise ValueError(f"Unsupported WorldPop ArcGIS filename: {name}")


async def _json_request(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    proxy: str | None,
    **kwargs: Any,
) -> dict[str, Any]:
    async with session.request(method, url, proxy=proxy, **kwargs) as response:
        response.raise_for_status()
        data = await response.json(content_type=None)
    if not isinstance(data, dict):
        raise RuntimeError(f"ArcGIS returned a non-object response from {url}")
    if "error" in data:
        error = data["error"]
        raise RuntimeError(f"ArcGIS error: {error.get('message', error)}")
    return data


async def load_service_meta(proxy: str | None) -> dict[str, ServiceMeta]:
    timeout = aiohttp.ClientTimeout(total=45, sock_connect=15, sock_read=30)
    headers = {"User-Agent": USER_AGENT}
    result: dict[str, ServiceMeta] = {}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, trust_env=False) as session:
        for service_url in (TOTAL_SERVICE, COHORT_SERVICE):
            data = await _json_request(session, "GET", service_url, proxy, params={"f": "json"})
            extent = data["fullExtent"]
            pixel_x = abs(float(data["pixelSizeX"]))
            pixel_y = abs(float(data["pixelSizeY"]))
            xmin = float(extent["xmin"])
            ymin = float(extent["ymin"])
            xmax = float(extent["xmax"])
            ymax = float(extent["ymax"])
            result[service_url] = ServiceMeta(
                url=service_url,
                xmin=xmin,
                ymin=ymin,
                xmax=xmax,
                ymax=ymax,
                pixel_x=pixel_x,
                pixel_y=pixel_y,
                width=round((xmax - xmin) / pixel_x),
                height=round((ymax - ymin) / pixel_y),
                nodata=float(data["noDataValue"]) if data.get("noDataValue") is not None else None,
            )
    return result


def _mosaic_rule(variable: str, timestamp_ms: int) -> str:
    return json.dumps(
        {
            "multidimensionalDefinition": [
                {
                    "variableName": variable,
                    "dimensionName": "StdTime",
                    "values": [timestamp_ms],
                    "isSlice": True,
                }
            ]
        },
        separators=(",", ":"),
    )


async def _export_tile(
    session: aiohttp.ClientSession,
    service_url: str,
    variable: str,
    timestamp_ms: int,
    bbox: tuple[float, float, float, float],
    width: int,
    height: int,
    proxy: str | None,
) -> tuple[str, int | None]:
    form = {
        "f": "json",
        "bbox": ",".join(f"{value:.14f}" for value in bbox),
        "bboxSR": "4326",
        "imageSR": "4326",
        "size": f"{width},{height}",
        "format": "tiff",
        "pixelType": "F32",
        "interpolation": "RSP_NearestNeighbor",
        "compression": "LZW",
        "mosaicRule": _mosaic_rule(variable, timestamp_ms),
    }
    data = await _json_request(
        session,
        "POST",
        f"{service_url}/exportImage",
        proxy,
        data=form,
    )
    href = data.get("href")
    if not isinstance(href, str) or not href.startswith("http"):
        raise RuntimeError(f"ArcGIS export did not return a download URL: {data}")

    remote_size = None
    async with session.head(href, proxy=proxy, allow_redirects=True) as response:
        response.raise_for_status()
        length = response.headers.get("Content-Length")
        if length and length.isdigit():
            remote_size = int(length)
    return href, remote_size


async def create_benchmark_url(proxy: str | None) -> str:
    timeout = aiohttp.ClientTimeout(total=90, sock_connect=20, sock_read=60)
    headers = {"User-Agent": USER_AGENT}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, trust_env=False) as session:
        href, _ = await _export_tile(
            session=session,
            service_url=TOTAL_SERVICE,
            variable="Total Population",
            timestamp_ms=_timestamp_ms(2000),
            bbox=(0.0, 0.0, 34.1333331968, 34.1333331968),
            width=4096,
            height=4096,
            proxy=proxy,
        )
    return href


def _tile_specs(job: ArcGISJob, tile_size: int, tile_dir: Path) -> list[TileSpec]:
    specs: list[TileSpec] = []
    for row in range(0, job.meta.height, tile_size):
        height = min(tile_size, job.meta.height - row)
        ymax = job.meta.ymax - row * job.meta.pixel_y
        ymin = ymax - height * job.meta.pixel_y
        for col in range(0, job.meta.width, tile_size):
            width = min(tile_size, job.meta.width - col)
            xmin = job.meta.xmin + col * job.meta.pixel_x
            xmax = xmin + width * job.meta.pixel_x
            specs.append(
                TileSpec(
                    row=row,
                    col=col,
                    width=width,
                    height=height,
                    bbox=(xmin, ymin, xmax, ymax),
                    path=tile_dir / f"r{row:05d}_c{col:05d}.tif",
                )
            )
    return specs


def _valid_raster(path: Path, width: int, height: int) -> bool:
    if not path.exists():
        return False
    try:
        import rasterio
        from rasterio.windows import Window

        with rasterio.open(path) as dataset:
            if dataset.width != width or dataset.height != height or dataset.count != 1:
                return False
            dataset.read(1, window=Window(width - 1, height - 1, 1, 1))
        return True
    except Exception:
        return False


async def _prepare_pending_tiles(
    job: ArcGISJob,
    specs: list[TileSpec],
    proxy: str | None,
    export_concurrency: int,
    console: Console,
) -> list[TileSpec]:
    pending = [spec for spec in specs if not _valid_raster(spec.path, spec.width, spec.height)]
    if not pending:
        return []

    state_path = specs[0].path.parent / "_exports.json"
    try:
        state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    except (OSError, json.JSONDecodeError):
        state = {}
    if not isinstance(state, dict):
        state = {}

    timeout = aiohttp.ClientTimeout(total=120, sock_connect=20, sock_read=90)
    headers = {"User-Agent": USER_AGENT}
    semaphore = asyncio.Semaphore(max(1, export_concurrency))
    state_lock = asyncio.Lock()
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=28),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    )

    async with aiohttp.ClientSession(timeout=timeout, headers=headers, trust_env=False) as session:
        async def prepare(spec: TileSpec) -> None:
            async with semaphore:
                saved = state.get(spec.path.name, {})
                saved_url = saved.get("url") if isinstance(saved, dict) else None
                saved_size = saved.get("size") if isinstance(saved, dict) else None
                if isinstance(saved_url, str):
                    try:
                        async with session.head(saved_url, proxy=proxy, allow_redirects=True) as response:
                            response.raise_for_status()
                            length = response.headers.get("Content-Length")
                            current_size = int(length) if length and length.isdigit() else None
                        if saved_size is None or current_size == saved_size:
                            spec.url = saved_url
                            spec.remote_size = current_size
                    except Exception:
                        pass

                if spec.url is None:
                    control_path = spec.path.with_name(spec.path.name + ".aria2")
                    if spec.path.exists() and not control_path.exists():
                        spec.path.unlink()
                    elif control_path.exists():
                        # A new ArcGIS export is not guaranteed to be byte-identical to the expired one.
                        spec.path.unlink(missing_ok=True)
                        control_path.unlink(missing_ok=True)

                    spec.url, spec.remote_size = await _export_tile(
                        session=session,
                        service_url=job.service_url,
                        variable=job.variable,
                        timestamp_ms=job.timestamp_ms,
                        bbox=spec.bbox,
                        width=spec.width,
                        height=spec.height,
                        proxy=proxy,
                    )

                async with state_lock:
                    state[spec.path.name] = {"url": spec.url, "size": spec.remote_size}
                    temporary_state = state_path.with_name(state_path.name + ".tmp")
                    temporary_state.write_text(json.dumps(state, indent=2), encoding="utf-8")
                    temporary_state.replace(state_path)
                progress.advance(task_id)

        with progress:
            task_id = progress.add_task(f"Preparing {job.source_item.display_name} tiles", total=len(pending))
            results = await asyncio.gather(*(prepare(spec) for spec in pending), return_exceptions=True)

    errors = [result for result in results if isinstance(result, Exception)]
    if errors:
        raise RuntimeError(f"{len(errors)} ArcGIS tile exports failed; first error: {errors[0]}")
    return pending


def _merge_tiles(job: ArcGISJob, specs: list[TileSpec], building_path: Path) -> None:
    import rasterio
    from rasterio.transform import from_origin
    from rasterio.windows import Window

    for spec in specs:
        if not _valid_raster(spec.path, spec.width, spec.height):
            raise RuntimeError(f"Tile is missing or invalid: {spec.path}")

    building_path.unlink(missing_ok=True)
    with rasterio.open(specs[0].path) as sample:
        dtype = sample.dtypes[0]
        nodata = job.meta.nodata if job.meta.nodata is not None else sample.nodata

    profile = {
        "driver": "GTiff",
        "width": job.meta.width,
        "height": job.meta.height,
        "count": 1,
        "dtype": dtype,
        "crs": "EPSG:4326",
        "transform": from_origin(job.meta.xmin, job.meta.ymax, job.meta.pixel_x, job.meta.pixel_y),
        "nodata": nodata,
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
        "compress": "DEFLATE",
        "predictor": 3,
        "zlevel": 6,
        "BIGTIFF": "YES",
        "NUM_THREADS": "ALL_CPUS",
    }

    with rasterio.Env(GDAL_NUM_THREADS="ALL_CPUS"):
        with rasterio.open(building_path, "w", **profile) as destination:
            for spec in specs:
                with rasterio.open(spec.path) as source:
                    data = source.read(1)
                destination.write(data, 1, window=Window(spec.col, spec.row, spec.width, spec.height))
            destination.update_tags(
                SOURCE="WorldPop ArcGIS ImageServer",
                VARIABLE=job.variable,
                YEAR=str(job.year),
            )

    if not _valid_raster(building_path, job.meta.width, job.meta.height):
        raise RuntimeError(f"Merged raster validation failed: {building_path}")


async def run_arcgis_downloads(
    *,
    items: list[Any],
    output_folder: Path,
    proxy: str | None,
    aria2c_path: str,
    download_batch: Callable[..., Any],
    item_factory: Callable[..., Any],
    tile_size: int,
    concurrent_tiles: int,
    split: int,
    retries: int,
    keep_tiles: bool,
    console: Console,
) -> ArcGISStats:
    stats = ArcGISStats()
    services = await load_service_meta(proxy)
    jobs = [parse_job(item, services) for item in items]
    tile_root = output_folder / "_arcgis_tiles"
    tile_root.mkdir(parents=True, exist_ok=True)

    console.print(
        f"ArcGIS grid: total={services[TOTAL_SERVICE].width}x{services[TOTAL_SERVICE].height}, "
        f"cohorts={services[COHORT_SERVICE].width}x{services[COHORT_SERVICE].height}"
    )

    for index, job in enumerate(jobs, start=1):
        output_path = job.source_item.output_path
        if _valid_raster(output_path, job.meta.width, job.meta.height):
            stats.skipped += 1
            console.print(f"[{index}/{len(jobs)}] Already complete: {output_path.name}")
            continue

        console.print(
            f"[bold][{index}/{len(jobs)}] {output_path.name}[/bold] "
            f"({job.variable}, {job.year})"
        )
        tile_dir = tile_root / output_path.stem
        tile_dir.mkdir(parents=True, exist_ok=True)
        specs = _tile_specs(job, tile_size, tile_dir)

        try:
            last_batch_error = None
            for batch_attempt in range(max(1, retries)):
                pending = await _prepare_pending_tiles(
                    job=job,
                    specs=specs,
                    proxy=proxy,
                    export_concurrency=concurrent_tiles,
                    console=console,
                )
                if not pending:
                    last_batch_error = None
                    break

                retry_split = max(1, split // (2**batch_attempt))
                if batch_attempt:
                    console.print(
                        f"[yellow]Retrying {len(pending)} incomplete tile(s), "
                        f"attempt {batch_attempt + 1}/{max(1, retries)}, split={retry_split}[/yellow]"
                    )
                tile_items = [
                    item_factory(
                        url=spec.url,
                        source_file=job.source_item.source_file,
                        output_path=spec.path,
                        temp_path=spec.path.with_name(spec.path.name + ".part"),
                        remote_size=spec.remote_size,
                        accept_ranges=True,
                    )
                    for spec in pending
                ]
                batch_stats = download_batch(
                    items=tile_items,
                    output_folder=tile_dir,
                    proxy=proxy,
                    concurrent_files=min(concurrent_tiles, len(tile_items)),
                    retries=retries,
                    aria2c_path=aria2c_path,
                    split=retry_split,
                    min_split_size="1M",
                )
                stats.downloaded_bytes += batch_stats.downloaded_bytes
                last_batch_error = batch_stats.failed[0] if batch_stats.failed else None

            remaining = [spec for spec in specs if not _valid_raster(spec.path, spec.width, spec.height)]
            if remaining:
                detail = last_batch_error or f"{len(remaining)} tiles remain incomplete"
                raise RuntimeError(detail)

            building_path = output_path.with_name(output_path.name + ".building.tif")
            console.print(f"Merging {len(specs)} tiles -> {output_path.name}")
            await asyncio.to_thread(_merge_tiles, job, specs, building_path)
            building_path.replace(output_path)
            stats.completed += 1

            if not keep_tiles:
                resolved_tile_dir = tile_dir.resolve()
                if resolved_tile_dir.parent == tile_root.resolve():
                    shutil.rmtree(resolved_tile_dir)
        except Exception as exc:
            stats.failed.append(f"{output_path.name}: {type(exc).__name__}: {exc}")
            console.print(f"[red]ArcGIS job failed:[/red] {stats.failed[-1]}")
            break

    if tile_root.exists() and not any(tile_root.iterdir()):
        with contextlib.suppress(OSError):
            tile_root.rmdir()
    return stats

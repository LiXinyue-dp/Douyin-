from __future__ import annotations

import asyncio
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote

import aiofiles
import httpx
import yaml
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from crawlers.douyin.web import web_crawler as douyin_web_module
from crawlers.hybrid.hybrid_crawler import HybridCrawler

try:
    from playwright.async_api import BrowserContext, Page, async_playwright
except ImportError:  # pragma: no cover - handled at runtime for clearer API errors.
    async_playwright = None
    BrowserContext = Any
    Page = Any


router = APIRouter()

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = PROJECT_ROOT / "config.yaml"
BROWSER_DATA_DIR = PROJECT_ROOT / ".batch_browser_profile"
VIDEO_URL_RE = re.compile(
    r"https?://(?:www\.)?douyin\.com/(?:video|note|slides)/(\d{10,25})"
)
MODAL_ID_RE = re.compile(r"[?&]modal_id=(\d{10,25})")
AWEME_ID_RE = re.compile(r"\b\d{18,25}\b")

HybridCrawlerInstance = HybridCrawler()

_playwright = None
_browser_context: BrowserContext | None = None
_browser_lock = asyncio.Lock()
_jobs: dict[str, "BatchJob"] = {}


class LoginRequest(BaseModel):
    keyword: str = Field(default="vlog", description="打开登录窗口后默认进入的搜索关键词")


class BatchStartRequest(BaseModel):
    keyword: str = Field(default="vlog", min_length=1)
    max_items: int = Field(default=30, ge=1, le=20000)
    scrolls: int = Field(default=20, ge=1, le=5000)
    delay_ms: int = Field(default=1200, ge=200, le=10000)
    download_concurrency: int = Field(default=4, ge=1, le=20)
    with_watermark: bool = False
    download: bool = True


@dataclass
class BatchJob:
    id: str
    keyword: str
    max_items: int
    scrolls: int
    delay_ms: int
    download_concurrency: int
    with_watermark: bool
    download: bool
    status: str = "queued"
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    found_urls: list[str] = field(default_factory=list)
    downloaded_files: list[str] = field(default_factory=list)
    failed: list[dict[str, str]] = field(default_factory=list)
    logs: list[str] = field(default_factory=list)

    def log(self, message: str) -> None:
        self.updated_at = time.time()
        self.logs.append(f"{time.strftime('%H:%M:%S')} {message}")
        self.logs = self.logs[-300:]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "keyword": self.keyword,
            "max_items": self.max_items,
            "scrolls": self.scrolls,
            "delay_ms": self.delay_ms,
            "download_concurrency": self.download_concurrency,
            "with_watermark": self.with_watermark,
            "download": self.download,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "found_count": len(self.found_urls),
            "downloaded_count": len(self.downloaded_files),
            "failed_count": len(self.failed),
            "found_urls": self.found_urls,
            "downloaded_files": self.downloaded_files,
            "failed": self.failed,
            "logs": self.logs,
        }


def _load_config() -> dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def _download_dir() -> Path:
    config = _load_config()
    base = Path(config.get("API", {}).get("Download_Path", "./download"))
    if not base.is_absolute():
        base = PROJECT_ROOT / base
    path = base / "batch_douyin"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_filename(value: str, fallback: str) -> str:
    value = re.sub(r'[\\/:*?"<>|\r\n]+', "_", value or "").strip(" ._")
    return (value[:90] or fallback).strip()


def _make_search_url(keyword: str) -> str:
    return f"https://www.douyin.com/search/{quote(keyword)}?type=video"


async def _ensure_browser() -> BrowserContext:
    global _playwright, _browser_context
    if async_playwright is None:
        raise HTTPException(
            status_code=500,
            detail="Playwright is not installed. Run: pip install -r requirements.txt",
        )

    async with _browser_lock:
        if _browser_context:
            try:
                await _browser_context.cookies("https://www.douyin.com")
                return _browser_context
            except Exception:
                _browser_context = None

        if _playwright is None:
            _playwright = await async_playwright().start()

        launch_options = {
            "headless": False,
            "viewport": {"width": 1365, "height": 900},
            "accept_downloads": True,
        }
        channels = [
            os.getenv("BATCH_BROWSER_CHANNEL"),
            "msedge",
            "chrome",
            None,
        ]
        last_error = None
        for channel in channels:
            if channel is False:
                continue
            options = dict(launch_options)
            if channel:
                options["channel"] = channel
            try:
                _browser_context = await _playwright.chromium.launch_persistent_context(
                    str(BROWSER_DATA_DIR),
                    **options,
                )
                return _browser_context
            except Exception as exc:
                last_error = exc
                _browser_context = None

        raise HTTPException(
            status_code=500,
            detail=f"Cannot launch browser for login: {last_error}",
        )


async def _reset_browser() -> None:
    global _browser_context
    async with _browser_lock:
        if _browser_context:
            try:
                await _browser_context.close()
            except Exception:
                pass
        _browser_context = None


async def _ensure_live_browser() -> BrowserContext:
    try:
        return await _ensure_browser()
    except Exception:
        await _reset_browser()
        return await _ensure_browser()


async def _get_page() -> Page:
    context = await _ensure_live_browser()
    try:
        if context.pages:
            return context.pages[0]
        return await context.new_page()
    except Exception:
        await _reset_browser()
        context = await _ensure_browser()
        return context.pages[0] if context.pages else await context.new_page()


async def _cookie_string() -> str:
    context = await _ensure_live_browser()
    try:
        cookies = await context.cookies("https://www.douyin.com")
    except Exception:
        await _reset_browser()
        context = await _ensure_browser()
        cookies = await context.cookies("https://www.douyin.com")
    return "; ".join(f"{item['name']}={item['value']}" for item in cookies)


async def _sync_douyin_cookie_from_browser() -> str:
    cookie = await _cookie_string()
    if cookie:
        douyin_web_module.config["TokenManager"]["douyin"]["headers"]["Cookie"] = cookie
    return cookie


async def _collect_urls_from_page(
    page: Page,
    job: BatchJob,
) -> list[str]:
    urls: dict[str, None] = {}

    async def collect_once() -> None:
        hrefs = await page.eval_on_selector_all(
            "a[href]",
            "els => els.map(el => el.href).filter(Boolean)",
        )
        html = await page.content()
        candidates = list(hrefs)
        candidates.extend(match.group(0) for match in VIDEO_URL_RE.finditer(html))
        candidates.extend(
            f"https://www.douyin.com/video/{match.group(1)}"
            for match in MODAL_ID_RE.finditer(html)
        )
        for candidate in candidates:
            match = VIDEO_URL_RE.search(candidate)
            if match:
                urls[f"https://www.douyin.com/video/{match.group(1)}"] = None
            elif modal_match := MODAL_ID_RE.search(candidate):
                urls[f"https://www.douyin.com/video/{modal_match.group(1)}"] = None

    await collect_once()
    for index in range(job.scrolls):
        if len(urls) >= job.max_items:
            break
        await page.mouse.wheel(0, 2600)
        await page.wait_for_timeout(job.delay_ms)
        await collect_once()
        job.found_urls = list(urls.keys())[: job.max_items]
        job.log(f"滚动 {index + 1}/{job.scrolls}，已采集 {len(job.found_urls)} 条 URL")

    return list(urls.keys())[: job.max_items]


async def _download_video(url: str, job: BatchJob) -> Path:
    data = await HybridCrawlerInstance.hybrid_parsing_single_video(url, minimal=True)
    if data.get("type") != "video":
        raise ValueError(f"不是视频作品，已跳过: {data.get('type')}")

    video_data = data.get("video_data") or {}
    source_url = (
        video_data.get("wm_video_url_HQ")
        if job.with_watermark
        else video_data.get("nwm_video_url_HQ")
    )
    if not source_url:
        raise ValueError("没有解析到 mp4 下载地址")

    headers = await HybridCrawlerInstance.DouyinWebCrawler.get_douyin_headers()
    filename = _safe_filename(
        f"{data.get('video_id')}_{data.get('desc')}",
        data.get("video_id") or AWEME_ID_RE.search(url).group(0),
    )
    file_path = _download_dir() / f"{filename}.mp4"
    if file_path.exists() and file_path.stat().st_size > 0:
        return file_path

    temp_path = file_path.with_suffix(file_path.suffix + ".part")
    request_headers = dict(headers.get("headers") or {})
    request_headers.setdefault("Referer", "https://www.douyin.com/")
    request_headers.setdefault("Accept", "*/*")

    last_error = None
    for attempt in range(1, 4):
        if temp_path.exists():
            temp_path.unlink()
        try:
            job.log(f"请求视频流，第 {attempt}/3 次: {data.get('video_id')}")
            timeout = httpx.Timeout(90.0, connect=20.0, read=60.0, write=30.0)
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=True,
            ) as client:
                async with client.stream(
                    "GET",
                    source_url,
                    headers=request_headers,
                ) as response:
                    response.raise_for_status()
                    async with aiofiles.open(temp_path, "wb") as output:
                        async for chunk in response.aiter_bytes():
                            await output.write(chunk)

            if not temp_path.exists() or temp_path.stat().st_size == 0:
                raise ValueError("下载结果为空")
            temp_path.replace(file_path)
            return file_path
        except Exception as exc:
            last_error = exc
            if temp_path.exists():
                temp_path.unlink()
            if attempt < 3:
                await asyncio.sleep(2 * attempt)

    raise ValueError(f"下载视频流失败，已重试 3 次: {last_error}")
    return file_path


async def _run_batch_job(job: BatchJob) -> None:
    try:
        job.status = "running"
        job.log("正在同步浏览器登录 Cookie")
        cookie = await _sync_douyin_cookie_from_browser()
        if not cookie:
            job.log("没有读到 douyin.com Cookie，请先在登录窗口完成登录")

        page = await _get_page()
        await page.goto(_make_search_url(job.keyword), wait_until="domcontentloaded")
        await page.wait_for_timeout(2500)
        job.log(f"已打开搜索页: {job.keyword}")

        job.found_urls = await _collect_urls_from_page(page, job)
        job.log(f"采集完成，共 {len(job.found_urls)} 条 URL")

        if not job.download:
            job.status = "done"
            return

        queue: asyncio.Queue[tuple[int, str]] = asyncio.Queue()
        for item_index, item_url in enumerate(job.found_urls, start=1):
            queue.put_nowait((item_index, item_url))

        async def worker(worker_id: int) -> None:
            while True:
                try:
                    item_index, item_url = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    job.log(
                        f"下载 {item_index}/{len(job.found_urls)} "
                        f"[worker {worker_id}]: {item_url}"
                    )
                    file_path = await _download_video(item_url, job)
                    job.downloaded_files.append(str(file_path))
                    job.log(f"完成: {file_path.name}")
                except Exception as exc:
                    job.failed.append({"url": item_url, "error": str(exc)})
                    job.log(f"失败: {item_url} -> {exc}")
                finally:
                    queue.task_done()

        job.log(f"开始并发下载，并发数: {job.download_concurrency}")
        workers = [
            asyncio.create_task(worker(worker_index))
            for worker_index in range(
                1,
                min(job.download_concurrency, len(job.found_urls)) + 1,
            )
        ]
        if workers:
            await asyncio.gather(*workers)
        job.status = "done"
    except Exception as exc:
        job.status = "failed"
        job.failed.append({"url": "", "error": str(exc)})
        job.log(f"任务异常: {exc}")
    finally:
        job.updated_at = time.time()

@router.get("/batch", response_class=HTMLResponse, include_in_schema=False)
async def batch_page():
    return HTMLResponse(BATCH_HTML)


@router.post("/batch/login", summary="打开抖音登录浏览器/Open Douyin login browser")
async def open_login_browser(payload: LoginRequest):
    page = await _get_page()
    await page.goto(_make_search_url(payload.keyword), wait_until="domcontentloaded")
    return {
        "code": 200,
        "message": "登录窗口已打开。请在弹出的浏览器中登录抖音账号。",
        "url": page.url,
    }


@router.get("/batch/login_status", summary="检查浏览器 Cookie 状态/Check browser cookie status")
async def login_status():
    cookie = await _cookie_string()
    page = await _get_page()
    return {
        "code": 200,
        "logged_cookie_count": len([part for part in cookie.split("; ") if part]),
        "has_login_cookie": any(
            key in cookie for key in ("sessionid", "sid_guard", "passport_csrf_token")
        ),
        "current_url": page.url,
    }


@router.post("/batch/start", summary="启动关键词采集和批量下载/Start keyword crawl and batch download")
async def start_batch(payload: BatchStartRequest):
    job = BatchJob(
        id=uuid.uuid4().hex,
        keyword=payload.keyword,
        max_items=payload.max_items,
        scrolls=payload.scrolls,
        delay_ms=payload.delay_ms,
        download_concurrency=payload.download_concurrency,
        with_watermark=payload.with_watermark,
        download=payload.download,
    )
    _jobs[job.id] = job
    asyncio.create_task(_run_batch_job(job))
    return {"code": 200, "job_id": job.id, "status_url": f"/api/batch/jobs/{job.id}"}


@router.get("/batch/jobs/{job_id}", summary="获取批量任务状态/Get batch job status")
async def get_job(job_id: str):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"code": 200, "data": job.to_dict()}


@router.get("/batch/jobs", summary="列出批量任务/List batch jobs")
async def list_jobs():
    return {"code": 200, "data": [job.to_dict() for job in _jobs.values()]}


BATCH_HTML = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>抖音关键词批量下载</title>
  <style>
    :root { color-scheme: light; font-family: Inter, "Microsoft YaHei", Arial, sans-serif; }
    body { margin: 0; background: #f7f7f4; color: #1f2933; }
    main { max-width: 1120px; margin: 0 auto; padding: 28px; }
    h1 { margin: 0 0 18px; font-size: 28px; }
    section { margin-top: 18px; padding: 18px; border: 1px solid #d8ded8; border-radius: 8px; background: #fff; }
    label { display: block; font-size: 13px; font-weight: 700; margin-bottom: 6px; }
    input { width: 100%; box-sizing: border-box; padding: 10px 12px; border: 1px solid #c8d0cc; border-radius: 6px; font-size: 14px; }
    .grid { display: grid; grid-template-columns: repeat(4, minmax(120px, 1fr)); gap: 12px; }
    .actions { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 14px; }
    button { border: 0; border-radius: 6px; padding: 10px 14px; cursor: pointer; color: #fff; background: #116149; font-weight: 700; }
    button.secondary { background: #46515c; }
    button:disabled { opacity: .55; cursor: wait; }
    pre { min-height: 220px; max-height: 420px; overflow: auto; padding: 14px; border-radius: 8px; background: #111827; color: #d1fae5; white-space: pre-wrap; }
    .muted { color: #65727d; font-size: 13px; }
    .pill { display: inline-block; padding: 4px 8px; border-radius: 999px; background: #e6f4ef; color: #116149; font-weight: 700; font-size: 12px; }
    @media (max-width: 760px) { main { padding: 16px; } .grid { grid-template-columns: 1fr 1fr; } }
  </style>
</head>
<body>
<main>
  <h1>抖音关键词批量下载</h1>
  <p class="muted">先打开登录窗口并登录自己的抖音账号，再启动采集。浏览器会保留登录状态到本项目的 <code>.batch_browser_profile</code> 目录。</p>
  <section>
    <div class="grid">
      <div><label>关键词</label><input id="keyword" value="vlog" /></div>
      <div><label>最大视频数</label><input id="max_items" type="number" value="30" min="1" max="20000" /></div>
      <div><label>滚动次数</label><input id="scrolls" type="number" value="20" min="1" max="300" /></div>
      <div><label>滚动间隔 ms</label><input id="delay_ms" type="number" value="1200" min="200" max="10000" /></div>
      <div><label>下载并发数</label><input id="download_concurrency" type="number" value="4" min="1" max="20" /></div>
    </div>
    <div class="actions">
      <button onclick="openLogin()">打开登录窗口</button>
      <button class="secondary" onclick="checkLogin()">检查登录状态</button>
      <button onclick="startJob()">开始采集并下载 mp4</button>
      <button class="secondary" onclick="startJob(false)">只采集 URL</button>
    </div>
  </section>
  <section>
    <span class="pill" id="status">idle</span>
    <pre id="log"></pre>
  </section>
</main>
<script>
let currentJob = null;
let timer = null;
const logEl = document.querySelector("#log");
const statusEl = document.querySelector("#status");

function payload(download = true) {
  return {
    keyword: document.querySelector("#keyword").value || "vlog",
    max_items: Number(document.querySelector("#max_items").value || 30),
    scrolls: Number(document.querySelector("#scrolls").value || 20),
    delay_ms: Number(document.querySelector("#delay_ms").value || 1200),
    download_concurrency: Number(document.querySelector("#download_concurrency").value || 4),
    with_watermark: false,
    download
  };
}
function write(obj) {
  logEl.textContent = typeof obj === "string" ? obj : JSON.stringify(obj, null, 2);
}
async function openLogin() {
  const res = await fetch("/api/batch/login", {
    method: "POST",
    headers: {"content-type": "application/json"},
    body: JSON.stringify({keyword: payload().keyword})
  });
  write(await res.json());
}
async function checkLogin() {
  const res = await fetch("/api/batch/login_status");
  write(await res.json());
}
async function startJob(download = true) {
  const res = await fetch("/api/batch/start", {
    method: "POST",
    headers: {"content-type": "application/json"},
    body: JSON.stringify(payload(download))
  });
  const data = await res.json();
  currentJob = data.job_id;
  if (timer) clearInterval(timer);
  timer = setInterval(refresh, 1500);
  await refresh();
}
async function refresh() {
  if (!currentJob) return;
  const res = await fetch(`/api/batch/jobs/${currentJob}`);
  const json = await res.json();
  const data = json.data;
  statusEl.textContent = `${data.status} | URL ${data.found_count} | 完成 ${data.downloaded_count} | 失败 ${data.failed_count}`;
  write([
    ...data.logs,
    "",
    "URL List:",
    ...data.found_urls,
    "",
    "Downloaded Files:",
    ...data.downloaded_files,
    "",
    "Failed:",
    ...data.failed.map(x => `${x.url} ${x.error}`)
  ].join("\\n"));
  if (["done", "failed"].includes(data.status)) clearInterval(timer);
}
</script>
</body>
</html>
"""


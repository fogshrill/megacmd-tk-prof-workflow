import os
import sys
import json
import asyncio
import random
import re
import zipfile
import glob
from pathlib import Path
from datetime import datetime
import httpx
from loguru import logger
import yt_dlp

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ---------------------------------------------------------
# 1. Batch Folder Setup
# ---------------------------------------------------------
_env_batch = os.environ.get("BATCH_FOLDER_NAME", "").strip()
BATCH_FOLDER_NAME = _env_batch if _env_batch else \
    f"Batch--{datetime.now().strftime('%Y-%m-%d-%A_%I-%M-%S-%p')}"

CHUNK_INDEX  = int(os.environ.get("CHUNK_INDEX",  "0"))
TOTAL_CHUNKS = int(os.environ.get("TOTAL_CHUNKS", "1"))

try:
    os.makedirs(BATCH_FOLDER_NAME, exist_ok=True)
    logger.info(f"📁 Batch Folder: '{BATCH_FOLDER_NAME}'  [Chunk {CHUNK_INDEX+1}/{TOTAL_CHUNKS}]")
except Exception as e:
    logger.warning(f"⚠️ Folder Error: {e}")

CONFIG = {
    "base_dir":               BATCH_FOLDER_NAME,
    "download_media":         True,
    "http2":                  False,
    "proxy":                  None,
    "timeout":                60.0,
    "delay_between_pages":    (1.0, 2.5),
    "delay_between_videos":   (1.0, 3.0),
    "video_concurrency":      10,
    "comment_concurrency":    8,
    "max_comments_limit":     10000,
    "upload_concurrency":     1,    # 1 per node — mega-put handles queuing internally
    "hard_link_limit":        99999,  # ← UPDATED: no artificial cap — handle all links
}

_upload_sem: asyncio.Semaphore = None

# ---------------------------------------------------------
# 2. TXT-Based Tracking System
# ---------------------------------------------------------
_suffix        = f"_chunk{CHUNK_INDEX}" if TOTAL_CHUNKS > 1 else ""
TRACKING_FILE  = f"tracking_report{_suffix}.txt"
COMPLETED_FILE = f"completed{_suffix}.txt"
FAILED_FILE    = f"failed{_suffix}.txt"
LOG_FILE       = f"scraper_log{_suffix}.txt"

# ---------------------------------------------------------
# 2b. SQLite Master Index — persistent across sessions
#     Stored locally during run, then uploaded to Mega _Reports/
#     Use find.py locally to search any post/session/failed
# ---------------------------------------------------------
import sqlite3 as _sqlite3

_INDEX_DB = "index.db"

def _init_index():
    db = _sqlite3.connect(_INDEX_DB)
    db.execute("""
        CREATE TABLE IF NOT EXISTS files (
            tiktok_url  TEXT PRIMARY KEY,
            tiktok_id   TEXT,
            author      TEXT,
            batch_id    TEXT,
            account_id  TEXT,
            mega_path   TEXT,
            sub_dir     TEXT,
            status      TEXT DEFAULT 'done',
            chunk_index INTEGER,
            uploaded_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
        )
    """)
    db.commit()
    db.close()

_init_index()

def _index_write(tiktok_url, tiktok_id, author, batch_id,
                 account_id, mega_path, sub_dir, status="done"):
    """Upload ke baad index.db mein record likho — silently fail karo agar error ho."""
    try:
        db = _sqlite3.connect(_INDEX_DB)
        db.execute("""
            INSERT OR REPLACE INTO files
              (tiktok_url, tiktok_id, author, batch_id, account_id,
               mega_path, sub_dir, status, chunk_index)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (tiktok_url, tiktok_id, author, batch_id, account_id,
              mega_path, sub_dir, status, CHUNK_INDEX))
        db.commit()
        db.close()
    except Exception:
        pass  # index failure scraper ko nahi rokna chahiye

def _append_tracking(status: str, url: str, note: str = ""):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{status}] {url}"
    if note:
        line += f" | {note}"
    try:
        with open(TRACKING_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        logger.warning(f"⚠️ Tracking write error: {e}")

async def track_success(url: str, file_lock: asyncio.Lock):
    async with file_lock:
        _append_tracking("SUCCESS", url)
        with open(COMPLETED_FILE, "a", encoding="utf-8") as f:
            f.write(url + "\n")

async def track_failed(url: str, note: str, file_lock: asyncio.Lock):
    async with file_lock:
        _append_tracking("FAILED", url, note)
        with open(FAILED_FILE, "a", encoding="utf-8") as f:
            f.write(url + "\n")

async def track_skipped(url: str, note: str, file_lock: asyncio.Lock):
    async with file_lock:
        _append_tracking("SKIPPED", url, note)

def load_set_from_file(filepath: str) -> set:
    if not os.path.exists(filepath):
        return set()
    with open(filepath, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

# ---------------------------------------------------------
# 3. Logger Setup
# ---------------------------------------------------------
logger.remove()
logger.add(sys.stdout,
           format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")
logger.add(LOG_FILE, level="DEBUG",
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
           rotation="10 MB")

# ---------------------------------------------------------
# 4. Helpers
# ---------------------------------------------------------
def clean_caption(text: str) -> str:
    text = re.sub(r'#\w*', '', text)
    text = re.sub(r'[^a-zA-Z0-9 \-_\.]', ' ', text)
    text = re.sub(r'[ _]+', '_', text).strip('_. ')
    return text[:40] if text.strip('_. ') else 'no_caption'

def sanitize_folder_name(name: str) -> str:
    # FIX 1: Replace dots and slashes — Mega rejects dots, slashes break local paths
    # FIX 2: Replace any other chars Mega dislikes
    name = name.replace(".", "_")   # dot → underscore  (fixes: Invalid arguments)
    name = name.replace("/", "_")   # slash → underscore (fixes: [Errno 2] No such file or directory)
    name = name.replace("\\", "_")  # backslash safety
    return name

def human_ts(unix_ts):
    if not unix_ts:
        return datetime.now().strftime("%Y-%m-%d")
    try:
        return datetime.fromtimestamp(int(unix_ts)).strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

# ---------------------------------------------------------
# ZIP artifact builder for GitHub Actions
#      Zips ALL scraped data (entire batch folder) + report files.
#      NO deletion anywhere — data stays on disk AND in ZIP.
#      GitHub Actions picks this up via actions/upload-artifact.
#
#      Add to your workflow YAML:
#        - uses: actions/upload-artifact@v4
#          with:
#            name: scraper-chunk-${{ matrix.chunk }}
#            path: "*_artifact.zip"
# ---------------------------------------------------------
def build_github_artifact():
    """
    Build a ZIP of everything this pod scraped:
      - All report/tracking/log files
      - Entire BATCH_FOLDER_NAME directory (all video folders, all files)
    Data is NOT deleted — ZIP is an additional copy for GitHub artifact store.
    """
    zip_name = f"{BATCH_FOLDER_NAME}{_suffix}_artifact.zip"
    logger.info(f"📦 Building GitHub artifact ZIP: {zip_name}")
    try:
        with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            # Include all report files
            for rfile in [TRACKING_FILE, COMPLETED_FILE, FAILED_FILE, LOG_FILE]:
                if os.path.exists(rfile):
                    zf.write(rfile, rfile)

            # Include ENTIRE batch folder — all scraped data, no exclusions
            base_path = Path(BATCH_FOLDER_NAME)
            if base_path.exists():
                for item in base_path.rglob("*"):
                    if item.is_file():
                        zf.write(str(item), str(item))

        size_mb = os.path.getsize(zip_name) / 1_048_576
        logger.success(f"📦 Artifact ZIP ready: {zip_name} ({size_mb:.1f} MB)")
        return zip_name
    except Exception as e:
        logger.error(f"❌ ZIP build failed: {e}")
        return None

# ---------------------------------------------------------
# 5. H.264 Codec Fix
# ---------------------------------------------------------
async def ensure_h264(video_path: Path, log_prefix: str) -> bool:
    if not video_path.exists():
        return False
    try:
        probe_cmd = [
            "ffprobe", "-v", "quiet", "-select_streams", "v:0",
            "-show_entries", "stream=codec_name",
            "-of", "csv=p=0", str(video_path)
        ]
        proc = await asyncio.create_subprocess_exec(
            *probe_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        codec = stdout.decode().strip().lower()

        if codec in ("h264", "avc1", "avc"):
            return True

        logger.debug(f"{log_prefix} codec={codec} → re-encoding to H.264 silently...")
        tmp_path = video_path.with_suffix(".h264_tmp.mp4")

        transcode_cmd = [
            "ffmpeg", "-i", str(video_path),
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-profile:v", "high", "-level", "4.1",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            "-y", str(tmp_path)
        ]
        proc = await asyncio.create_subprocess_exec(
            *transcode_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()

        if proc.returncode == 0 and tmp_path.exists():
            tmp_path.replace(video_path)
            logger.debug(f"{log_prefix} re-encoded → H.264 done.")
            return True
        else:
            logger.error(f"{log_prefix} ❌ Transcode failed: {stderr.decode()[:300]}")
            tmp_path.unlink(missing_ok=True)
            return False

    except FileNotFoundError:
        logger.warning(f"{log_prefix} ⚠️ ffprobe/ffmpeg not found — skipping codec check.")
        return True
    except Exception as e:
        logger.error(f"{log_prefix} ❌ ensure_h264 error: {e}")
        return False

# ---------------------------------------------------------
# 6. YT-DLP
# ---------------------------------------------------------
def download_with_ytdlp(url, output_path):
    ydl_opts = {
        'outtmpl':             str(output_path),
        'quiet':               True,
        'no_warnings':         True,
        'noprogress':          True,
        'socket_timeout':      30,
        'format': (
            'bestvideo[vcodec^=avc1]+bestaudio[acodec^=mp4a]'
            '/bestvideo[vcodec^=avc1]+bestaudio'
            '/bestvideo+bestaudio/best'
        ),
        'merge_output_format': 'mp4',
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True
    except Exception as e:
        logger.error(f"yt-dlp error: {e}")
        return False

# ---------------------------------------------------------
# 7. MEGAcmd Upload (mega-put — single session, all nodes)
# sub_dir flows through: main → worker_task → scrape_video → upload_to_mega
# Mega folder structure:
#   /Batch--xxx/mehranaslam45/posts/@author_slug_id/
# ---------------------------------------------------------
async def upload_to_mega(local_folder_path, folder_name, log_prefix, sub_dir="",
                         tiktok_url="", tiktok_id="", author=""):
    global _upload_sem
    sem = _upload_sem or asyncio.Semaphore(CONFIG["upload_concurrency"])
    async with sem:
        # Build remote path — with or without sub_dir
        if sub_dir:
            remote_path = f"/{BATCH_FOLDER_NAME}/{sub_dir}/{folder_name}"
        else:
            remote_path = f"/{BATCH_FOLDER_NAME}/{folder_name}"

        logger.info(f"{log_prefix} ☁️ mega-put → {remote_path}")

        # 3 attempts with backoff
        for attempt in range(1, 4):
            cmd = ["mega-put", "-c", str(local_folder_path), remote_path]
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                _, stderr = await proc.communicate()

                if proc.returncode == 0:
                    logger.success(f"{log_prefix} 🚀 mega-put Done → {remote_path}")
                    # ── INDEX: record successful upload ──
                    _index_write(
                        tiktok_url=tiktok_url,
                        tiktok_id=tiktok_id,
                        author=author,
                        batch_id=BATCH_FOLDER_NAME,
                        account_id="MEGA_SESSION",
                        mega_path=remote_path,
                        sub_dir=sub_dir,
                        status="done"
                    )
                    return True
                else:
                    err = stderr.decode().strip()
                    logger.warning(f"{log_prefix} ⚠️ Attempt {attempt}/3 failed: {err}")
                    # ── INDEX: record failed attempt ──
                    if attempt == 3:
                        _index_write(
                            tiktok_url=tiktok_url,
                            tiktok_id=tiktok_id,
                            author=author,
                            batch_id=BATCH_FOLDER_NAME,
                            account_id="MEGA_SESSION",
                            mega_path="",
                            sub_dir=sub_dir,
                            status="failed"
                        )
                    else:
                        await asyncio.sleep(5 * attempt)  # 5s, 10s backoff
            except Exception as e:
                logger.error(f"{log_prefix} ❌ mega-put exception: {e}")
                if attempt < 3:
                    await asyncio.sleep(5)

        logger.error(f"{log_prefix} ❌ mega-put failed after 3 attempts.")
        return False

async def upload_report_files():
    remote_path = f"/{BATCH_FOLDER_NAME}/_Reports"
    for fpath in [TRACKING_FILE, LOG_FILE, COMPLETED_FILE, FAILED_FILE, _INDEX_DB]:
        if not os.path.exists(fpath):
            continue
        try:
            cmd = ["mega-put", "-c", fpath, remote_path]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            logger.success(f"✅ Report uploaded: {fpath} → {remote_path}")
        except Exception as e:
            logger.error(f"❌ Report upload failed ({fpath}): {e}")

# ---------------------------------------------------------
# 8. Scraper Engine
# ---------------------------------------------------------
class TikTokScraperV5:
    def __init__(self, config):
        self.cfg = config
        self.base_path = Path(config["base_dir"])
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept":     "application/json, text/plain, */*",
            "Referer":    "https://www.tiktok.com/"
        }
        self.client = httpx.AsyncClient(
            http2=config["http2"],
            timeout=config["timeout"],
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20)
        )
        self.sem_comments = asyncio.Semaphore(config["comment_concurrency"])

    async def download_file_httpx(self, url, path, log_prefix, item_name="Media"):
        if path.exists():
            return True
        try:
            dl_headers = self.headers.copy()
            dl_headers["Accept"] = "*/*"
            resp = await self.client.get(url, headers=dl_headers, timeout=60, follow_redirects=True)
            if resp.status_code == 403:
                del dl_headers["Referer"]
                resp = await self.client.get(url, headers=dl_headers, timeout=60, follow_redirects=True)
            resp.raise_for_status()
            Path(path).write_bytes(resp.content)
            logger.success(f"{log_prefix} 📥 Saved: {item_name}")
            return True
        except Exception as e:
            logger.error(f"{log_prefix} ❌ {item_name} Error: {e}")
            return False

    async def get_video_meta(self, url, track_id):
        clean_url = url.replace("/photo/", "/video/")
        logger.info(f"{track_id} 🌐 Fetching HTML page...")
        try:
            resp  = await self.client.get(clean_url, headers=self.headers, follow_redirects=True)
            match = re.search(
                r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">([\s\S]*?)</script>',
                resp.text
            )
            if not match:
                return None
            data = json.loads(match.group(1))
            item = (data.get("__DEFAULT_SCOPE__", {})
                        .get("webapp.video-detail", {})
                        .get("itemInfo", {})
                        .get("itemStruct"))
            if not item:
                item = (data.get("__DEFAULT_SCOPE__", {})
                            .get("webapp.image-detail", {})
                            .get("itemInfo", {})
                            .get("itemStruct"))
            return item
        except:
            return None

    # ── CHANGE 2: sub_dir parameter added to scrape_video ──
    # sub_dir flows from main() → worker_task() → scrape_video() → upload_to_mega()
    # It determines both local subfolder AND Mega remote path
    async def scrape_video(self, url, index, total, file_lock, sub_dir=""):
        track_id  = f"[{index}/{total}]"
        logger.info(f"{'-'*50}\n{track_id} 🚀 URL: {url} | 📂 Category: {sub_dir or 'root'}")

        # ── CHECKPOINT 1: Meta fetch ──────────────────────────────────────────
        item = await self.get_video_meta(url, track_id)
        if not item:
            logger.error(f"{track_id} ❌ Meta not found or Blocked.")
            await track_failed(url, "FAIL:meta_fetch — TikTok blocked or page unavailable", file_lock)
            return False

        v_id       = item["id"]
        author     = item.get("author", {}).get("uniqueId", "unknown")
        cap_slug   = clean_caption(item.get("desc", "no_caption"))
        post_ts    = human_ts(item.get("createTime"))
        log_prefix = f"{track_id} [@{author}]"

        raw_folder  = f"@{author}_{cap_slug}_{v_id}"
        folder_name = sanitize_folder_name(raw_folder)

        f_base      = f"@{author}_{cap_slug}"
        f_ts_id     = f"{post_ts}_{v_id}"

        # ── CHANGE 3: v_path now includes sub_dir for local folder structure ──
        # Local:  Batch--xxx/mehranaslam45/posts/@author_slug_id/
        # Mega:   vfx:/Batch--xxx/mehranaslam45/posts/@author_slug_id/
        if sub_dir:
            v_path = self.base_path / sub_dir / folder_name
        else:
            v_path = self.base_path / folder_name
        v_path.mkdir(parents=True, exist_ok=True)

        # ── 1. JSON FILES ─────────────────────────────────────────────────────
        # CHECKPOINT 2: File save
        try:
            (v_path / f"{f_base}_RAW-meta_{f_ts_id}.json").write_text(
                json.dumps(item, indent=2, ensure_ascii=False), encoding="utf-8")

            (v_path / f"{f_base}_meta_{f_ts_id}.json").write_text(
                json.dumps({
                    "post_info": {
                        "id":         v_id,
                        "desc":       item.get("desc"),
                        "createTime": item.get("createTime"),
                        "posted_at":  post_ts
                    },
                    "stats":  item.get("statsV2", item.get("stats", {})),
                    "author": item.get("author", {}),
                    "music":  item.get("music", {})
                }, indent=2, ensure_ascii=False), encoding="utf-8")

            (v_path / f"{f_base}_caption_{f_ts_id}.json").write_text(
                json.dumps({
                    "username": author,
                    "post_url": url,
                    "caption":  item.get("desc", ""),
                    "hashtags": re.findall(r"#\w+", item.get("desc", ""))
                }, indent=2, ensure_ascii=False), encoding="utf-8")

            (v_path / f"{f_base}_account_{f_ts_id}.json").write_text(
                json.dumps({
                    "author_details": item.get("author", {}),
                    "author_stats":   item.get("authorStats", {})
                }, indent=2, ensure_ascii=False), encoding="utf-8")

            logger.success(f"{log_prefix} 📝 Saved: RAW-meta, meta, caption, account")
        except Exception as e:
            logger.error(f"{log_prefix} ❌ JSON save failed: {e}")
            await track_failed(url, f"FAIL:json_save — {e}", file_lock)
            return False

        # ── 2. MEDIA DOWNLOADS ────────────────────────────────────────────────
        # CHECKPOINT 3: Media (video/thumbnail/audio/caption)
        media_ok = True
        if self.cfg.get("download_media", True):

            # Avatar
            avatar_url = (item.get("author", {}).get("avatarLarger")
                          or item.get("author", {}).get("avatarMedium"))
            if avatar_url:
                ok = await self.download_file_httpx(
                    avatar_url,
                    v_path / f"{f_base}_avatar_{f_ts_id}.jpg",
                    log_prefix, "Avatar")
                if not ok:
                    media_ok = False

            image_post = item.get("imagePost")
            if image_post and image_post.get("images"):
                # ── CAROUSEL ────────────────────────────────────────────────
                images = image_post.get("images", [])
                logger.info(f"{log_prefix} 📸 Carousel mode ({len(images)} images).")
                failed_indices = []

                for i, img in enumerate(images):
                    img_url = (
                        img.get("imageURL",    {}).get("urlList", [None])[0]
                        or img.get("displayImage", {}).get("urlList", [None])[0]
                    )
                    img_path = v_path / f"{f_base}_carousel-{i+1:03d}_{f_ts_id}.jpg"
                    if img_url:
                        ok = await self.download_file_httpx(
                            img_url, img_path, log_prefix, f"Carousel {i+1}")
                        if not ok:
                            failed_indices.append(i)
                    else:
                        failed_indices.append(i)

                if failed_indices:
                    logger.info(
                        f"{log_prefix} 🔄 Carousel yt-dlp fallback "
                        f"for {len(failed_indices)} failed images...")
                    yt_out = v_path / f"{f_base}_carousel-ytdlp_{f_ts_id}.%(ext)s"
                    if await asyncio.to_thread(download_with_ytdlp, url, yt_out):
                        logger.success(f"{log_prefix} 📥 Carousel yt-dlp done.")
                    else:
                        logger.error(f"{log_prefix} ❌ Carousel yt-dlp fallback failed.")
                        media_ok = False

                music_data = item.get("music", {})
                audio_url  = music_data.get("playUrl")
                if isinstance(audio_url, dict):
                    audio_url = audio_url.get("urlList", [None])[0]
                if isinstance(audio_url, list):
                    audio_url = audio_url[0]
                if audio_url:
                    ok = await self.download_file_httpx(
                        audio_url,
                        v_path / f"{f_base}_audio_{f_ts_id}.mp3",
                        log_prefix, "Carousel Audio")
                    if not ok:
                        media_ok = False

            else:
                # ── VIDEO ────────────────────────────────────────────────────
                video_data = item.get("video", {})
                play_url   = None

                for br in (video_data.get("bitrateInfo") or video_data.get("bitRateList") or []):
                    try:
                        play_url = br.get("PlayAddr", {}).get("UrlList", [None])[0]
                        if play_url:
                            break
                    except:
                        pass

                if not play_url:
                    for key in ("downloadAddr", "playAddr"):
                        val = video_data.get(key)
                        if isinstance(val, str) and val:
                            play_url = val; break
                        elif isinstance(val, list) and val:
                            play_url = val[0]; break

                video_path = v_path / f"{f_base}_video_{f_ts_id}.mp4"
                success    = False

                if play_url:
                    try:
                        resp = await self.client.get(
                            play_url, headers=self.headers,
                            timeout=90, follow_redirects=True)
                        if resp.status_code == 200:
                            video_path.write_bytes(resp.content)
                            logger.success(f"{log_prefix} 📥 Video Saved (Direct).")
                            success = True
                            await ensure_h264(video_path, log_prefix)
                        else:
                            logger.warning(
                                f"{log_prefix} ⚠️ Direct {resp.status_code} → yt-dlp...")
                    except Exception as e:
                        logger.warning(f"{log_prefix} ⚠️ Direct error → yt-dlp: {e}")

                if not success:
                    logger.info(f"{log_prefix} 🔄 yt-dlp fallback (strict H.264)...")
                    if await asyncio.to_thread(download_with_ytdlp, url, video_path):
                        logger.success(f"{log_prefix} 📥 Video Saved (yt-dlp).")
                        await ensure_h264(video_path, log_prefix)
                    else:
                        logger.error(f"{log_prefix} ❌ Video download failed.")
                        media_ok = False

                music_data = item.get("music", {})
                audio_url  = music_data.get("playUrl")
                if isinstance(audio_url, dict):
                    audio_url = audio_url.get("urlList", [None])[0]
                if isinstance(audio_url, list):
                    audio_url = audio_url[0]
                if audio_url:
                    ok = await self.download_file_httpx(
                        audio_url,
                        v_path / f"{f_base}_audio_{f_ts_id}.mp3",
                        log_prefix, "Audio")
                    if not ok:
                        media_ok = False

        if not media_ok:
            logger.warning(f"{log_prefix} ⚠️ Some media files failed — proceeding to upload remaining.")

        # ── 3. COMMENTS ───────────────────────────────────────────────────────
        # CHECKPOINT 4: Comments
        comments_ok = await self.fetch_comments(v_id, v_path, f_base, f_ts_id, log_prefix)
        if not comments_ok:
            logger.warning(f"{log_prefix} ⚠️ Comments incomplete — proceeding to upload.")

        # ── 4. UPLOAD + TRACK ─────────────────────────────────────────────────
        # CHECKPOINT 5: Mega upload — only SUCCESS when Mega confirms
        # ── CHANGE 4: sub_dir passed to upload_to_mega ──
        upload_ok = await upload_to_mega(v_path, folder_name, log_prefix, sub_dir,
                                         tiktok_url=url, tiktok_id=v_id, author=author)

        if not upload_ok:
            fail_parts = []
            if not media_ok:     fail_parts.append("media_partial")
            if not comments_ok:  fail_parts.append("comments_incomplete")
            fail_parts.append("FAIL:mega_upload")
            await track_failed(url, " | ".join(fail_parts), file_lock)
            return False

        # All 5 checkpoints passed
        if not media_ok or not comments_ok:
            note = []
            if not media_ok:    note.append("media_partial")
            if not comments_ok: note.append("comments_incomplete")
            _append_tracking("SUCCESS_PARTIAL", url, " | ".join(note))
            async with file_lock:
                with open(COMPLETED_FILE, "a", encoding="utf-8") as f:
                    f.write(url + "\n")
        else:
            await track_success(url, file_lock)

        return True

    async def fetch_replies(self, video_id, comment_id, raw_list, clean_list, log_prefix):
        async with self.sem_comments:
            cursor, has_more = 0, 1
            while has_more:
                try:
                    resp = await self.client.get(
                        "https://www.tiktok.com/api/comment/list/reply/",
                        params={"item_id": video_id, "comment_id": comment_id,
                                "cursor": cursor, "count": 50, "aid": "1988"},
                        headers=self.headers)
                    data    = resp.json()
                    replies = data.get("comments") or []
                    if not replies:
                        break
                    raw_list.extend(replies)
                    for c in replies:
                        clean_list.append({
                            "is_reply":          True,
                            "parent_comment_id": comment_id,
                            "cid":               c.get("cid"),
                            "text":              c.get("text"),
                            "likes":             c.get("digg_count"),
                            "create_time":       c.get("create_time"),
                            "user":              {"username": c.get("user", {}).get("unique_id")}
                        })
                    has_more = data.get("has_more", 0)
                    cursor   = data.get("cursor", cursor + len(replies))
                    await asyncio.sleep(random.uniform(*self.cfg["delay_between_pages"]))
                except:
                    break

    async def fetch_comments(self, video_id, path, f_base, f_ts_id, log_prefix):
        raw_path   = path / f"{f_base}_RAW-comments_{f_ts_id}.json"
        clean_path = path / f"{f_base}_comments_{f_ts_id}.json"

        raw_comments, clean_comments, cursor = [], [], 0

        if raw_path.exists() and clean_path.exists():
            try:
                raw_comments   = json.loads(raw_path.read_text(encoding="utf-8"))
                clean_comments = json.loads(clean_path.read_text(encoding="utf-8"))
                cursor         = len([c for c in clean_comments if not c.get("is_reply")])
                logger.info(f"{log_prefix} 🔄 Resuming from {cursor} comments...")
            except:
                raw_comments, clean_comments, cursor = [], [], 0

        if len(raw_comments) >= self.cfg["max_comments_limit"]:
            return True

        logger.info(f"{log_prefix} 💬 Fetching comments...")
        has_more = 1

        while has_more and len(raw_comments) < self.cfg["max_comments_limit"]:
            async with self.sem_comments:
                try:
                    resp = await self.client.get(
                        "https://www.tiktok.com/api/comment/list/",
                        params={"aweme_id": video_id, "cursor": cursor,
                                "count": 50, "aid": "1988"},
                        headers=self.headers)
                    data = resp.json()
                except:
                    return False

            curr_batch = data.get("comments") or []
            if not curr_batch:
                break

            raw_comments.extend(curr_batch)
            reply_tasks = []
            for c in curr_batch:
                clean_comments.append({
                    "is_reply":    False,
                    "cid":         c.get("cid"),
                    "text":        c.get("text"),
                    "likes":       c.get("digg_count"),
                    "reply_total": c.get("reply_comment_total"),
                    "create_time": c.get("create_time"),
                    "user":        {"username": c.get("user", {}).get("unique_id")}
                })
                if c.get("reply_comment_total", 0) > 0:
                    reply_tasks.append(
                        self.fetch_replies(
                            video_id, c.get("cid"),
                            raw_comments, clean_comments, log_prefix))

            if reply_tasks:
                await asyncio.gather(*reply_tasks)

            has_more = data.get("has_more", 0)
            cursor   = data.get("cursor", cursor + len(curr_batch))

            raw_path.write_text(
                json.dumps(raw_comments,    indent=2, ensure_ascii=False), encoding="utf-8")
            clean_path.write_text(
                json.dumps(clean_comments, indent=2, ensure_ascii=False), encoding="utf-8")

            if len(raw_comments) % 100 < 50:
                logger.info(f"{log_prefix} 💬 Saved {len(raw_comments)} comments so far...")
            await asyncio.sleep(random.uniform(*self.cfg["delay_between_pages"]))

        logger.success(f"{log_prefix} 🎉 Comments Done: {len(raw_comments)}")
        return True

    async def close(self):
        await self.client.aclose()

# ---------------------------------------------------------
# 9. Worker
# ── CHANGE 2 (continued): sub_dir flows through worker_task ──
# ---------------------------------------------------------
async def worker_task(scraper, url, index, total, sem_video, file_lock, sub_dir=""):
    async with sem_video:
        try:
            result = await scraper.scrape_video(url, index, total, file_lock, sub_dir)
            await asyncio.sleep(random.uniform(*CONFIG["delay_between_videos"]))
            return result
        except Exception as e:
            logger.error(f"Worker Error [{url}]: {e}")
            await track_failed(url, f"FAIL:worker_exception — {e}", file_lock)
            return False

# ---------------------------------------------------------
# 10. Main
# ── CORE UPGRADE: links.txt → Input_Links/ folder ──
#
# File naming convention for Input_Links/*.txt:
#   mehranaslam45_posts.txt       → Mega: .../mehranaslam45/posts/
#   mehranaslam45_reposts.txt     → Mega: .../mehranaslam45/reposts/
#   mehranaslam45_video_links.txt → Mega: .../mehranaslam45/playlist/
#   mehranaslam45.txt             → Mega: .../mehranaslam45/
#
# Each task dict: {"url": "https://...", "sub_dir": "accname/category"}
# sub_dir flows → worker_task → scrape_video → upload_to_mega
# ---------------------------------------------------------
async def main():
    INPUT_FOLDER = "Input_Links"

    if not os.path.exists(INPUT_FOLDER):
        logger.error(f"❌ '{INPUT_FOLDER}' folder not found! Create it and add your .txt files.")
        return

    input_files = glob.glob(os.path.join(INPUT_FOLDER, "*.txt"))
    if not input_files:
        logger.error(f"❌ No .txt files found in '{INPUT_FOLDER}' folder.")
        return

    logger.info(f"📂 Found {len(input_files)} file(s) in {INPUT_FOLDER}/")

    # ── Build master task list with sub_dir per URL ──
    all_tasks = []
    for file_path in sorted(input_files):
        file_name = os.path.basename(file_path)
        base_name = file_name.replace(".txt", "")

        # Determine Mega folder structure from filename
        # e.g. "mehranaslam45_posts"     → sub_dir = "mehranaslam45/posts"
        # e.g. "mehranaslam45_reposts"   → sub_dir = "mehranaslam45/reposts"
        # e.g. "mehranaslam45_video_links" → sub_dir = "mehranaslam45/playlist"
        # e.g. "mehranaslam45"           → sub_dir = "mehranaslam45"
        if "_" in base_name:
            acc, cat = base_name.rsplit("_", 1)
            if cat == "video_links":
                cat = "playlist"
            sub_dir = f"{acc}/{cat}"
        else:
            sub_dir = base_name

        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                url = line.strip()
                if url and not url.startswith("Playlist") and not url.startswith("---"):
                    all_tasks.append({"url": url, "sub_dir": sub_dir})

        logger.info(f"   📄 {file_name} → Mega: .../{sub_dir}/")

    if not all_tasks:
        logger.error("❌ No valid URLs found across all input files.")
        return

    logger.info(f"📊 Total URLs collected: {len(all_tasks)}")

    # ── Hard limit ──
    hard_limit = CONFIG.get("hard_link_limit", 1700)
    if len(all_tasks) > hard_limit:
        logger.warning(
            f"⚠️ Total URLs {len(all_tasks)} exceeds hard limit {hard_limit}. "
            f"Truncating to first {hard_limit}."
        )
        all_tasks = all_tasks[:hard_limit]

    # ── Chunking for GitHub Actions matrix / K8s parallel workers ──
    if TOTAL_CHUNKS > 1:
        my_tasks = [t for i, t in enumerate(all_tasks) if i % TOTAL_CHUNKS == CHUNK_INDEX]
        logger.info(
            f"📦 Chunk {CHUNK_INDEX+1}/{TOTAL_CHUNKS}: "
            f"assigned {len(my_tasks)}/{len(all_tasks)} URLs")
    else:
        my_tasks = all_tasks

    done_urls   = load_set_from_file(COMPLETED_FILE)
    failed_urls = load_set_from_file(FAILED_FILE)

    if failed_urls:
        open(FAILED_FILE, "w").close()
        logger.info(f"🔄 Retrying {len(failed_urls)} previously failed URLs.")

    retry_set = failed_urls - done_urls
    new_set   = {t["url"] for t in my_tasks} - done_urls - retry_set

    pending = [t for t in my_tasks if t["url"] in retry_set or t["url"] in new_set]
    skipped = [t for t in my_tasks if t["url"] in done_urls]

    if not pending:
        logger.info("✅ All links already done.")
        await asyncio.to_thread(build_github_artifact)
        return

    logger.info(
        f"🚀 Batch Start | Folder: {BATCH_FOLDER_NAME}\n"
        f"   My URLs        : {len(my_tasks)}\n"
        f"   Done (skip)    : {len(skipped)}\n"
        f"   Retry failed   : {len(retry_set)}\n"
        f"   New            : {len(new_set)}\n"
        f"   Pending        : {len(pending)}\n"
        f"   Concurrency    : {CONFIG['video_concurrency']} videos parallel"
    )

    file_lock = asyncio.Lock()
    for t in skipped:
        await track_skipped(t["url"], "Already completed", file_lock)

    global _upload_sem
    _upload_sem = asyncio.Semaphore(CONFIG["upload_concurrency"])
    sem_video   = asyncio.Semaphore(CONFIG["video_concurrency"])
    scraper     = TikTokScraperV5(CONFIG)

    try:
        tasks = [
            worker_task(scraper, t["url"], i + 1, len(pending), sem_video, file_lock, t["sub_dir"])
            for i, t in enumerate(pending)
        ]
        await asyncio.gather(*tasks)
    finally:
        await scraper.close()

    done_final   = load_set_from_file(COMPLETED_FILE)
    failed_final = load_set_from_file(FAILED_FILE)

    async with file_lock:
        with open(TRACKING_FILE, "a", encoding="utf-8") as f:
            f.write("\n" + "="*60 + "\n")
            f.write(f"RUN COMPLETE  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"CHUNK         : {CHUNK_INDEX+1}/{TOTAL_CHUNKS}\n")
            f.write(f"  Processed   : {len(pending)}\n")
            f.write(f"  Success     : {len(done_final)}\n")
            f.write(f"  Failed      : {len(failed_final)}\n")
            f.write(f"  Skipped     : {len(skipped)}\n")
            f.write("="*60 + "\n")

    logger.success(
        f"\n{'='*50}\n✅ RUN COMPLETE  [Chunk {CHUNK_INDEX+1}/{TOTAL_CHUNKS}]\n"
        f"   Success : {len(done_final)}\n"
        f"   Failed  : {len(failed_final)}\n"
        f"   Skipped : {len(skipped)}\n{'='*50}"
    )

    logger.info("📤 Uploading reports to Mega...")
    await upload_report_files()

    logger.info("📦 Building GitHub artifact ZIP...")
    zip_path = await asyncio.to_thread(build_github_artifact)
    if zip_path:
        logger.success(f"📦 Artifact ready for GitHub Actions upload: {zip_path}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("\n🛑 Stopped by user.")

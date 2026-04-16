"""
磁力爬虫模块 - 三路并行方案

info_hash 发现：
  路径A (DHT):  libtorrent 加入 DHT 网络，通过 alert 发现 info_hash
  路径B (HTTP): 定期从公开种子列表（nyaa/solidtorrents/apibay 等）获取 info_hash

Metadata 获取：
  路径1 (libtorrent): 通过 BEP-9 从 peer 获取 metadata
  路径2 (HTTP 缓存):  通过 itorrents.org 下载 .torrent 文件解析 metadata
  路径3 (API 直取):   部分源（solidtorrents/apibay）直接返回名称和大小

工作流程：
  1. 多源发现 info_hash -> 去重 -> 加入待处理队列
  2. HTTP 缓存优先获取 metadata（速度快、成功率高）
  3. libtorrent 作为补充（需要 UDP 网络）
  4. 获取到 name/size/files 后存入数据库
  5. 获取失败则丢弃，不存入垃圾数据
"""
import os
import io
import json
import time
import random
import hashlib
import logging
import threading
import traceback
from datetime import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

# 尝试导入 libtorrent
try:
    import libtorrent as lt
    HAS_LIBTORRENT = True
    logger.info(f"libtorrent {lt.version} 已加载")
except ImportError:
    HAS_LIBTORRENT = False
    lt = None
    logger.warning("libtorrent 未安装，仅使用 HTTP 模式")


# ==================== HTTP 请求工具 ====================

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

TORRENT_CACHE_URLS = [
    "https://itorrents.org/torrent/{info_hash}.torrent",
    "https://hash2torrent.com/{info_hash_lower}",
]

PUBLIC_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "https://tracker.opentrackr.org:443/announce",
]

_HTTP_SESSION = requests.Session()
_HTTP_SESSION.headers.update(HTTP_HEADERS)
_HTTP_SESSION.mount("https://", HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=1))
_HTTP_SESSION.mount("http://", HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=1))


def _http_get(url, timeout=15):
    """带连接复用的 HTTP GET"""
    try:
        return _HTTP_SESSION.get(url, timeout=timeout)
    except Exception:
        return None


# ==================== Metadata 获取 ====================

def _parse_torrent_payload(data: bytes) -> dict | None:
    """兼容标准 .torrent 文件与仅返回 info 字典的服务"""
    if not data or data[0:1] != b"d":
        return None

    decoded = None
    try:
        decoded = lt.bdecode(data) if HAS_LIBTORRENT else None
    except Exception:
        decoded = None

    if isinstance(decoded, dict) and b"info" in decoded:
        if HAS_LIBTORRENT:
            return _extract_lt_info(lt.torrent_info(decoded))
        return _parse_torrent_bencode(data)

    try:
        info_dict, _ = _bdecode(data, 0)
    except Exception:
        return None
    if not isinstance(info_dict, dict):
        return None

    wrapped = _bencode({b"info": info_dict})
    if HAS_LIBTORRENT:
        try:
            return _extract_lt_info(lt.torrent_info(lt.bdecode(wrapped)))
        except Exception:
            return None
    return _parse_torrent_bencode(wrapped)


def fetch_metadata_from_cache(info_hash_hex: str) -> dict | None:
    """通过 HTTP torrent 缓存服务获取 metadata"""
    ih_upper = info_hash_hex.upper()
    ih_lower = info_hash_hex.lower()
    for url_tpl in TORRENT_CACHE_URLS:
        url = url_tpl.format(info_hash=ih_upper, info_hash_lower=ih_lower)
        try:
            resp = _http_get(url, timeout=15)
            if not resp or resp.status_code != 200:
                continue
            metadata = _parse_torrent_payload(resp.content)
            if metadata and metadata.get("name"):
                return metadata
        except Exception as e:
            logger.debug(f"HTTP 缓存失败 [{info_hash_hex[:12]}]: {e}")
    return None


def _extract_lt_info(ti) -> dict | None:
    """从 libtorrent torrent_info 提取 metadata"""
    name = ti.name()
    if not name or not name.strip():
        return None
    fs = ti.files()
    files_list = []
    for i in range(min(ti.num_files(), 100)):
        try:
            files_list.append({
                "path": fs.file_path(i),
                "size": fs.file_size(i),
            })
        except Exception:
            pass
    return {
        "name": name.strip(),
        "size": ti.total_size(),
        "file_count": ti.num_files(),
        "files": files_list,
    }


# ==================== info_hash 发现源 ====================

import re


def discover_from_nyaa() -> list[dict]:
    """从 nyaa.si RSS 获取最新种子"""
    results = []
    try:
        resp = _http_get("https://nyaa.si/?page=rss", timeout=10)
        if not resp or resp.status_code != 200:
            return results
        text = resp.text
        # 提取 info_hash
        hashes = re.findall(r'[0-9a-fA-F]{40}', text)
        # 提取标题
        titles = re.findall(r'<title><!\[CDATA\[(.+?)\]\]></title>', text)
        # 提取大小
        sizes = re.findall(r'<nyaa:size>(.+?)</nyaa:size>', text)

        seen = set()
        for i, h in enumerate(hashes):
            h_lower = h.lower()
            if h_lower in seen:
                continue
            seen.add(h_lower)
            item = {"info_hash": h_lower}
            if i < len(titles):
                item["name"] = titles[i].strip()
            if i < len(sizes):
                item["size_str"] = sizes[i]
            results.append(item)
    except Exception as e:
        logger.debug(f"nyaa 发现失败: {e}")
    return results


def discover_from_apibay() -> list[dict]:
    """从 apibay.org (TPB API) 获取热门种子"""
    results = []
    endpoints = [
        "https://apibay.org/precompiled/data_top100_recent.json",
        "https://apibay.org/precompiled/data_top100_all.json",
    ]
    seen = set()
    for url in endpoints:
        try:
            resp = _http_get(url, timeout=10)
            if not resp or resp.status_code != 200:
                continue
            data = resp.json()
            for item in data:
                ih = item.get("info_hash", "").lower()
                if not ih or len(ih) != 40 or ih in seen:
                    continue
                seen.add(ih)
                name = item.get("name", "")
                size = int(item.get("size", 0))
                results.append({
                    "info_hash": ih,
                    "name": name if name else None,
                    "size": size if size > 0 else None,
                    "file_count": int(item.get("num_files", 0)) or None,
                })
        except Exception as e:
            logger.debug(f"apibay 发现失败: {e}")
    return results


def discover_from_solidtorrents(keyword: str = "") -> list[dict]:
    """从 solidtorrents API 搜索种子"""
    results = []
    keywords = [keyword] if keyword else [
        "linux", "ubuntu", "python", "movie", "music", "game",
        "software", "book", "anime", "documentary", "series",
    ]
    kw = random.choice(keywords)
    try:
        resp = _http_get(
            f"https://solidtorrents.to/api/v1/search?q={kw}&sort=date",
            timeout=10
        )
        if not resp or resp.status_code != 200:
            return results
        data = resp.json()
        seen = set()
        for item in data.get("results", []):
            ih = item.get("infohash", "").lower()
            if not ih or len(ih) != 40 or ih in seen:
                continue
            seen.add(ih)
            title = item.get("title", "")
            size = item.get("size", 0)
            results.append({
                "info_hash": ih,
                "name": title if title else None,
                "size": size if size else None,
            })
    except Exception as e:
        logger.debug(f"solidtorrents 发现失败: {e}")
    return results


# ==================== Bencode 解析（纯 Python 回退） ====================

def _parse_torrent_bencode(data: bytes) -> dict | None:
    try:
        decoded, _ = _bdecode(data, 0)
        if not isinstance(decoded, dict):
            return None
        info = decoded.get(b"info", {})
        if not info:
            return None
        name = info.get(b"name", b"")
        if isinstance(name, bytes):
            name = _try_decode(name)
        if not name or not name.strip():
            return None
        if b"length" in info:
            total_size = info[b"length"]
            return {
                "name": name.strip(),
                "size": total_size,
                "file_count": 1,
                "files": [{"path": name, "size": total_size}],
            }
        files = info.get(b"files", [])
        total_size = 0
        files_list = []
        for f in files[:100]:
            fsize = f.get(b"length", 0)
            total_size += fsize
            path_parts = f.get(b"path", [])
            path = "/".join(
                _try_decode(p) if isinstance(p, bytes) else str(p)
                for p in path_parts
            )
            files_list.append({"path": path, "size": fsize})
        return {
            "name": name.strip(),
            "size": total_size,
            "file_count": len(files),
            "files": files_list,
        }
    except Exception:
        return None


def _try_decode(data: bytes) -> str:
    for enc in ("utf-8", "gbk", "gb2312", "big5", "shift_jis", "euc-kr", "latin-1"):
        try:
            return data.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return data.hex()


def _bdecode(data: bytes, idx: int):
    if idx >= len(data):
        raise ValueError("Unexpected end")
    ch = data[idx:idx + 1]
    if ch == b"i":
        end = data.index(b"e", idx)
        return int(data[idx + 1:end]), end + 1
    elif ch == b"l":
        result = []
        idx += 1
        while data[idx:idx + 1] != b"e":
            item, idx = _bdecode(data, idx)
            result.append(item)
        return result, idx + 1
    elif ch == b"d":
        result = {}
        idx += 1
        while data[idx:idx + 1] != b"e":
            key, idx = _bdecode(data, idx)
            val, idx = _bdecode(data, idx)
            result[key] = val
        return result, idx + 1
    elif ch and ch.isdigit():
        colon = data.index(b":", idx)
        length = int(data[idx:colon])
        start = colon + 1
        return data[start:start + length], start + length
    else:
        raise ValueError(f"Invalid bencode at {idx}")


def _bencode(value) -> bytes:
    if isinstance(value, int):
        return f"i{value}e".encode()
    if isinstance(value, bytes):
        return str(len(value)).encode() + b":" + value
    if isinstance(value, str):
        encoded = value.encode("utf-8")
        return str(len(encoded)).encode() + b":" + encoded
    if isinstance(value, list):
        return b"l" + b"".join(_bencode(item) for item in value) + b"e"
    if isinstance(value, tuple):
        return b"l" + b"".join(_bencode(item) for item in value) + b"e"
    if isinstance(value, dict):
        items = []
        for key in sorted(value.keys(), key=lambda k: k if isinstance(k, bytes) else str(k).encode("utf-8")):
            key_bytes = key if isinstance(key, bytes) else str(key).encode("utf-8")
            items.append(_bencode(key_bytes))
            items.append(_bencode(value[key]))
        return b"d" + b"".join(items) + b"e"
    raise TypeError(f"Unsupported bencode type: {type(value)!r}")


# ==================== 解析文件大小字符串 ====================

def _parse_size_str(s: str) -> int:
    """解析 '1.2 GiB' 等大小字符串为字节数"""
    if not s:
        return 0
    s = s.strip()
    multipliers = {
        "B": 1, "KB": 1024, "KIB": 1024,
        "MB": 1024**2, "MIB": 1024**2,
        "GB": 1024**3, "GIB": 1024**3,
        "TB": 1024**4, "TIB": 1024**4,
    }
    m = re.match(r"([\d.]+)\s*(\w+)", s)
    if m:
        num = float(m.group(1))
        unit = m.group(2).upper()
        return int(num * multipliers.get(unit, 1))
    return 0


# ==================== DHT 爬虫管理器 ====================

class DHTCrawlerManager:
    """爬虫管理器 - 对外接口: start() / stop() / get_status() / reload_config()"""

    METADATA_TIMEOUT = 120
    MAX_CONCURRENT_LT = 480
    CLEANUP_INTERVAL = 20
    DISCOVER_INTERVAL_MIN = 15
    DISCOVER_INTERVAL_MAX = 120
    PENDING_QUEUE_MAXLEN = 120000

    def __init__(self, db, config):
        self.db = db
        self.config = config
        self._running = False
        self._session = None

        self._stats_lock = threading.Lock()
        self._stats = {
            "status": "stopped",
            "total_discovered": 0,
            "total_saved": 0,
            "total_duplicates": 0,
            "total_metadata_ok": 0,
            "total_metadata_fail": 0,
            "total_filtered": 0,
            "start_time": None,
            "errors": 0,
            "stop_reason": "",
            "db_limit_reached": False,
        }

        self._active_handles = {}
        self._handles_lock = threading.Lock()
        self._known_hashes = set()
        self._known_lock = threading.Lock()
        self._done_hashes = set()
        self._done_lock = threading.Lock()
        self._failed_hashes = set()
        self._failed_lock = threading.Lock()
        self._metadata_attempts = {}
        self._retry_lock = threading.Lock()
        self._pending_queue = deque(maxlen=self.PENDING_QUEUE_MAXLEN)
        self._pending_lock = threading.Lock()
        self._pending_set = set()
        self._metadata_inflight = set()
        self._metadata_lock = threading.Lock()
        self._http_pool = None
        self._http_worker_count = 0

    # ==================== 动态容量计算 ====================

    def _effective_http_workers(self, cpu_limit: int | None = None, max_workers: int | None = None) -> int:
        cpu_limit = int(cpu_limit if cpu_limit is not None else self.config.get("cpu_limit", 50) or 50)
        base_workers = int(max_workers if max_workers is not None else self.config.get("max_workers", 5) or 5)
        if cpu_limit <= 20:
            factor = 1
        elif cpu_limit <= 50:
            factor = 2
        elif cpu_limit <= 80:
            factor = 4
        else:
            factor = 6
        return max(1, min(128, base_workers * factor))

    def _backlog_limit(self, cpu_limit: int | None = None, max_workers: int | None = None) -> int:
        cpu_limit = int(cpu_limit if cpu_limit is not None else self.config.get("cpu_limit", 50) or 50)
        workers = self._effective_http_workers(cpu_limit, max_workers)
        multiplier = 2 if cpu_limit <= 20 else 3 if cpu_limit <= 50 else 5 if cpu_limit <= 80 else 8
        return max(workers, min(2000, workers * multiplier))

    def _lt_limit(self, cpu_limit: int | None = None) -> int:
        cpu_limit = int(cpu_limit if cpu_limit is not None else self.config.get("cpu_limit", 50) or 50)
        return 80 if cpu_limit <= 20 else 160 if cpu_limit <= 50 else 320 if cpu_limit <= 80 else self.MAX_CONCURRENT_LT

    def _session_connections_limit(self, cpu_limit: int | None = None) -> int:
        cpu_limit = int(cpu_limit if cpu_limit is not None else self.config.get("cpu_limit", 50) or 50)
        return 150 if cpu_limit <= 20 else 320 if cpu_limit <= 50 else 650 if cpu_limit <= 80 else 1000

    def _discover_interval(self, cpu_limit: int | None = None) -> float:
        cpu_limit = int(cpu_limit if cpu_limit is not None else self.config.get("cpu_limit", 50) or 50)
        return self.DISCOVER_INTERVAL_MAX if cpu_limit <= 20 else 60 if cpu_limit <= 50 else 30 if cpu_limit <= 80 else self.DISCOVER_INTERVAL_MIN

    def _metadata_dispatch_delay(self, cpu_limit: int | None = None) -> float:
        cpu_limit = int(cpu_limit if cpu_limit is not None else self.config.get("cpu_limit", 50) or 50)
        return 0.15 if cpu_limit <= 20 else 0.05 if cpu_limit <= 50 else 0.015 if cpu_limit <= 80 else 0.003

    def _dht_burst_count(self, cpu_limit: int | None = None) -> int:
        cpu_limit = int(cpu_limit if cpu_limit is not None else self.config.get("cpu_limit", 50) or 50)
        return 3 if cpu_limit <= 20 else 8 if cpu_limit <= 50 else 16 if cpu_limit <= 80 else 24

    def _dht_idle_delay(self, cpu_limit: int | None = None) -> float:
        cpu_limit = int(cpu_limit if cpu_limit is not None else self.config.get("cpu_limit", 50) or 50)
        return 10 if cpu_limit <= 20 else 4 if cpu_limit <= 50 else 1.5 if cpu_limit <= 80 else 0.6

    # ==================== 生命周期 ====================

    def start(self):
        if self._running:
            return
        self._running = True
        cpu_limit = int(self.config.get("cpu_limit", 50) or 50)
        max_workers = int(self.config.get("max_workers", 5) or 5)
        dht_port = int(self.config.get("dht_port", 6881) or 6881)
        effective_workers = self._effective_http_workers(cpu_limit, max_workers)

        self._http_worker_count = effective_workers
        self._http_pool = ThreadPoolExecutor(
            max_workers=effective_workers,
            thread_name_prefix="meta-worker"
        )

        if HAS_LIBTORRENT:
            self._create_session(dht_port, cpu_limit)

        with self._stats_lock:
            self._stats.update({
                "status": "running",
                "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_discovered": 0,
                "total_saved": 0,
                "total_duplicates": 0,
                "total_metadata_ok": 0,
                "total_metadata_fail": 0,
                "total_filtered": 0,
                "errors": 0,
                "stop_reason": "",
                "db_limit_reached": False,
            })

        threading.Thread(target=self._http_discover_loop, daemon=True, name="http-discover").start()
        threading.Thread(target=self._metadata_dispatch_loop, daemon=True, name="meta-dispatch").start()

        if HAS_LIBTORRENT:
            threading.Thread(target=self._alert_loop, daemon=True, name="lt-alert").start()
            threading.Thread(target=self._lt_dispatch_loop, daemon=True, name="lt-dispatch").start()
            threading.Thread(target=self._cleanup_loop, daemon=True, name="lt-cleanup").start()
            threading.Thread(target=self._dht_sample_loop, daemon=True, name="dht-sample").start()

        logger.info(
            f"爬虫已启动 (端口: {dht_port}, CPU限制: {cpu_limit}%, 配置线程: {max_workers}, "
            f"实际HTTP线程: {effective_workers}, libtorrent: {HAS_LIBTORRENT})"
        )

    def _create_session(self, port, cpu_limit):
        lt_limit = self._lt_limit(cpu_limit)
        settings = {
            "listen_interfaces": f"0.0.0.0:{port}",
            "alert_mask": (
                lt.alert.category_t.dht_notification |
                lt.alert.category_t.status_notification |
                lt.alert.category_t.error_notification
            ),
            "enable_dht": True,
            "enable_lsd": True,
            "enable_upnp": True,
            "enable_natpmp": True,
            "dht_bootstrap_nodes": (
                "router.bittorrent.com:6881,"
                "dht.transmissionbt.com:6881,"
                "router.utorrent.com:6881,"
                "router.bitcomet.com:6881"
            ),
            "download_rate_limit": 2 * 1024 * 1024,
            "upload_rate_limit": 1024 * 1024,
            "connections_limit": self._session_connections_limit(cpu_limit),
            "active_downloads": lt_limit,
            "active_seeds": 0,
            "active_limit": lt_limit + 120,
            "anonymous_mode": True,
        }
        self._session = lt.session(settings)
        logger.info("libtorrent session 已创建")

    def stop(self):
        self._running = False
        if self._session:
            try:
                with self._handles_lock:
                    for _, (handle, _) in list(self._active_handles.items()):
                        try:
                            self._session.remove_torrent(handle)
                        except Exception:
                            pass
                    self._active_handles.clear()
                self._session.pause()
                self._session = None
            except Exception as e:
                logger.error(f"停止 session 出错: {e}")
        if self._http_pool:
            self._http_pool.shutdown(wait=False)
            self._http_pool = None
        self._http_worker_count = 0
        with self._metadata_lock:
            self._metadata_inflight.clear()
        with self._pending_lock:
            self._pending_queue.clear()
            self._pending_set.clear()
        with self._failed_lock:
            self._failed_hashes.clear()
        with self._retry_lock:
            self._metadata_attempts.clear()
        with self._stats_lock:
            self._stats["status"] = "stopped"
        logger.info("爬虫已停止")

    def _stop_due_to_db_limit(self, size_limit_gb: float):
        with self._stats_lock:
            if self._stats.get("db_limit_reached"):
                return
            current_size = self.db.get_db_size_bytes()
            self._stats["db_limit_reached"] = True
            self._stats["stop_reason"] = (
                f"数据库大小已达到上限 {float(size_limit_gb):.2f} GB，当前约 {self._human_size(current_size)}，爬虫已自动停止"
            )
        logger.warning(self._stats["stop_reason"])
        self.stop()

    def reload_config(self):
        try:
            cpu_limit = int(self.config.get("cpu_limit", 50) or 50)
            max_workers = int(self.config.get("max_workers", 5) or 5)
            effective_workers = self._effective_http_workers(cpu_limit, max_workers)

            if self._session:
                lt_limit = self._lt_limit(cpu_limit)
                self._session.apply_settings({
                    "connections_limit": self._session_connections_limit(cpu_limit),
                    "active_downloads": lt_limit,
                    "active_limit": lt_limit + 120,
                })

            if self._running and effective_workers != self._http_worker_count:
                old_pool = self._http_pool
                self._http_worker_count = effective_workers
                self._http_pool = ThreadPoolExecutor(
                    max_workers=effective_workers,
                    thread_name_prefix="meta-worker"
                )
                if old_pool:
                    old_pool.shutdown(wait=False)
        except Exception as e:
            logger.error(f"重载配置出错: {e}")

    def get_status(self) -> dict:
        with self._stats_lock:
            stats = dict(self._stats)
        cpu_limit = int(self.config.get("cpu_limit", 50) or 50)
        max_workers = int(self.config.get("max_workers", 5) or 5)
        stats["cpu_limit"] = cpu_limit
        stats["max_workers"] = max_workers
        stats["effective_workers"] = self._effective_http_workers(cpu_limit, max_workers)
        stats["db_size_limit_gb"] = float(self.config.get("db_size_limit_gb", 0) or 0)
        with self._handles_lock:
            lt_active = len(self._active_handles)
        with self._metadata_lock:
            http_inflight = len(self._metadata_inflight)
        pending_len = self._pending_len()
        stats["queue_size"] = lt_active + http_inflight + pending_len
        stats["http_inflight"] = http_inflight
        stats["lt_active"] = lt_active
        stats["pending_queue"] = pending_len
        if self._session:
            try:
                ss = self._session.status()
                stats["dht_nodes"] = ss.dht_nodes
                stats["dht_torrents"] = ss.dht_torrents
            except Exception:
                stats["dht_nodes"] = 0
                stats["dht_torrents"] = 0
        else:
            stats["dht_nodes"] = 0
            stats["dht_torrents"] = 0
        stats["has_libtorrent"] = HAS_LIBTORRENT
        return stats

    # ==================== 队列与重试 ====================

    def _pending_len(self) -> int:
        with self._pending_lock:
            return len(self._pending_queue)

    def _enqueue_pending(self, info_hash_hex: str) -> bool:
        with self._pending_lock, self._metadata_lock:
            if info_hash_hex in self._pending_set or info_hash_hex in self._metadata_inflight:
                return False
            if len(self._pending_queue) >= self._pending_queue.maxlen:
                dropped = self._pending_queue.popleft()
                self._pending_set.discard(dropped)
            self._pending_queue.append(info_hash_hex)
            self._pending_set.add(info_hash_hex)
            return True

    def _dequeue_pending(self) -> str | None:
        with self._pending_lock:
            if not self._pending_queue:
                return None
            info_hash_hex = self._pending_queue.popleft()
            self._pending_set.discard(info_hash_hex)
            return info_hash_hex

    def _peek_pending(self) -> str | None:
        with self._pending_lock:
            if not self._pending_queue:
                return None
            return self._pending_queue[0]

    def _try_acquire_metadata_slot(self, info_hash_hex: str) -> bool:
        with self._metadata_lock:
            if info_hash_hex in self._metadata_inflight:
                return False
            self._metadata_inflight.add(info_hash_hex)
            return True

    def _release_metadata_slot(self, info_hash_hex: str):
        with self._metadata_lock:
            self._metadata_inflight.discard(info_hash_hex)

    def _register_metadata_attempt(self, info_hash_hex: str) -> int:
        with self._retry_lock:
            attempt = self._metadata_attempts.get(info_hash_hex, 0) + 1
            self._metadata_attempts[info_hash_hex] = attempt
            return attempt

    def _clear_metadata_attempts(self, info_hash_hex: str):
        with self._retry_lock:
            self._metadata_attempts.pop(info_hash_hex, None)

    def _mark_failed(self, info_hash_hex: str, reason: str = ""):
        with self._done_lock:
            if info_hash_hex in self._done_hashes:
                return
        with self._failed_lock:
            if info_hash_hex in self._failed_hashes:
                return
            self._failed_hashes.add(info_hash_hex)
        self._clear_metadata_attempts(info_hash_hex)
        with self._stats_lock:
            self._stats["total_metadata_fail"] += 1
        if reason:
            logger.debug(f"metadata 最终失败 [{info_hash_hex[:12]}]: {reason}")

    # ==================== 过滤与容量控制 ====================

    def _get_excluded_keywords(self) -> list[str]:
        raw = self.config.get("save_filter_keywords", "") or ""
        return [item.strip().lower() for item in re.split(r"[\n,，;；]+", str(raw)) if item.strip()]

    def _metadata_should_be_filtered(self, metadata: dict) -> tuple[bool, str]:
        size = int(metadata.get("size", 0) or 0)
        name = (metadata.get("name") or "").strip()
        files = metadata.get("files") or []
        haystack = "\n".join([name] + [str(item.get("path") or "") for item in files]).lower()

        keywords = self._get_excluded_keywords()
        for keyword in keywords:
            if keyword and keyword in haystack:
                return True, f"命中过滤关键词: {keyword}"

        try:
            min_size_mb = float(self.config.get("save_filter_min_size_mb", 0) or 0)
        except Exception:
            min_size_mb = 0
        try:
            max_size_gb = float(self.config.get("save_filter_max_size_gb", 0) or 0)
        except Exception:
            max_size_gb = 0

        if size > 0:
            min_bytes = int(min_size_mb * 1024 * 1024)
            max_bytes = int(max_size_gb * 1024 * 1024 * 1024)
            if min_bytes > 0 and size < min_bytes:
                return True, f"文件大小低于最小限制: {self._human_size(size)}"
            if max_bytes > 0 and size > max_bytes:
                return True, f"文件大小高于最大限制: {self._human_size(size)}"
        return False, ""

    def _check_db_size_limit(self) -> bool:
        try:
            size_limit_gb = float(self.config.get("db_size_limit_gb", 0) or 0)
        except Exception:
            size_limit_gb = 0
        if size_limit_gb <= 0:
            return False
        if self.db.is_size_limit_reached(size_limit_gb):
            self._stop_due_to_db_limit(size_limit_gb)
            return True
        return False

    # ==================== info_hash 管理 ====================

    def _add_info_hash(self, info_hash_hex: str, pre_metadata: dict = None):
        if not info_hash_hex or len(info_hash_hex) != 40:
            return
        info_hash_hex = info_hash_hex.lower()

        with self._known_lock:
            if info_hash_hex in self._known_hashes:
                return
            self._known_hashes.add(info_hash_hex)
            if len(self._known_hashes) > 500000:
                self._known_hashes = set(list(self._known_hashes)[-250000:])

        with self._done_lock:
            if info_hash_hex in self._done_hashes:
                self.db.insert_magnet(info_hash_hex, "")
                with self._stats_lock:
                    self._stats["total_duplicates"] += 1
                return

        existing = self.db.get_by_hash(info_hash_hex)
        if existing and existing.get("name") and not existing["name"].startswith("Unknown"):
            with self._done_lock:
                self._done_hashes.add(info_hash_hex)
            self.db.insert_magnet(info_hash_hex, "")
            with self._stats_lock:
                self._stats["total_duplicates"] += 1
            return

        with self._stats_lock:
            self._stats["total_discovered"] += 1

        if pre_metadata and pre_metadata.get("name") and pre_metadata.get("size"):
            self._mark_done(info_hash_hex, pre_metadata, "API")
            return
        self._enqueue_pending(info_hash_hex)

    def _mark_done(self, info_hash_hex: str, metadata: dict, source: str):
        with self._done_lock:
            if info_hash_hex in self._done_hashes:
                return
            self._done_hashes.add(info_hash_hex)
            if len(self._done_hashes) > 500000:
                self._done_hashes = set(list(self._done_hashes)[-250000:])

        with self._failed_lock:
            self._failed_hashes.discard(info_hash_hex)
        self._clear_metadata_attempts(info_hash_hex)

        filtered, reason = self._metadata_should_be_filtered(metadata)
        if filtered:
            with self._stats_lock:
                self._stats["total_filtered"] += 1
            logger.info(f"[{source}] 已过滤 {info_hash_hex[:12]}: {reason}")
            return

        if self._check_db_size_limit():
            return

        files_json = json.dumps(metadata.get("files", [])[:100], ensure_ascii=False)
        is_new = self.db.insert_magnet(
            info_hash_hex,
            metadata["name"],
            metadata.get("size", 0),
            metadata.get("file_count", 0),
            files_json,
        )

        with self._stats_lock:
            self._stats["total_metadata_ok"] += 1
            if is_new:
                self._stats["total_saved"] += 1
            else:
                self._stats["total_duplicates"] += 1

        logger.info(
            f"[{source}] {metadata['name'][:80]} | "
            f"{self._human_size(metadata.get('size', 0))} | "
            f"{metadata.get('file_count', 0)} 文件"
        )

        self._check_db_size_limit()

    # ==================== HTTP 种子发现循环 ====================

    def _http_discover_loop(self):
        discover_funcs = [
            ("nyaa", discover_from_nyaa),
            ("apibay", discover_from_apibay),
            ("solidtorrents", discover_from_solidtorrents),
        ]
        func_idx = 0

        time.sleep(2)
        for name, func in discover_funcs:
            if not self._running:
                break
            try:
                items = func()
                count = 0
                for item in items:
                    ih = item.get("info_hash", "")
                    pre_meta = None
                    if item.get("name") and item.get("size"):
                        pre_meta = {
                            "name": item["name"],
                            "size": item["size"],
                            "file_count": item.get("file_count", 0),
                            "files": [],
                        }
                    self._add_info_hash(ih, pre_meta)
                    count += 1
                logger.info(f"[{name}] 发现 {count} 个 info_hash")
            except Exception as e:
                logger.error(f"[{name}] 发现出错: {e}")
            time.sleep(0.6)

        while self._running:
            try:
                time.sleep(self._discover_interval())
                if not self._running:
                    break
                if self._check_db_size_limit():
                    break

                name, func = discover_funcs[func_idx % len(discover_funcs)]
                func_idx += 1
                items = func()
                count = 0
                for item in items:
                    ih = item.get("info_hash", "")
                    pre_meta = None
                    if item.get("name") and item.get("size"):
                        pre_meta = {
                            "name": item["name"],
                            "size": item["size"],
                            "file_count": item.get("file_count", 0),
                            "files": [],
                        }
                    self._add_info_hash(ih, pre_meta)
                    count += 1
                if count > 0:
                    logger.info(f"[{name}] 轮询发现 {count} 个 info_hash")
            except Exception as e:
                logger.error(f"HTTP 发现循环出错: {e}")
                time.sleep(5)

    # ==================== Metadata 获取循环 ====================

    def _metadata_dispatch_loop(self):
        while self._running:
            try:
                if self._check_db_size_limit():
                    break
                if not self._http_pool:
                    time.sleep(0.1)
                    continue

                backlog_limit = self._backlog_limit()
                with self._metadata_lock:
                    inflight = len(self._metadata_inflight)
                available_slots = max(0, backlog_limit - inflight)
                if available_slots <= 0:
                    time.sleep(0.02)
                    continue

                dispatched = 0
                for _ in range(available_slots):
                    info_hash_hex = self._dequeue_pending()
                    if not info_hash_hex:
                        break
                    with self._done_lock:
                        if info_hash_hex in self._done_hashes:
                            continue
                    if not self._try_acquire_metadata_slot(info_hash_hex):
                        continue
                    self._http_pool.submit(self._fetch_metadata, info_hash_hex)
                    dispatched += 1

                if dispatched == 0:
                    time.sleep(0.05)
                else:
                    time.sleep(self._metadata_dispatch_delay())
            except Exception as e:
                logger.error(f"metadata dispatch 出错: {e}")
                time.sleep(0.1)

    def _fetch_metadata(self, info_hash_hex: str):
        should_retry = False
        if not self._running:
            self._release_metadata_slot(info_hash_hex)
            return
        with self._done_lock:
            if info_hash_hex in self._done_hashes:
                self._release_metadata_slot(info_hash_hex)
                return
        attempt = 0
        try:
            attempt = self._register_metadata_attempt(info_hash_hex)
            metadata = fetch_metadata_from_cache(info_hash_hex)
            if metadata and metadata.get("name"):
                self._mark_done(info_hash_hex, metadata, "HTTP")
                self._remove_handle(info_hash_hex)
            elif self._running and HAS_LIBTORRENT and self._session:
                with self._handles_lock:
                    already_active = info_hash_hex in self._active_handles
                if not already_active:
                    self._add_to_lt_session(info_hash_hex)
            elif attempt < 3:
                should_retry = True
            else:
                self._mark_failed(info_hash_hex, "HTTP 缓存多次失败")
        except Exception as e:
            logger.debug(f"metadata 获取失败 [{info_hash_hex[:12]}]: {e}")
            with self._stats_lock:
                self._stats["errors"] += 1
            if attempt < 3:
                should_retry = True
            else:
                self._mark_failed(info_hash_hex, "HTTP 请求异常达到上限")
        finally:
            self._release_metadata_slot(info_hash_hex)
            if should_retry and self._running:
                self._enqueue_pending(info_hash_hex)

    # ==================== libtorrent DHT 路径 ====================

    def _alert_loop(self):
        while self._running:
            if not self._session:
                time.sleep(1)
                continue
            try:
                alerts = self._session.pop_alerts()
                for alert in alerts:
                    atype = type(alert).__name__
                    if atype == "metadata_received_alert":
                        self._on_lt_metadata(alert)
                    elif atype in ("dht_get_peers_alert", "dht_announce_alert"):
                        try:
                            ih_hex = str(alert.info_hash)
                            if len(ih_hex) == 40:
                                self._add_info_hash(ih_hex)
                        except Exception:
                            pass
                if not alerts:
                    time.sleep(0.05)
            except Exception as e:
                logger.error(f"alert loop 出错: {e}")
                time.sleep(1)

    def _on_lt_metadata(self, alert):
        try:
            handle = alert.handle
            ti = handle.torrent_file()
            if not ti:
                return
            ih_hex = str(ti.info_hashes().v1)
            metadata = _extract_lt_info(ti)
            if metadata:
                self._mark_done(ih_hex, metadata, "DHT")
            self._remove_handle(ih_hex, handle)
        except Exception as e:
            logger.error(f"lt metadata 处理出错: {e}")

    def _lt_dispatch_loop(self):
        while self._running:
            try:
                if self._check_db_size_limit():
                    break
                with self._handles_lock:
                    active = len(self._active_handles)
                if active >= self._lt_limit() or self._pending_len() == 0:
                    time.sleep(0.05)
                    continue

                ih = self._peek_pending()
                if not ih:
                    time.sleep(0.02)
                    continue

                with self._done_lock:
                    if ih in self._done_hashes:
                        continue
                with self._handles_lock:
                    if ih in self._active_handles:
                        continue

                self._add_to_lt_session(ih)
                time.sleep(self._metadata_dispatch_delay())
            except Exception as e:
                logger.error(f"lt dispatch 出错: {e}")
                time.sleep(0.2)

    def _add_to_lt_session(self, info_hash_hex: str):
        if not self._session:
            return
        try:
            trackers = "&".join(f"tr={requests.utils.quote(tr, safe='')}" for tr in PUBLIC_TRACKERS)
            magnet = f"magnet:?xt=urn:btih:{info_hash_hex}&{trackers}"
            params = lt.parse_magnet_uri(magnet)
            params.save_path = "/tmp/magnet_metadata"
            params.flags |= lt.torrent_flags.upload_mode
            params.flags &= ~lt.torrent_flags.auto_managed
            handle = self._session.add_torrent(params)
            handle.resume()
            with self._handles_lock:
                self._active_handles[info_hash_hex] = (handle, time.time())
        except Exception as e:
            if "duplicate torrent" not in str(e).lower():
                logger.debug(f"lt 添加失败: {e}")

    def _remove_handle(self, ih_hex: str, handle=None):
        with self._handles_lock:
            stored = self._active_handles.pop(ih_hex, None)
            if stored and not handle:
                handle = stored[0]
        if handle and self._session:
            try:
                self._session.remove_torrent(handle)
            except Exception:
                pass

    def _cleanup_loop(self):
        while self._running:
            try:
                time.sleep(self.CLEANUP_INTERVAL)
                now = time.time()
                to_remove = []
                with self._handles_lock:
                    for ih, (handle, started_at) in list(self._active_handles.items()):
                        if now - started_at > self.METADATA_TIMEOUT:
                            to_remove.append((ih, handle))
                for ih, handle in to_remove:
                    self._remove_handle(ih, handle)
                    self._mark_failed(ih, "DHT metadata 超时")
                if to_remove:
                    logger.debug(f"清理 {len(to_remove)} 个超时任务")
            except Exception as e:
                logger.error(f"cleanup 出错: {e}")

    def _dht_sample_loop(self):
        while self._running:
            try:
                if not self._session:
                    time.sleep(5)
                    continue
                try:
                    ss = self._session.status()
                    if ss.dht_nodes < 5:
                        time.sleep(2)
                        continue
                except Exception:
                    time.sleep(2)
                    continue
                for _ in range(self._dht_burst_count()):
                    if not self._running:
                        break
                    rand_hash = hashlib.sha1(random.randbytes(20)).digest()
                    try:
                        self._session.dht_get_peers(lt.sha1_hash(rand_hash))
                    except Exception:
                        pass
                    time.sleep(0.08)
                time.sleep(self._dht_idle_delay())
            except Exception as e:
                logger.error(f"DHT sample 出错: {e}")
                time.sleep(3)

    # ==================== 工具 ====================

    @staticmethod
    def _human_size(size: int) -> str:
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"

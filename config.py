"""
配置管理模块 - 支持从文件读取、动态修改和敏感字段保护
"""
import os
import json
import hashlib
import threading
import logging

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
DEFAULT_SQLITE_PATH = os.environ.get("DB_PATH", os.path.join(DEFAULT_DATA_DIR, "magnet.db"))

DEFAULT_CONFIG = {
    "admin_password": "admin123",
    "secret_key": "magnet-search-secret-key-2024",
    "cpu_limit": 50,
    "max_workers": 5,
    "crawler_auto_start": False,
    "dht_port": 6881,
    "max_magnets": 1000000,
    "db_backend": os.environ.get("DB_BACKEND", "sqlite"),
    "db_sqlite_path": DEFAULT_SQLITE_PATH,
    "db_mysql_host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
    "db_mysql_port": int(os.environ.get("MYSQL_PORT", "3306")),
    "db_mysql_user": os.environ.get("MYSQL_USER", "root"),
    "db_mysql_password": os.environ.get("MYSQL_PASSWORD", ""),
    "db_mysql_database": os.environ.get("MYSQL_DATABASE", "magnet_search"),
    "db_mysql_charset": os.environ.get("MYSQL_CHARSET", "utf8mb4"),
    "db_size_limit_gb": 0,
    "save_filter_keywords": "",
    "save_filter_min_size_mb": 0,
    "save_filter_max_size_gb": 0,
    "theme_mode": "dark",
    "theme_color": "#4f46e5",
    "qbittorrent_enabled": False,
    "qbittorrent_url": "http://127.0.0.1:8080",
    "qbittorrent_username": "admin",
    "qbittorrent_password": "",
    "qbittorrent_save_path": "",
    "qbittorrent_category": "",
    "qbittorrent_tags": "",
    "qbittorrent_paused": False,
    "qbittorrent_auto_tmm": False,
}

CONFIG_PATH = os.environ.get("CONFIG_PATH", os.path.join(DEFAULT_DATA_DIR, "config.json"))
SENSITIVE_KEYS = {
    "admin_password",
    "secret_key",
    "qbittorrent_password",
    "db_mysql_password",
}


class Config:
    """线程安全的配置管理器"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._data = dict(DEFAULT_CONFIG)
        self._file_lock = threading.Lock()
        self._load()

    def _load(self):
        """从配置文件加载"""
        try:
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    file_data = json.load(f)
                    self._data.update(file_data)
                logger.info(f"配置已从 {CONFIG_PATH} 加载")
            else:
                self._data["admin_password"] = hashlib.sha256(
                    self._data["admin_password"].encode()
                ).hexdigest()
                self._save()
                logger.info(f"已创建默认配置文件: {CONFIG_PATH}")

            admin_password = self._data.get("admin_password", "")
            if admin_password and len(admin_password) != 64:
                self._data["admin_password"] = hashlib.sha256(admin_password.encode()).hexdigest()
                self._save()
        except Exception as e:
            logger.error(f"加载配置失败: {e}")

    def _save(self):
        """保存配置到文件"""
        with self._file_lock:
            try:
                os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
                with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                    json.dump(self._data, f, indent=2, ensure_ascii=False)
            except Exception as e:
                logger.error(f"保存配置失败: {e}")

    def get(self, key: str, default=None):
        return self._data.get(key, default)

    def set(self, key: str, value):
        self._data[key] = value
        self._save()

    def update(self, values: dict):
        self._data.update(values)
        self._save()

    def get_all(self) -> dict:
        safe = dict(self._data)
        for key in SENSITIVE_KEYS:
            safe.pop(key, None)
        return safe

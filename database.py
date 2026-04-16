"""
数据库模块 - 支持 SQLite / MySQL 双后端
SQLite 使用 FTS5 trigram + LIKE 双引擎；MySQL 使用 LIKE 多关键词搜索。
"""
import os
import re
import json
import sqlite3
import threading
import logging
from typing import Any

logger = logging.getLogger(__name__)

try:
    import pymysql
    from pymysql.cursors import DictCursor
    HAS_PYMYSQL = True
except Exception:
    pymysql = None
    DictCursor = None
    HAS_PYMYSQL = False


CJK_RANGES = re.compile(
    r'[\u2e80-\u9fff'
    r'\u3000-\u303f'
    r'\u3040-\u309f'
    r'\u30a0-\u30ff'
    r'\uff00-\uffef'
    r'\uac00-\ud7af'
    r']'
)


def has_cjk(text: str) -> bool:
    return bool(CJK_RANGES.search(text or ""))


class DatabaseError(Exception):
    """数据库操作异常"""


class Database:
    """线程安全的数据库管理器，支持运行时切换后端。"""

    def __init__(self, config):
        self.config = config
        self._lock = threading.Lock()
        self._local = threading.local()
        self._backend = None
        self._settings = {}
        self._load_settings_from_config()

    # ==================== 配置与连接 ====================

    def _normalize_backend(self, backend: str | None) -> str:
        backend = (backend or "sqlite").strip().lower()
        return "mysql" if backend == "mysql" else "sqlite"

    def _load_settings_from_config(self):
        self._settings = {
            "backend": self._normalize_backend(self.config.get("db_backend", "sqlite")),
            "sqlite_path": (self.config.get("db_sqlite_path", "") or "").strip(),
            "mysql_host": (self.config.get("db_mysql_host", "127.0.0.1") or "127.0.0.1").strip(),
            "mysql_port": int(self.config.get("db_mysql_port", 3306) or 3306),
            "mysql_user": (self.config.get("db_mysql_user", "root") or "root").strip(),
            "mysql_password": self.config.get("db_mysql_password", "") or "",
            "mysql_database": (self.config.get("db_mysql_database", "magnet_search") or "magnet_search").strip(),
            "mysql_charset": (self.config.get("db_mysql_charset", "utf8mb4") or "utf8mb4").strip(),
        }
        if not self._settings["sqlite_path"]:
            self._settings["sqlite_path"] = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "data", "magnet.db"
            )
        self._backend = self._settings["backend"]

    def _close_local_conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        self._local.conn = None

    def _ensure_connection_alive(self, conn):
        if self._backend == "mysql":
            conn.ping(reconnect=True)
        return conn

    def _connect_sqlite(self):
        db_path = self._settings["sqlite_path"]
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")
        return conn

    def _connect_mysql(self):
        if not HAS_PYMYSQL:
            raise DatabaseError("当前环境未安装 PyMySQL，无法使用 MySQL")
        return pymysql.connect(
            host=self._settings["mysql_host"],
            port=int(self._settings["mysql_port"]),
            user=self._settings["mysql_user"],
            password=self._settings["mysql_password"],
            database=self._settings["mysql_database"],
            charset=self._settings["mysql_charset"],
            cursorclass=DictCursor,
            autocommit=False,
            connect_timeout=5,
            read_timeout=10,
            write_timeout=10,
        )

    def _get_conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect_mysql() if self._backend == "mysql" else self._connect_sqlite()
            self._local.conn = conn
        return self._ensure_connection_alive(conn)

    def _make_temp_connection(self, settings: dict):
        backend = self._normalize_backend(settings.get("backend"))
        if backend == "mysql":
            if not HAS_PYMYSQL:
                raise DatabaseError("当前环境未安装 PyMySQL，无法使用 MySQL")
            return pymysql.connect(
                host=settings["mysql_host"],
                port=int(settings["mysql_port"]),
                user=settings["mysql_user"],
                password=settings["mysql_password"],
                database=settings["mysql_database"],
                charset=settings.get("mysql_charset", "utf8mb4"),
                cursorclass=DictCursor,
                autocommit=False,
                connect_timeout=5,
                read_timeout=10,
                write_timeout=10,
            )
        db_path = settings["sqlite_path"]
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        return sqlite3.connect(db_path, timeout=10, check_same_thread=False)

    def _normalize_external_settings(self, settings: dict | None, include_password=True) -> dict:
        settings = settings or {}
        normalized = {
            "backend": self._normalize_backend(settings.get("backend", self._settings.get("backend", "sqlite"))),
            "sqlite_path": (settings.get("sqlite_path", self._settings.get("sqlite_path", "")) or "").strip(),
            "mysql_host": (settings.get("mysql_host", self._settings.get("mysql_host", "127.0.0.1")) or "127.0.0.1").strip(),
            "mysql_port": int(settings.get("mysql_port", self._settings.get("mysql_port", 3306)) or 3306),
            "mysql_user": (settings.get("mysql_user", self._settings.get("mysql_user", "root")) or "root").strip(),
            "mysql_database": (settings.get("mysql_database", self._settings.get("mysql_database", "magnet_search")) or "magnet_search").strip(),
            "mysql_charset": (settings.get("mysql_charset", self._settings.get("mysql_charset", "utf8mb4")) or "utf8mb4").strip(),
        }
        if include_password:
            normalized["mysql_password"] = settings.get("mysql_password", self._settings.get("mysql_password", "")) or ""
        else:
            normalized["mysql_password"] = ""

        if not normalized["sqlite_path"]:
            normalized["sqlite_path"] = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "data", "magnet.db"
            )
        return normalized

    def get_config_view(self) -> dict:
        cfg = self._normalize_external_settings(self._settings)
        return {
            "backend": cfg["backend"],
            "sqlite_path": cfg["sqlite_path"],
            "mysql_host": cfg["mysql_host"],
            "mysql_port": cfg["mysql_port"],
            "mysql_user": cfg["mysql_user"],
            "mysql_database": cfg["mysql_database"],
            "mysql_charset": cfg["mysql_charset"],
            "mysql_password_set": bool(cfg.get("mysql_password")),
            "available_mysql": HAS_PYMYSQL,
        }

    def test_settings(self, settings: dict | None = None) -> dict:
        cfg = self._normalize_external_settings(settings)
        conn = None
        try:
            conn = self._make_temp_connection(cfg)
            if cfg["backend"] == "mysql":
                with conn.cursor() as cursor:
                    cursor.execute("SELECT VERSION() AS version")
                    row = cursor.fetchone() or {}
                version = row.get("version", "unknown")
                return {
                    "backend": "mysql",
                    "message": f"MySQL 连接成功，版本：{version}",
                    "version": version,
                }

            cursor = conn.cursor()
            cursor.execute("SELECT sqlite_version()")
            row = cursor.fetchone()
            version = row[0] if row else "unknown"
            cursor.close()
            return {
                "backend": "sqlite",
                "message": f"内置 SQLite 可用，版本：{version}",
                "version": version,
            }
        except Exception as exc:
            raise DatabaseError(str(exc)) from exc
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def switch_backend(self, settings: dict | None = None):
        cfg = self._normalize_external_settings(settings)
        self.test_settings(cfg)
        with self._lock:
            self._close_local_conn()
            self._settings = cfg
            self._backend = cfg["backend"]
            self.init()

    def get_runtime_info(self) -> dict:
        size_bytes = self.get_db_size_bytes()
        return {
            "backend": self._backend,
            "size_bytes": size_bytes,
            "size_human": self._human_size(size_bytes),
        }

    # ==================== 初始化 ====================

    def init(self):
        conn = self._get_conn()
        if self._backend == "mysql":
            self._init_mysql(conn)
        else:
            self._init_sqlite(conn)
        logger.info(f"数据库初始化完成，当前后端: {self._backend}")

    def _init_sqlite(self, conn):
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS magnets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                info_hash TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL DEFAULT '',
                name_lower TEXT NOT NULL DEFAULT '',
                size INTEGER DEFAULT 0,
                file_count INTEGER DEFAULT 0,
                files TEXT DEFAULT '[]',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                hot INTEGER DEFAULT 1
            );

            CREATE INDEX IF NOT EXISTS idx_magnets_info_hash ON magnets(info_hash);
            CREATE INDEX IF NOT EXISTS idx_magnets_created_at ON magnets(created_at);
            CREATE INDEX IF NOT EXISTS idx_magnets_size ON magnets(size);
            CREATE INDEX IF NOT EXISTS idx_magnets_hot ON magnets(hot);
            CREATE INDEX IF NOT EXISTS idx_magnets_name_lower ON magnets(name_lower);
            """
        )

        cols = [row[1] for row in conn.execute("PRAGMA table_info(magnets)").fetchall()]
        if "name_lower" not in cols:
            conn.execute("ALTER TABLE magnets ADD COLUMN name_lower TEXT NOT NULL DEFAULT ''")
            conn.execute("UPDATE magnets SET name_lower = LOWER(name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_magnets_name_lower ON magnets(name_lower)")
            conn.commit()

        try:
            conn.execute("SELECT COUNT(*) FROM magnets_fts")
            schema = conn.execute(
                "SELECT sql FROM sqlite_master WHERE name='magnets_fts'"
            ).fetchone()
            if schema and "trigram" not in schema[0]:
                conn.executescript(
                    """
                    DROP TRIGGER IF EXISTS magnets_ai;
                    DROP TRIGGER IF EXISTS magnets_ad;
                    DROP TRIGGER IF EXISTS magnets_au;
                    DROP TABLE IF EXISTS magnets_fts;
                    """
                )
                raise sqlite3.OperationalError("需要重建 FTS 表")
        except sqlite3.OperationalError:
            conn.executescript(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS magnets_fts USING fts5(
                    name,
                    content='magnets',
                    content_rowid='id',
                    tokenize='trigram'
                );

                CREATE TRIGGER IF NOT EXISTS magnets_ai AFTER INSERT ON magnets BEGIN
                    INSERT INTO magnets_fts(rowid, name) VALUES (new.id, new.name);
                END;

                CREATE TRIGGER IF NOT EXISTS magnets_ad AFTER DELETE ON magnets BEGIN
                    INSERT INTO magnets_fts(magnets_fts, rowid, name) VALUES('delete', old.id, old.name);
                END;

                CREATE TRIGGER IF NOT EXISTS magnets_au AFTER UPDATE ON magnets BEGIN
                    INSERT INTO magnets_fts(magnets_fts, rowid, name) VALUES('delete', old.id, old.name);
                    INSERT INTO magnets_fts(rowid, name) VALUES (new.id, new.name);
                END;
                """
            )
            existing = conn.execute("SELECT COUNT(*) FROM magnets").fetchone()[0]
            if existing > 0:
                conn.execute("INSERT INTO magnets_fts(magnets_fts) VALUES('rebuild')")
        conn.commit()

    def _init_mysql(self, conn):
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS magnets (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    info_hash CHAR(40) NOT NULL UNIQUE,
                    name VARCHAR(1024) NOT NULL DEFAULT '',
                    name_lower VARCHAR(1024) NOT NULL DEFAULT '',
                    size BIGINT NOT NULL DEFAULT 0,
                    file_count INT NOT NULL DEFAULT 0,
                    files LONGTEXT,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    hot INT NOT NULL DEFAULT 1,
                    INDEX idx_magnets_created_at (created_at),
                    INDEX idx_magnets_size (size),
                    INDEX idx_magnets_hot (hot),
                    INDEX idx_magnets_name_lower (name_lower(255))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )
        conn.commit()

    # ==================== 基础查询工具 ====================

    def _fetchone(self, query: str, params: tuple | list = ()): 
        conn = self._get_conn()
        if self._backend == "mysql":
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchone()
        cursor = conn.execute(query, params)
        return cursor.fetchone()

    def _fetchall(self, query: str, params: tuple | list = ()): 
        conn = self._get_conn()
        if self._backend == "mysql":
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        cursor = conn.execute(query, params)
        return cursor.fetchall()

    def _execute(self, query: str, params: tuple | list = ()):
        conn = self._get_conn()
        if self._backend == "mysql":
            with conn.cursor() as cursor:
                cursor.execute(query, params)
            conn.commit()
            return
        conn.execute(query, params)
        conn.commit()

    def _row_value(self, row: Any, key: str, default=None):
        if row is None:
            return default
        if isinstance(row, dict):
            return row.get(key, default)
        try:
            return row[key]
        except Exception:
            return default

    def _to_result(self, row):
        return {
            "id": self._row_value(row, "id"),
            "info_hash": self._row_value(row, "info_hash", ""),
            "name": self._row_value(row, "name", ""),
            "size": self._row_value(row, "size", 0),
            "file_count": self._row_value(row, "file_count", 0),
            "files": self._row_value(row, "files", "[]"),
            "created_at": str(self._row_value(row, "created_at", "") or ""),
            "updated_at": str(self._row_value(row, "updated_at", "") or ""),
            "hot": self._row_value(row, "hot", 1),
            "magnet_link": f"magnet:?xt=urn:btih:{self._row_value(row, 'info_hash', '')}",
        }

    # ==================== 数据读写 ====================

    def insert_magnet(self, info_hash: str, name: str, size: int = 0, file_count: int = 0, files: str = "[]") -> bool:
        conn = self._get_conn()
        info_hash_lower = (info_hash or "").lower()
        if not info_hash_lower:
            return False

        if not name or not name.strip():
            try:
                if self._backend == "mysql":
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "UPDATE magnets SET hot = hot + 1, updated_at = CURRENT_TIMESTAMP WHERE info_hash = %s",
                            (info_hash_lower,),
                        )
                    conn.commit()
                else:
                    conn.execute(
                        "UPDATE magnets SET hot = hot + 1, updated_at = datetime('now') WHERE info_hash = ?",
                        (info_hash_lower,),
                    )
                    conn.commit()
                return False
            except Exception as exc:
                logger.error(f"更新热度失败: {exc}")
                return False

        name_lower = name.lower()
        try:
            if self._backend == "mysql":
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO magnets (info_hash, name, name_lower, size, file_count, files, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """,
                        (info_hash_lower, name, name_lower, size, file_count, files),
                    )
                conn.commit()
            else:
                conn.execute(
                    """
                    INSERT INTO magnets (info_hash, name, name_lower, size, file_count, files, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                    """,
                    (info_hash_lower, name, name_lower, size, file_count, files),
                )
                conn.commit()
            return True
        except Exception:
            try:
                existing = self.get_by_hash(info_hash_lower)
                if existing and ((existing.get("name") or "").startswith("Unknown-") or not (existing.get("name") or "").strip()):
                    if self._backend == "mysql":
                        with conn.cursor() as cursor:
                            cursor.execute(
                                """
                                UPDATE magnets
                                SET name = %s, name_lower = %s, size = %s, file_count = %s, files = %s,
                                    hot = hot + 1, updated_at = CURRENT_TIMESTAMP
                                WHERE info_hash = %s
                                """,
                                (name, name_lower, size, file_count, files, info_hash_lower),
                            )
                        conn.commit()
                    else:
                        conn.execute(
                            """
                            UPDATE magnets
                            SET name = ?, name_lower = ?, size = ?, file_count = ?, files = ?,
                                hot = hot + 1, updated_at = datetime('now')
                            WHERE info_hash = ?
                            """,
                            (name, name_lower, size, file_count, files, info_hash_lower),
                        )
                        conn.commit()
                else:
                    if self._backend == "mysql":
                        with conn.cursor() as cursor:
                            cursor.execute(
                                "UPDATE magnets SET hot = hot + 1, updated_at = CURRENT_TIMESTAMP WHERE info_hash = %s",
                                (info_hash_lower,),
                            )
                        conn.commit()
                    else:
                        conn.execute(
                            "UPDATE magnets SET hot = hot + 1, updated_at = datetime('now') WHERE info_hash = ?",
                            (info_hash_lower,),
                        )
                        conn.commit()
                return False
            except Exception as exc:
                logger.error(f"插入磁力链接失败: {exc}")
                return False

    def get_by_hash(self, info_hash: str) -> dict | None:
        try:
            if self._backend == "mysql":
                row = self._fetchone("SELECT * FROM magnets WHERE info_hash = %s", (info_hash.lower(),))
            else:
                row = self._fetchone("SELECT * FROM magnets WHERE info_hash = ?", (info_hash.lower(),))
            return dict(row) if isinstance(row, dict) else (dict(row) if row else None)
        except Exception:
            return None

    def search(self, keyword: str, page: int = 1, page_size: int = 20, sort: str = "time_desc") -> tuple[list, int]:
        return self._search_mysql(keyword, page, page_size, sort) if self._backend == "mysql" else self._search_sqlite(keyword, page, page_size, sort)

    def _search_sqlite(self, keyword: str, page: int = 1, page_size: int = 20, sort: str = "time_desc") -> tuple[list, int]:
        conn = self._get_conn()
        offset = (page - 1) * page_size
        sort_map = {
            "time_desc": "m.created_at DESC",
            "time_asc": "m.created_at ASC",
            "size_desc": "m.size DESC",
            "size_asc": "m.size ASC",
            "hot_desc": "m.hot DESC",
        }
        order_by = sort_map.get(sort, "m.created_at DESC")

        try:
            keyword = (keyword or "").strip()
            if not keyword:
                count_row = conn.execute("SELECT COUNT(*) as cnt FROM magnets").fetchone()
                total = count_row["cnt"] if count_row else 0
                rows = conn.execute(
                    f"SELECT * FROM magnets m ORDER BY {order_by} LIMIT ? OFFSET ?",
                    (page_size, offset),
                ).fetchall()
            else:
                keywords = keyword.split()
                use_like = any(has_cjk(kw) or len(kw) < 3 for kw in keywords)
                if use_like:
                    where_parts = []
                    params = []
                    for kw in keywords:
                        where_parts.append("m.name_lower LIKE ?")
                        params.append(f"%{kw.lower()}%")
                    where_clause = " AND ".join(where_parts)
                    count_row = conn.execute(
                        f"SELECT COUNT(*) as cnt FROM magnets m WHERE {where_clause}", params
                    ).fetchone()
                    total = count_row["cnt"] if count_row else 0
                    rows = conn.execute(
                        f"SELECT * FROM magnets m WHERE {where_clause} ORDER BY {order_by} LIMIT ? OFFSET ?",
                        params + [page_size, offset],
                    ).fetchall()
                else:
                    fts_query = " AND ".join(f'"{kw}"' for kw in keywords)
                    count_row = conn.execute(
                        "SELECT COUNT(*) as cnt FROM magnets m INNER JOIN magnets_fts fts ON m.id = fts.rowid WHERE magnets_fts MATCH ?",
                        (fts_query,),
                    ).fetchone()
                    total = count_row["cnt"] if count_row else 0
                    rows = conn.execute(
                        f"""SELECT m.* FROM magnets m
                            INNER JOIN magnets_fts fts ON m.id = fts.rowid
                            WHERE magnets_fts MATCH ?
                            ORDER BY {order_by}
                            LIMIT ? OFFSET ?""",
                        (fts_query, page_size, offset),
                    ).fetchall()
            return [self._to_result(row) for row in rows], total
        except Exception as exc:
            logger.error(f"SQLite 搜索失败: {exc}")
            return [], 0

    def _search_mysql(self, keyword: str, page: int = 1, page_size: int = 20, sort: str = "time_desc") -> tuple[list, int]:
        conn = self._get_conn()
        offset = (page - 1) * page_size
        sort_map = {
            "time_desc": "created_at DESC",
            "time_asc": "created_at ASC",
            "size_desc": "size DESC",
            "size_asc": "size ASC",
            "hot_desc": "hot DESC",
        }
        order_by = sort_map.get(sort, "created_at DESC")
        keyword = (keyword or "").strip()
        try:
            with conn.cursor() as cursor:
                if not keyword:
                    cursor.execute("SELECT COUNT(*) AS cnt FROM magnets")
                    total = (cursor.fetchone() or {}).get("cnt", 0)
                    cursor.execute(
                        f"SELECT * FROM magnets ORDER BY {order_by} LIMIT %s OFFSET %s",
                        (page_size, offset),
                    )
                    rows = cursor.fetchall()
                else:
                    keywords = keyword.split()
                    where_parts = []
                    params = []
                    for kw in keywords:
                        where_parts.append("name_lower LIKE %s")
                        params.append(f"%{kw.lower()}%")
                    where_clause = " AND ".join(where_parts) if where_parts else "1=1"
                    cursor.execute(f"SELECT COUNT(*) AS cnt FROM magnets WHERE {where_clause}", params)
                    total = (cursor.fetchone() or {}).get("cnt", 0)
                    cursor.execute(
                        f"SELECT * FROM magnets WHERE {where_clause} ORDER BY {order_by} LIMIT %s OFFSET %s",
                        params + [page_size, offset],
                    )
                    rows = cursor.fetchall()
            return [self._to_result(row) for row in rows], total
        except Exception as exc:
            logger.error(f"MySQL 搜索失败: {exc}")
            return [], 0

    def delete(self, magnet_id: int):
        if self._backend == "mysql":
            self._execute("DELETE FROM magnets WHERE id = %s", (magnet_id,))
        else:
            self._execute("DELETE FROM magnets WHERE id = ?", (magnet_id,))

    def get_db_size_bytes(self) -> int:
        try:
            if self._backend == "mysql":
                row = self._fetchone(
                    """
                    SELECT COALESCE(SUM(data_length + index_length), 0) AS size_bytes
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    """,
                    (self._settings["mysql_database"],),
                )
                return int(self._row_value(row, "size_bytes", 0) or 0)
            db_path = self._settings["sqlite_path"]
            wal_path = db_path + "-wal"
            shm_path = db_path + "-shm"
            total = 0
            for path in (db_path, wal_path, shm_path):
                if os.path.exists(path):
                    total += os.path.getsize(path)
            return total
        except Exception as exc:
            logger.error(f"获取数据库大小失败: {exc}")
            return 0

    def is_size_limit_reached(self, limit_gb: float | int) -> bool:
        try:
            limit_value = float(limit_gb or 0)
        except Exception:
            limit_value = 0
        if limit_value <= 0:
            return False
        limit_bytes = int(limit_value * 1024 * 1024 * 1024)
        return self.get_db_size_bytes() >= limit_bytes

    def delete_many(self, ids: list[int]) -> int:
        ids = [int(mid) for mid in ids if str(mid).strip()]
        if not ids:
            return 0
        conn = self._get_conn()
        placeholders = ", ".join(["%s"] * len(ids)) if self._backend == "mysql" else ", ".join(["?"] * len(ids))
        query = f"DELETE FROM magnets WHERE id IN ({placeholders})"
        if self._backend == "mysql":
            with conn.cursor() as cursor:
                cursor.execute(query, ids)
                deleted = cursor.rowcount
            conn.commit()
            return int(deleted or 0)
        cursor = conn.execute(query, ids)
        conn.commit()
        return int(cursor.rowcount or 0)

    def delete_by_rules(self, keyword: str = "", created_before: str = "", min_size: int | None = None, max_size: int | None = None) -> int:
        where_parts = []
        params = []

        keyword = (keyword or "").strip().lower()
        if keyword:
            words = [item for item in re.split(r"[\s,，;；\n]+", keyword) if item]
            for word in words:
                where_parts.append("name_lower LIKE %s" if self._backend == "mysql" else "name_lower LIKE ?")
                params.append(f"%{word}%")

        if created_before:
            where_parts.append("created_at < %s" if self._backend == "mysql" else "created_at < ?")
            params.append(created_before)

        if min_size is not None and int(min_size) > 0:
            where_parts.append("size >= %s" if self._backend == "mysql" else "size >= ?")
            params.append(int(min_size))

        if max_size is not None and int(max_size) > 0:
            where_parts.append("size <= %s" if self._backend == "mysql" else "size <= ?")
            params.append(int(max_size))

        if not where_parts:
            return 0

        query = f"DELETE FROM magnets WHERE {' AND '.join(where_parts)}"
        conn = self._get_conn()
        if self._backend == "mysql":
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                deleted = cursor.rowcount
            conn.commit()
            return int(deleted or 0)
        cursor = conn.execute(query, params)
        conn.commit()
        return int(cursor.rowcount or 0)

    def get_stats(self) -> dict:
        try:
            if self._backend == "mysql":
                total_row = self._fetchone("SELECT COUNT(*) AS cnt FROM magnets")
                today_row = self._fetchone("SELECT COUNT(*) AS cnt FROM magnets WHERE DATE(created_at) = CURRENT_DATE")
                total = int(self._row_value(total_row, "cnt", 0) or 0)
                today = int(self._row_value(today_row, "cnt", 0) or 0)
            else:
                total_row = self._fetchone("SELECT COUNT(*) as cnt FROM magnets")
                today_row = self._fetchone("SELECT COUNT(*) as cnt FROM magnets WHERE date(created_at) = date('now')")
                total = int(self._row_value(total_row, "cnt", 0) or 0)
                today = int(self._row_value(today_row, "cnt", 0) or 0)

            db_size = self.get_db_size_bytes()
            return {
                "total_magnets": total,
                "today_magnets": today,
                "db_size": db_size,
                "db_size_human": self._human_size(db_size),
                "db_backend": self._backend,
            }
        except Exception as exc:
            logger.error(f"获取统计失败: {exc}")
            return {
                "total_magnets": 0,
                "today_magnets": 0,
                "db_size": 0,
                "db_size_human": "0 B",
                "db_backend": self._backend,
            }

    @staticmethod
    def _human_size(size: int) -> str:
        size = float(size or 0)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"

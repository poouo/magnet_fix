"""
磁力搜索网站 - 主应用
"""
import os
import hashlib
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Depends, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware

from config import Config
from database import Database, DatabaseError
from dht_crawler import DHTCrawlerManager
from qbittorrent_client import QBittorrentClient, QBittorrentError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

config = Config()
db = Database(config)
crawler_manager = DHTCrawlerManager(db, config)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    db.init()
    logger.info("数据库初始化完成")
    yield
    crawler_manager.stop()
    logger.info("应用关闭，爬虫已停止")


app = FastAPI(title="磁力搜索", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=config.get("secret_key", "magnet-search-secret-key-2024"))
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ==================== 通用工具 ====================


def check_admin_auth(request: Request):
    """检查后台认证"""
    if not request.session.get("admin_logged_in"):
        raise HTTPException(status_code=401, detail="未登录")
    return True


def _read_page_html(filename: str) -> str:
    base_dir = os.path.dirname(__file__)
    candidates = [
        os.path.join(base_dir, "static", filename),
        os.path.join(base_dir, filename),
    ]
    for path in candidates:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
    raise FileNotFoundError(f"页面文件不存在: {filename}")


def _to_bool(value, default=False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _get_qbittorrent_config(include_password: bool = False) -> dict:
    cfg = {
        "enabled": _to_bool(config.get("qbittorrent_enabled", False)),
        "url": (config.get("qbittorrent_url", "") or "").strip(),
        "username": config.get("qbittorrent_username", "") or "",
        "save_path": config.get("qbittorrent_save_path", "") or "",
        "category": config.get("qbittorrent_category", "") or "",
        "tags": config.get("qbittorrent_tags", "") or "",
        "paused": _to_bool(config.get("qbittorrent_paused", False)),
        "auto_tmm": _to_bool(config.get("qbittorrent_auto_tmm", False)),
    }
    password = config.get("qbittorrent_password", "") or ""
    cfg["password_set"] = bool(password)
    cfg["configured"] = bool(cfg["url"] and cfg["username"] and password)
    if include_password:
        cfg["password"] = password
    return cfg


def _build_qbittorrent_client() -> QBittorrentClient:
    cfg = _get_qbittorrent_config(include_password=True)
    if not cfg["enabled"]:
        raise HTTPException(status_code=400, detail="qBittorrent 对接尚未启用")
    if not cfg["configured"]:
        raise HTTPException(status_code=400, detail="qBittorrent 配置信息不完整")
    return QBittorrentClient(
        base_url=cfg["url"],
        username=cfg["username"],
        password=cfg["password"],
    )


def _build_database_payload(body: dict, include_password: bool = True) -> dict:
    current_password = config.get("db_mysql_password", "") or ""
    posted_password = body.get("mysql_password")
    clear_password = _to_bool(body.get("clear_mysql_password"), False)

    if clear_password:
        mysql_password = ""
    elif include_password and posted_password not in (None, ""):
        mysql_password = str(posted_password)
    else:
        mysql_password = current_password

    payload = {
        "backend": (body.get("backend") or config.get("db_backend", "sqlite") or "sqlite").strip().lower(),
        "sqlite_path": (body.get("sqlite_path") or config.get("db_sqlite_path", "") or "").strip(),
        "mysql_host": (body.get("mysql_host") or config.get("db_mysql_host", "127.0.0.1") or "127.0.0.1").strip(),
        "mysql_port": int(body.get("mysql_port") or config.get("db_mysql_port", 3306) or 3306),
        "mysql_user": (body.get("mysql_user") or config.get("db_mysql_user", "root") or "root").strip(),
        "mysql_password": mysql_password,
        "mysql_database": (body.get("mysql_database") or config.get("db_mysql_database", "magnet_search") or "magnet_search").strip(),
        "mysql_charset": (body.get("mysql_charset") or config.get("db_mysql_charset", "utf8mb4") or "utf8mb4").strip(),
    }
    return payload


def _database_config_response() -> dict:
    data = db.get_config_view()
    data["runtime"] = db.get_runtime_info()
    return data


def _normalize_hex_color(value: str, default: str = "#4f46e5") -> str:
    value = (value or "").strip()
    if len(value) == 7 and value.startswith("#"):
        try:
            int(value[1:], 16)
            return value.lower()
        except ValueError:
            return default
    return default


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _site_settings_payload() -> dict:
    return {
        "theme_mode": (config.get("theme_mode", "dark") or "dark").strip().lower(),
        "theme_color": _normalize_hex_color(config.get("theme_color", "#4f46e5") or "#4f46e5"),
    }


def _crawler_settings_payload() -> dict:
    return {
        "cpu_limit": int(config.get("cpu_limit", 50) or 50),
        "max_workers": int(config.get("max_workers", 5) or 5),
        "db_size_limit_gb": _to_float(config.get("db_size_limit_gb", 0), 0),
        "save_filter_keywords": config.get("save_filter_keywords", "") or "",
        "save_filter_min_size_mb": _to_float(config.get("save_filter_min_size_mb", 0), 0),
        "save_filter_max_size_gb": _to_float(config.get("save_filter_max_size_gb", 0), 0),
    }


def _public_settings_payload() -> dict:
    return {
        "site": _site_settings_payload(),
        "qbittorrent": {
            "enabled": _to_bool(config.get("qbittorrent_enabled", False)),
        },
    }


# ==================== 前端页面 ====================


@app.get("/api/settings/public")
async def public_settings():
    """前台可读取的公共站点设置"""
    return {"code": 0, "data": _public_settings_payload()}



@app.get("/", response_class=HTMLResponse)
async def index():
    """首页 - 搜索页面"""
    return _read_page_html("index.html")


@app.get("/admin", response_class=HTMLResponse)
async def admin_page():
    """后台管理页面"""
    return _read_page_html("admin.html")


# ==================== 搜索 API ====================


@app.get("/api/search")
async def search(
    q: str = Query("", description="搜索关键词"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    sort: str = Query("time_desc", description="排序方式: time_desc, time_asc, size_desc, size_asc, hot_desc")
):
    """搜索磁力链接"""
    results, total = db.search(q, page, page_size, sort)
    return {
        "code": 0,
        "data": {
            "list": results,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
        }
    }


@app.get("/api/stats")
async def stats():
    """获取统计信息"""
    return {
        "code": 0,
        "data": db.get_stats()
    }


@app.get("/api/qbittorrent/status")
async def qbittorrent_status():
    """获取前台可见的 qBittorrent 状态"""
    cfg = _get_qbittorrent_config(include_password=False)
    return {
        "code": 0,
        "data": {
            "enabled": cfg["enabled"],
            "configured": cfg["configured"],
            "available": cfg["enabled"] and cfg["configured"],
            "category": cfg["category"],
            "tags": cfg["tags"],
            "paused": cfg["paused"],
        }
    }


@app.post("/api/qbittorrent/add")
async def qbittorrent_add(request: Request):
    """前台快速添加到 qBittorrent"""
    body = await request.json()
    magnet_link = (body.get("magnet_link") or "").strip()
    if not magnet_link.startswith("magnet:?"):
        raise HTTPException(status_code=400, detail="无效的磁力链接")

    try:
        client = _build_qbittorrent_client()
        cfg = _get_qbittorrent_config(include_password=False)
        client.add_magnet(
            magnet_link=magnet_link,
            save_path=cfg["save_path"],
            category=cfg["category"],
            tags=cfg["tags"],
            paused=cfg["paused"],
            auto_tmm=cfg["auto_tmm"],
        )
    except HTTPException:
        raise
    except QBittorrentError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return {"code": 0, "message": "已添加到 qBittorrent 下载队列"}


# ==================== 后台认证 API ====================


@app.post("/api/admin/login")
async def admin_login(request: Request):
    """后台登录"""
    body = await request.json()
    password = body.get("password", "")
    stored_password = config.get("admin_password", "admin123")

    if hashlib.sha256(password.encode()).hexdigest() == stored_password or password == stored_password:
        request.session["admin_logged_in"] = True
        return {"code": 0, "message": "登录成功"}
    raise HTTPException(status_code=403, detail="密码错误")


@app.post("/api/admin/logout")
async def admin_logout(request: Request):
    """后台登出"""
    request.session.clear()
    return {"code": 0, "message": "已登出"}


@app.get("/api/admin/check")
async def admin_check(request: Request):
    """检查登录状态"""
    if request.session.get("admin_logged_in"):
        return {"code": 0, "logged_in": True}
    return {"code": 0, "logged_in": False}


# ==================== 后台管理 API ====================


@app.post("/api/admin/password")
async def change_password(request: Request, auth=Depends(check_admin_auth)):
    """修改密码"""
    body = await request.json()
    old_password = body.get("old_password", "")
    new_password = body.get("new_password", "")

    stored_password = config.get("admin_password", "admin123")
    if hashlib.sha256(old_password.encode()).hexdigest() != stored_password and old_password != stored_password:
        raise HTTPException(status_code=403, detail="原密码错误")

    if len(new_password) < 4:
        raise HTTPException(status_code=400, detail="新密码长度不能少于4位")

    hashed = hashlib.sha256(new_password.encode()).hexdigest()
    config.set("admin_password", hashed)
    return {"code": 0, "message": "密码修改成功"}


@app.get("/api/admin/crawler/status")
async def crawler_status(auth=Depends(check_admin_auth)):
    """获取爬虫状态"""
    status = crawler_manager.get_status()
    return {"code": 0, "data": status}


@app.post("/api/admin/crawler/start")
async def crawler_start(auth=Depends(check_admin_auth)):
    """启动爬虫"""
    crawler_manager.start()
    return {"code": 0, "message": "爬虫已启动"}


@app.post("/api/admin/crawler/stop")
async def crawler_stop(auth=Depends(check_admin_auth)):
    """停止爬虫"""
    crawler_manager.stop()
    return {"code": 0, "message": "爬虫已停止"}


@app.get("/api/admin/crawler/config")
async def crawler_config_get(auth=Depends(check_admin_auth)):
    """获取爬虫与入库过滤配置"""
    return {"code": 0, "data": _crawler_settings_payload()}


@app.post("/api/admin/crawler/config")
async def crawler_config(request: Request, auth=Depends(check_admin_auth)):
    """修改爬虫配置"""
    body = await request.json()
    values = {}

    if body.get("cpu_limit") is not None:
        values["cpu_limit"] = max(10, min(100, int(body.get("cpu_limit"))))
    if body.get("max_workers") is not None:
        values["max_workers"] = max(1, min(64, int(body.get("max_workers"))))
    if body.get("db_size_limit_gb") is not None:
        values["db_size_limit_gb"] = max(0.0, round(_to_float(body.get("db_size_limit_gb"), 0), 3))
    if body.get("save_filter_keywords") is not None:
        values["save_filter_keywords"] = str(body.get("save_filter_keywords") or "").strip()
    if body.get("save_filter_min_size_mb") is not None:
        values["save_filter_min_size_mb"] = max(0.0, round(_to_float(body.get("save_filter_min_size_mb"), 0), 3))
    if body.get("save_filter_max_size_gb") is not None:
        values["save_filter_max_size_gb"] = max(0.0, round(_to_float(body.get("save_filter_max_size_gb"), 0), 3))

    if values:
        config.update(values)
    crawler_manager.reload_config()
    return {"code": 0, "message": "配置已更新", "data": _crawler_settings_payload()}


@app.get("/api/admin/site-settings")
async def get_admin_site_settings(auth=Depends(check_admin_auth)):
    """获取站点主题设置"""
    return {"code": 0, "data": _site_settings_payload()}


@app.post("/api/admin/site-settings")
async def save_admin_site_settings(request: Request, auth=Depends(check_admin_auth)):
    """保存站点主题设置"""
    body = await request.json()
    theme_mode = (body.get("theme_mode") or config.get("theme_mode", "dark") or "dark").strip().lower()
    if theme_mode not in {"dark", "light", "auto"}:
        theme_mode = "dark"
    theme_color = _normalize_hex_color(body.get("theme_color") or config.get("theme_color", "#4f46e5"))
    config.update({
        "theme_mode": theme_mode,
        "theme_color": theme_color,
    })
    return {"code": 0, "message": "站点主题设置已保存", "data": _site_settings_payload()}


@app.get("/api/admin/database/config")
async def get_admin_database_config(auth=Depends(check_admin_auth)):
    """获取数据库配置（隐藏密码）"""
    return {"code": 0, "data": _database_config_response()}


@app.post("/api/admin/database/test")
async def test_admin_database(request: Request, auth=Depends(check_admin_auth)):
    """测试数据库连接"""
    body = await request.json()
    payload = _build_database_payload(body)
    try:
        result = db.test_settings(payload)
    except DatabaseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"code": 0, "message": result["message"], "data": result}


@app.post("/api/admin/database/config")
async def save_admin_database_config(request: Request, auth=Depends(check_admin_auth)):
    """保存数据库配置；保存前必须验证连接成功"""
    body = await request.json()
    payload = _build_database_payload(body)
    try:
        test_result = db.test_settings(payload)
    except DatabaseError as exc:
        raise HTTPException(status_code=400, detail=f"保存前验证失败：{exc}") from exc

    was_running = crawler_manager.get_status().get("status") == "running"
    if was_running:
        crawler_manager.stop()

    try:
        db.switch_backend(payload)
        config.update({
            "db_backend": payload["backend"],
            "db_sqlite_path": payload["sqlite_path"],
            "db_mysql_host": payload["mysql_host"],
            "db_mysql_port": payload["mysql_port"],
            "db_mysql_user": payload["mysql_user"],
            "db_mysql_password": payload["mysql_password"],
            "db_mysql_database": payload["mysql_database"],
            "db_mysql_charset": payload["mysql_charset"],
        })
    except DatabaseError as exc:
        raise HTTPException(status_code=400, detail=f"数据库切换失败：{exc}") from exc

    message = "数据库配置已保存并切换成功"
    if was_running:
        message += "；由于切换了数据库，爬虫已自动停止，请确认后再启动"

    return {
        "code": 0,
        "message": message,
        "data": {
            "test": test_result,
            "config": _database_config_response(),
        }
    }


@app.get("/api/admin/qbittorrent/config")
async def get_admin_qbittorrent_config(auth=Depends(check_admin_auth)):
    """获取 qBittorrent 配置（隐藏密码原文）"""
    return {"code": 0, "data": _get_qbittorrent_config(include_password=False)}


@app.post("/api/admin/qbittorrent/config")
async def save_admin_qbittorrent_config(request: Request, auth=Depends(check_admin_auth)):
    """保存 qBittorrent 配置"""
    body = await request.json()
    current_password = config.get("qbittorrent_password", "") or ""
    posted_password = body.get("password")
    clear_password = _to_bool(body.get("clear_password"), False)

    if clear_password:
        password = ""
    elif posted_password is None or str(posted_password) == "":
        password = current_password
    else:
        password = str(posted_password)

    values = {
        "qbittorrent_enabled": _to_bool(body.get("enabled"), False),
        "qbittorrent_url": (body.get("url") or "").strip().rstrip("/"),
        "qbittorrent_username": (body.get("username") or "").strip(),
        "qbittorrent_password": password,
        "qbittorrent_save_path": (body.get("save_path") or "").strip(),
        "qbittorrent_category": (body.get("category") or "").strip(),
        "qbittorrent_tags": (body.get("tags") or "").strip(),
        "qbittorrent_paused": _to_bool(body.get("paused"), False),
        "qbittorrent_auto_tmm": _to_bool(body.get("auto_tmm"), False),
    }
    config.update(values)
    return {"code": 0, "message": "qBittorrent 配置已保存", "data": _get_qbittorrent_config(include_password=False)}


@app.post("/api/admin/qbittorrent/test")
async def test_admin_qbittorrent(request: Request, auth=Depends(check_admin_auth)):
    """测试 qBittorrent 连通性"""
    body = await request.json()
    test_url = (body.get("url") or config.get("qbittorrent_url", "") or "").strip().rstrip("/")
    test_username = (body.get("username") or config.get("qbittorrent_username", "") or "").strip()
    posted_password = body.get("password")
    test_password = posted_password if posted_password not in (None, "") else (config.get("qbittorrent_password", "") or "")

    try:
        client = QBittorrentClient(test_url, test_username, test_password)
        version = client.get_version()
    except QBittorrentError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return {"code": 0, "message": f"连接成功，qBittorrent 版本：{version}", "data": {"version": version}}


@app.get("/api/admin/magnets")
async def admin_magnets(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    q: str = Query("", description="搜索关键词"),
    auth=Depends(check_admin_auth)
):
    """后台管理 - 获取磁力链接列表"""
    results, total = db.search(q, page, page_size, "time_desc")
    return {
        "code": 0,
        "data": {
            "list": results,
            "total": total,
            "page": page,
            "page_size": page_size,
        }
    }


@app.delete("/api/admin/magnets/{magnet_id}")
async def delete_magnet(magnet_id: int, auth=Depends(check_admin_auth)):
    """删除磁力链接"""
    db.delete(magnet_id)
    return {"code": 0, "message": "已删除"}


@app.delete("/api/admin/magnets")
async def batch_delete_magnets(request: Request, auth=Depends(check_admin_auth)):
    """批量删除磁力链接"""
    body = await request.json()
    ids = body.get("ids", [])
    deleted = db.delete_many(ids)
    return {"code": 0, "message": f"已删除 {deleted} 条记录", "data": {"deleted": deleted}}


@app.post("/api/admin/magnets/delete-by-rules")
async def delete_magnets_by_rules(request: Request, auth=Depends(check_admin_auth)):
    """根据关键词、时间和大小规则批量删除磁力"""
    body = await request.json()
    keyword = (body.get("keyword") or "").strip()
    created_before = (body.get("created_before") or "").strip()
    min_size_mb = _to_float(body.get("min_size_mb"), -1)
    max_size_gb = _to_float(body.get("max_size_gb"), -1)

    min_size = int(min_size_mb * 1024 * 1024) if min_size_mb >= 0 else None
    max_size = int(max_size_gb * 1024 * 1024 * 1024) if max_size_gb >= 0 else None

    if not keyword and not created_before and min_size is None and max_size is None:
        raise HTTPException(status_code=400, detail="请至少提供一个删除条件")

    deleted = db.delete_by_rules(
        keyword=keyword,
        created_before=created_before,
        min_size=min_size,
        max_size=max_size,
    )
    return {"code": 0, "message": f"已按规则删除 {deleted} 条记录", "data": {"deleted": deleted}}


# ==================== 静态文件 ====================


static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

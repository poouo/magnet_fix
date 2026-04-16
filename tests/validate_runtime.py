import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import app, crawler_manager, db, _read_page_html, _public_settings_payload, _crawler_settings_payload  # noqa: E402


def main():
    index_html = _read_page_html("index.html")
    admin_html = _read_page_html("admin.html")

    assert "磁力搜索" in index_html
    assert "管理后台" in admin_html
    assert "qBittorrent" in index_html
    assert "数据库配置" in admin_html
    assert "按规则批量删除" in admin_html

    routes = {route.path for route in app.routes}
    expected_routes = {
        "/",
        "/admin",
        "/api/settings/public",
        "/api/search",
        "/api/stats",
        "/api/qbittorrent/status",
        "/api/qbittorrent/add",
        "/api/admin/login",
        "/api/admin/logout",
        "/api/admin/check",
        "/api/admin/password",
        "/api/admin/crawler/status",
        "/api/admin/crawler/start",
        "/api/admin/crawler/stop",
        "/api/admin/crawler/config",
        "/api/admin/site-settings",
        "/api/admin/database/config",
        "/api/admin/database/test",
        "/api/admin/qbittorrent/config",
        "/api/admin/qbittorrent/test",
        "/api/admin/magnets",
        "/api/admin/magnets/{magnet_id}",
        "/api/admin/magnets/delete-by-rules",
    }
    missing = expected_routes - routes
    assert not missing, f"missing routes: {sorted(missing)}"

    public_payload = _public_settings_payload()
    assert "site" in public_payload
    assert "theme_mode" in public_payload["site"]
    assert "theme_color" in public_payload["site"]
    assert "qbittorrent" in public_payload

    crawler_payload = _crawler_settings_payload()
    for key in [
        "cpu_limit",
        "max_workers",
        "db_size_limit_gb",
        "save_filter_keywords",
        "save_filter_min_size_mb",
        "save_filter_max_size_gb",
    ]:
        assert key in crawler_payload, f"missing crawler config key: {key}"

    status = crawler_manager.get_status()
    for key in [
        "status",
        "cpu_limit",
        "max_workers",
        "queue_size",
        "pending_queue",
        "total_metadata_ok",
        "total_metadata_fail",
    ]:
        assert key in status, f"missing crawler status key: {key}"

    stats = db.get_stats()
    assert "db_size_human" in stats
    assert "total_magnets" in stats
    print("runtime validation passed")


if __name__ == "__main__":
    main()

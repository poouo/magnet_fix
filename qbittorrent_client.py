"""
qBittorrent WebUI API 客户端
"""
import requests


class QBittorrentError(Exception):
    """qBittorrent 对接异常"""


class QBittorrentClient:
    def __init__(self, base_url: str, username: str, password: str, timeout: int = 15):
        self.base_url = (base_url or "").strip().rstrip("/")
        self.username = username or ""
        self.password = password or ""
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "Referer": self.base_url,
            "User-Agent": "magnet-search/1.0"
        })

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _ensure_ready(self):
        if not self.base_url:
            raise QBittorrentError("未配置 qBittorrent 地址")
        if not self.username:
            raise QBittorrentError("未配置 qBittorrent 用户名")
        if not self.password:
            raise QBittorrentError("未配置 qBittorrent 密码")

    def login(self):
        self._ensure_ready()
        try:
            resp = self.session.post(
                self._url("/api/v2/auth/login"),
                data={"username": self.username, "password": self.password},
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise QBittorrentError(f"连接 qBittorrent 失败: {exc}") from exc

        if resp.status_code == 403:
            raise QBittorrentError("qBittorrent 拒绝登录，可能触发了失败次数限制")
        if resp.status_code != 200:
            raise QBittorrentError(f"qBittorrent 登录失败，HTTP {resp.status_code}")

        text = (resp.text or "").strip()
        if text and text.lower() == "fails.":
            raise QBittorrentError("qBittorrent 用户名或密码错误")
        return True

    def get_version(self) -> str:
        self.login()
        try:
            resp = self.session.get(self._url("/api/v2/app/version"), timeout=self.timeout)
        except requests.RequestException as exc:
            raise QBittorrentError(f"获取 qBittorrent 版本失败: {exc}") from exc

        if resp.status_code != 200:
            raise QBittorrentError(f"获取 qBittorrent 版本失败，HTTP {resp.status_code}")
        return (resp.text or "").strip() or "unknown"

    def add_magnet(self, magnet_link: str, save_path: str = "", category: str = "", tags: str = "",
                   paused: bool = False, auto_tmm: bool = False):
        self.login()
        payload = {
            "urls": magnet_link,
            "paused": "true" if paused else "false",
            "autoTMM": "true" if auto_tmm else "false",
        }
        if save_path:
            payload["savepath"] = save_path
        if category:
            payload["category"] = category
        if tags:
            payload["tags"] = tags

        try:
            resp = self.session.post(
                self._url("/api/v2/torrents/add"),
                data=payload,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise QBittorrentError(f"提交到 qBittorrent 失败: {exc}") from exc

        if resp.status_code != 200:
            raise QBittorrentError(f"添加下载任务失败，HTTP {resp.status_code}")

        text = (resp.text or "").strip()
        if text and text.lower() == "fails.":
            raise QBittorrentError("qBittorrent 返回失败，请检查参数或下载目录配置")
        return True

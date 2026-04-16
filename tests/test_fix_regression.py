import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dht_crawler import DHTCrawlerManager  # noqa: E402
import dht_crawler  # noqa: E402


class DummyDB:
    def get_by_hash(self, info_hash):
        return None

    def insert_magnet(self, info_hash, name="", size=0, file_count=0, files_json=""):
        return True

    def estimate_size_bytes(self):
        return 0


class DummyConfig:
    def __init__(self):
        self.data = {
            "cpu_limit": 100,
            "max_workers": 2,
            "dht_port": 6881,
            "db_size_limit_gb": 0,
            "save_filter_keywords": "",
            "save_filter_min_size_mb": 0,
            "save_filter_max_size_gb": 0,
        }

    def get(self, key, default=None):
        return self.data.get(key, default)

    def set(self, key, value):
        self.data[key] = value


def wait_until(predicate, timeout=3.0, interval=0.02):
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(interval)
    return False


def main():
    config = DummyConfig()
    manager = DHTCrawlerManager(DummyDB(), config)

    original_fetch = dht_crawler.fetch_metadata_from_cache
    success_hashes = {f"{i:040x}" for i in range(12)}
    fail_hash = f"{999:040x}"
    attempts = {}

    def fake_fetch(info_hash):
        attempts[info_hash] = attempts.get(info_hash, 0) + 1
        time.sleep(0.08)
        if info_hash == fail_hash:
            return None
        if info_hash in success_hashes:
            return {
                "name": f"test-{info_hash[:8]}",
                "size": 1024,
                "file_count": 1,
                "files": [{"path": "a.bin", "size": 1024}],
            }
        return None

    try:
        dht_crawler.fetch_metadata_from_cache = fake_fetch
        manager.start()
        for i in range(12):
            manager._add_info_hash(f"{i:040x}")
        manager._add_info_hash(fail_hash)

        assert wait_until(lambda: manager.get_status()["total_metadata_ok"] >= 4), "metadata 未开始快速消费"
        status = manager.get_status()
        assert status["http_inflight"] <= manager._backlog_limit(), "HTTP 等待队列未按当前吞吐策略受控"

        old_pool = manager._http_pool
        config.set("max_workers", 4)
        manager.reload_config()
        expected_workers = manager._effective_http_workers(cpu_limit=config.get("cpu_limit"), max_workers=4)
        assert manager._http_pool is not old_pool, "线程池未热更新"
        assert manager._http_worker_count == expected_workers, "有效工作线程数未按高 CPU 放大语义热更新"

        assert wait_until(lambda: manager.get_status()["total_metadata_ok"] >= 10, timeout=4.0), "metadata 消费速度异常"
        assert wait_until(lambda: manager.get_status()["total_metadata_fail"] == 1, timeout=3.0), "终态失败计数异常"
        assert attempts.get(fail_hash) == 3, "HTTP 重试次数不符合预期"
        print("regression test passed")
    finally:
        try:
            manager.stop()
        finally:
            dht_crawler.fetch_metadata_from_cache = original_fetch


if __name__ == "__main__":
    main()

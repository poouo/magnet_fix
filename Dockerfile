FROM ubuntu:22.04

LABEL maintainer="magnet-search"
LABEL description="磁力搜索引擎 - 基于 DHT 网络的磁力链接搜索系统"

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Shanghai \
    CONFIG_PATH=/data/config.json \
    DB_PATH=/data/magnet.db \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        tzdata \
        python3 \
        python3-pip \
        python3-libtorrent \
        && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./requirements.txt
RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir -r requirements.txt

COPY app.py config.py database.py dht_crawler.py qbittorrent_client.py ./
COPY static ./static

RUN mkdir -p /data /tmp/magnet_metadata

EXPOSE 8080
EXPOSE 6881/udp

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/api/stats', timeout=5)" || exit 1

CMD ["python3", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]

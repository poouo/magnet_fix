# 部署文档

本文档补充说明如何在本地或服务器上部署磁力搜索引擎，以及如何启用 qBittorrent 联动、MySQL 后端和后台高级管理能力。

## 一、Docker Compose 部署

### 1. 启动服务

```bash
# 仅启动搜索站点（默认 SQLite）
docker compose up -d --build

# 搜索站点 + 示例 qBittorrent
docker compose --profile with-qb up -d --build

# 搜索站点 + 示例 MySQL
docker compose --profile with-mysql up -d --build

# 搜索站点 + 示例 qBittorrent + 示例 MySQL
docker compose --profile with-qb --profile with-mysql up -d --build
```

### 2. 访问地址

| 服务 | 地址 |
| --- | --- |
| 磁力搜索前台 | `http://localhost:8080` |
| 管理后台 | `http://localhost:8080/admin` |
| qBittorrent WebUI | `http://localhost:18080`（启用 `with-qb` 时） |
| MySQL | `127.0.0.1:13306`（启用 `with-mysql` 时） |

### 3. 首次配置 qBittorrent

`linuxserver/qbittorrent` 镜像首次启动后会生成 WebUI 默认账号信息。请先登录 qBittorrent，完成密码确认或修改，然后在本项目后台进行如下配置：

| 字段 | 推荐值 |
| --- | --- |
| WebUI 地址 | `http://qbittorrent:8080` |
| 用户名 | qBittorrent WebUI 用户名 |
| 密码 | qBittorrent WebUI 密码 |
| 保存路径 | `/downloads` |

保存后可点击“测试连接”，确认站点容器能够正常访问 qBittorrent 容器。

### 4. 首次配置 MySQL

如果启用了 `with-mysql`，可在后台将数据库切换到 MySQL。示例 Compose 中的默认参数如下：

| 字段 | 示例值 |
| --- | --- |
| 主机 | `mysql` |
| 端口 | `3306` |
| 用户名 | `magnet` |
| 密码 | `magnet123456` |
| 数据库名 | `magnet_search` |
| 字符集 | `utf8mb4` |

后台会要求你先点击“测试连接”，只有验证成功后才能保存。保存并切换数据库后，系统会自动停止正在运行的爬虫，请确认无误后再重新启动。

## 二、本地运行

### 1. 安装依赖

```bash
python3 -m pip install -r requirements.txt
```

### 2. 准备数据目录

```bash
mkdir -p data
export CONFIG_PATH=./data/config.json
export DB_PATH=./data/magnet.db
```

### 3. 启动应用

```bash
python3 -m uvicorn app:app --host 0.0.0.0 --port 8080
```

如需使用外部 MySQL，请在后台“数据库配置”中填写连接信息并验证保存；如需更完整的 DHT metadata 能力，请在宿主机额外安装系统版 `libtorrent`。

## 三、后台配置建议

### 1. 主题与显示模式

后台支持统一设置主题色与显示模式：

| 项目 | 说明 |
| --- | --- |
| 主题色 | 同步作用于前台与后台主要按钮、强调色 |
| 显示模式 | 支持 `dark`、`light`、`auto` |

在移动端和桌面端都会共同生效。

### 2. 爬虫高级配置

| 项目 | 说明 |
| --- | --- |
| CPU 限制 | 控制吞吐等级，高档位会提升 metadata 抓取与调度速度 |
| 基础并发线程数 | 与 CPU 限制共同计算有效并发 |
| 数据库大小上限（GB） | 超过阈值自动停止爬虫 |
| 保存过滤关键词 | 命中后不入库 |
| 最小文件大小（MB） | 小于阈值不入库 |
| 最大文件大小（GB） | 大于阈值不入库 |

### 3. 磁力管理与批量清理

后台磁力管理除了普通搜索、单条删除和勾选批量删除外，还支持按规则删除：

| 条件 | 说明 |
| --- | --- |
| 关键词 | 按标题或文件列表关键字批量删除 |
| 指定时间前 | 删除早于某个时间点的数据 |
| 最小文件大小（MB） | 删除大于等于该阈值的数据 |
| 最大文件大小（GB） | 删除小于等于该阈值的数据 |

## 四、生产环境建议

### 1. 网络与端口

| 端口 | 协议 | 用途 |
| --- | --- | --- |
| 8080 | TCP | Web 服务 |
| 6881 | UDP | DHT 网络 |
| 18080 | TCP | qBittorrent WebUI |
| 16881 | TCP/UDP | qBittorrent 下载通信 |
| 13306 | TCP | MySQL（如启用示例容器） |

如果不需要把 qBittorrent WebUI 或 MySQL 暴露到公网，可以只在内网访问，或者移除对应映射端口。

### 2. 持久化目录

| 路径 | 说明 |
| --- | --- |
| `./data` | 应用配置与 SQLite 数据库 |
| `./qbittorrent/config` | qBittorrent 配置 |
| `./qbittorrent/downloads` | qBittorrent 下载目录 |
| `./mysql/data` | MySQL 数据目录 |

### 3. 反向代理建议

若要对公网开放，建议在应用前增加 Nginx / Caddy：

1. 只暴露 Web 服务的 80/443 端口；
2. 对 `/admin` 增加额外访问控制；
3. 使用 HTTPS；
4. 保留 `6881/udp` 供 DHT 使用。

## 五、常见问题

### 1. 搜索页没有“添加到 qBittorrent”按钮

通常是以下原因：

- 后台尚未启用 qBittorrent 对接；
- WebUI 地址、用户名或密码未配置完整；
- 保存配置后尚未刷新搜索页。

### 2. Metadata 失败数较高

这在公开 DHT / 磁力网络里很常见。发现 hash 并不意味着一定能取到 metadata。建议重点观察：

- `Metadata 成功 / (成功 + 终态失败)` 的趋势；
- DHT 节点数是否稳定；
- 服务器是否放行 `6881/udp`；
- 容器中是否已安装并启用 `libtorrent`。

### 3. 切换 MySQL 后无法保存

重点排查：

- 主机、端口、用户名、密码、数据库名是否填写正确；
- MySQL 用户是否具有建表和写入权限；
- 容器网络中是否使用了正确的主机名（如 `mysql`）；
- 是否先通过了“测试连接”。

### 4. 数据库达到上限后爬虫自动停止

这是预期行为。请进入后台处理以下任一项后再重新启动爬虫：

- 提高数据库容量上限；
- 删除历史数据；
- 切换到更大容量的数据库存储位置；
- 改用 MySQL 并扩容后端存储。

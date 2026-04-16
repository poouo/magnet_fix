# Magnet Search

一个基于 **FastAPI + 可切换 SQLite/MySQL + DHT/HTTP metadata 抓取** 的磁力搜索网站，提供公开搜索页面、后台管理面板，以及 **qBittorrent WebUI API** 快速投递能力。

本项目适合用于学习以下主题：

- DHT / info_hash 发现与 metadata 获取流程；
- 基于 SQLite / MySQL 的轻量搜索服务设计；
- 后台可热更新的爬虫调度、过滤与容量控制；
- 与 qBittorrent WebUI API 联动；
- Docker / Docker Compose 快速部署与开源整理。

> 本项目仅供技术研究、协议学习与私有环境实践使用。请遵守你所在地区的法律法规，以及站点、网络和内容使用规范。

## 功能概览

| 模块 | 说明 |
| --- | --- |
| 搜索前台 | 支持关键词搜索、分页、排序、复制 Hash、复制磁力链接、直接打开磁力链接、快速添加到 qBittorrent |
| 统一主题 | 支持后台集中设置主题色、明暗模式，前台与后台统一生效，兼容手机端与 PC 端 |
| 后台仪表盘 | 查看总收录、今日新增、数据库大小、爬虫状态、Metadata 成功与失败情况 |
| 爬虫控制 | 支持启动/停止、CPU 限制、基础并发、数据库容量上限、保存过滤规则 |
| Metadata 获取 | 支持 HTTP 缓存、公开源直取、DHT/peer 回退、失败重试、终态失败去重 |
| 数据库 | 默认使用内置 SQLite，也支持在后台切换为指定 MySQL，且保存前必须验证连接成功 |
| 磁力管理 | 支持后台搜索、单条删除、批量删除，以及按关键词、时间、文件大小规则批量清理 |
| 下载器对接 | 后台配置 qBittorrent WebUI 地址/账号/保存策略，前台一键添加到 qBittorrent |

## 本次版本的重点改进

| 类别 | 改进 |
| --- | --- |
| 数据库 | 新增 SQLite / MySQL 可切换支持，保存前强制测试连接，切换成功后运行时生效 |
| 主题体验 | 新增集中主题色、黑夜/白天/跟随系统模式，前后台统一主题变量 |
| 容量保护 | 新增数据库大小上限（GB），超出后自动停止爬虫，避免磁盘失控增长 |
| 入库过滤 | 新增保存前关键词过滤、最小文件大小过滤、最大文件大小过滤 |
| 资源清理 | 新增后台规则批量删除，可按关键词、指定时间前、文件大小范围清理 |
| 抓取吞吐 | 在高 CPU 限制下放大有效并发、回退调度与等待队列，提高整体处理速度 |
| 对接能力 | 保留 qBittorrent 后台配置与前台快捷添加，并适配新的主题界面 |
| 部署 | Compose 模板新增可选 MySQL 与 qBittorrent profile，更适合 GitHub 开源展示 |

## 技术栈

| 组件 | 技术 |
| --- | --- |
| 后端 | FastAPI |
| 数据库 | SQLite / MySQL |
| 网络抓取 | requests、libtorrent（可选增强） |
| 前端 | 原生 HTML / CSS / JavaScript |
| 部署 | Docker / Docker Compose |
| 下载器集成 | qBittorrent WebUI API |

## 目录结构

```text
magnet-fix/
├── app.py
├── config.py
├── database.py
├── dht_crawler.py
├── qbittorrent_client.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── static/
│   ├── index.html
│   └── admin.html
├── tests/
│   ├── test_fix_regression.py
│   └── validate_runtime.py
├── docs/
│   └── deployment.md
├── data/
│   └── .gitkeep
├── .dockerignore
├── .gitignore
├── LICENSE
└── README.md
```

## 快速开始

### 方式一：Docker Compose

```bash
git clone <your-repo-url> magnet-fix
cd magnet-fix

# 仅启动搜索站点（默认 SQLite）
docker compose up -d --build

# 同时启动搜索站点和示例 qBittorrent 服务
docker compose --profile with-qb up -d --build

# 同时启动搜索站点和示例 MySQL 服务
docker compose --profile with-mysql up -d --build

# 同时启动搜索站点、qBittorrent、MySQL
docker compose --profile with-qb --profile with-mysql up -d --build
```

启动后默认访问：

| 页面 / 服务 | 地址 |
| --- | --- |
| 搜索首页 | `http://localhost:8080` |
| 管理后台 | `http://localhost:8080/admin` |
| qBittorrent WebUI | `http://localhost:18080`（启用 `with-qb` 时） |
| MySQL | `127.0.0.1:13306`（启用 `with-mysql` 时） |

> 如果你使用的是外部已有 qBittorrent 或 MySQL，也可以只启动 `magnet-search` 服务，再在后台填入对应配置。

### 方式二：本地直接运行

```bash
python3 -m pip install -r requirements.txt
mkdir -p data
export CONFIG_PATH=./data/config.json
export DB_PATH=./data/magnet.db
python3 -m uvicorn app:app --host 0.0.0.0 --port 8080
```

如果希望启用更完整的 DHT metadata 能力，请在宿主机额外安装系统版 `libtorrent`。

## 默认后台账号

系统首次启动时会自动生成配置文件，默认后台密码为：

```text
admin123
```

首次登录后请立即在后台修改密码。

## 后台重点配置说明

### 数据库配置

后台新增 **数据库配置** 区域，支持以下模式：

| 配置项 | 说明 |
| --- | --- |
| 数据库类型 | `sqlite` 或 `mysql` |
| SQLite 路径 | 默认使用项目内 `data/magnet.db` |
| MySQL 主机 / 端口 | 例如 `mysql` / `3306` 或外部地址 |
| MySQL 用户 / 密码 | 指定数据库账号 |
| MySQL 数据库名 | 例如 `magnet_search` |
| 字符集 | 默认 `utf8mb4` |

保存前必须先通过连接验证；验证失败则不能保存。数据库切换成功后会立即生效，并自动停掉正在运行的爬虫，避免跨库写入冲突。

### 站点主题与明暗模式

后台新增 **站点主题设置**，支持：

| 配置项 | 说明 |
| --- | --- |
| 主题色 | 统一控制前后台主色 |
| 明暗模式 | `dark`、`light`、`auto` |

保存后前台搜索页和后台管理页会同步应用。

### 爬虫高级配置

后台新增以下高级项：

| 配置项 | 说明 |
| --- | --- |
| CPU 限制 | 控制整体吞吐等级；高档位会显著放大有效并发 |
| 基础并发线程数 | 与 CPU 限制共同决定最终 metadata 工作线程数 |
| 数据库大小上限（GB） | 超出后自动停止爬虫 |
| 保存过滤关键词 | 命中关键词的资源不入库 |
| 最小文件大小（MB） | 小于该值的不入库 |
| 最大文件大小（GB） | 大于该值的不入库 |

### 磁力管理

后台新增 **按规则批量删除**，支持以下组合条件：

| 条件 | 说明 |
| --- | --- |
| 关键词 | 删除标题或文件列表中命中关键词的资源 |
| 指定时间前 | 删除早于某个时间点的资源 |
| 最小文件大小（MB） | 删除大于等于该值的资源 |
| 最大文件大小（GB） | 删除小于等于该值的资源 |

## qBittorrent 对接说明

后台新增了 **qBittorrent 对接** 配置区，支持以下项目：

| 配置项 | 说明 |
| --- | --- |
| 启用对接 | 是否在搜索结果中展示“添加到 qBittorrent”按钮 |
| WebUI 地址 | 例如 `http://qbittorrent:8080` 或 `http://127.0.0.1:18080` |
| 用户名 / 密码 | qBittorrent WebUI 登录凭据 |
| 保存路径 | 可选，指定默认下载目录 |
| Category | 可选，自动分类 |
| Tags | 可选，多个标签使用英文逗号分隔 |
| 添加后暂停 | 是否进入下载器后默认暂停 |
| 自动管理 | 是否启用 qBittorrent 自动管理模式 |

配置完成后，前台搜索结果会显示 **添加到 qBittorrent** 按钮，点击即可通过站点后端转发到 qBittorrent。

## 关于较高 Metadata 失败率

在 DHT 抓取场景里，**“发现 hash 很快，但 metadata 成功率明显低于发现数”本身是常见现象**。常见原因包括：

1. hash 已经失活，没有在线 peer；
2. 公共缓存未命中；
3. 服务器没有开放 UDP / 没有正确启用 libtorrent；
4. 公开源能发现 hash，但无法保证元数据始终可取。

当前版本已经进一步优化了失败统计口径、重试逻辑、回退链路、队列控制和高 CPU 档位下的吞吐策略，但无法从根本上消除公共网络环境导致的天然失败率。

## 主要 API

### 公开接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/settings/public` | 获取前台主题与公开设置 |
| GET | `/api/search` | 搜索磁力链接 |
| GET | `/api/stats` | 获取站点统计信息 |
| GET | `/api/qbittorrent/status` | 获取前台 qBittorrent 可用状态 |
| POST | `/api/qbittorrent/add` | 将磁力链接添加到 qBittorrent |

### 管理接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/admin/login` | 登录后台 |
| POST | `/api/admin/logout` | 退出后台 |
| GET | `/api/admin/check` | 检查后台登录状态 |
| POST | `/api/admin/password` | 修改后台密码 |
| GET | `/api/admin/crawler/status` | 查询爬虫状态 |
| GET | `/api/admin/crawler/config` | 获取爬虫高级配置 |
| POST | `/api/admin/crawler/config` | 更新爬虫高级配置 |
| GET | `/api/admin/site-settings` | 获取站点主题设置 |
| POST | `/api/admin/site-settings` | 保存站点主题设置 |
| GET | `/api/admin/database/config` | 获取数据库配置 |
| POST | `/api/admin/database/test` | 测试数据库连接 |
| POST | `/api/admin/database/config` | 保存数据库配置 |
| GET | `/api/admin/qbittorrent/config` | 获取 qBittorrent 配置 |
| POST | `/api/admin/qbittorrent/config` | 保存 qBittorrent 配置 |
| POST | `/api/admin/qbittorrent/test` | 测试 qBittorrent 连通性 |
| GET | `/api/admin/magnets` | 查询磁力资源 |
| DELETE | `/api/admin/magnets/{id}` | 删除单条记录 |
| DELETE | `/api/admin/magnets` | 批量删除记录 |
| POST | `/api/admin/magnets/delete-by-rules` | 按规则批量删除记录 |

## 生产部署建议

| 项目 | 建议 |
| --- | --- |
| 管理密码 | 首次启动后立即修改 |
| DHT 端口 | 放行 `6881/udp`，否则 DHT 成功率会明显下降 |
| 数据挂载 | 将 `./data`、`./qbittorrent/config`、`./qbittorrent/downloads`、`./mysql/data` 视情况持久化 |
| 反向代理 | 如对外开放，请通过 Nginx / Caddy 暴露，并增加 HTTPS |
| 访问控制 | 后台建议限制来源 IP 或增加额外鉴权 |
| 日志与磁盘 | 资源数据与下载文件增长较快，建议启用数据库容量上限并监控磁盘空间 |

## 测试

仓库保留了两个基础验证脚本：

```bash
python3 tests/validate_runtime.py
python3 tests/test_fix_regression.py
```

## License

本项目以 **MIT License** 开源，详见 [LICENSE](./LICENSE)。

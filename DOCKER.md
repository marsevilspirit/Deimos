# Deimos Docker 部署指南

本文档介绍如何使用 Docker 部署 Deimos 分布式键值存储系统。

## 🚀 快速开始

### 单节点部署

Deimos Docker 镜像默认运行单节点模式：

```bash
# 构建镜像
docker build -t deimos:latest .

# 运行单节点
docker run -d \
  --name deimos-single \
  -p 4001:4001 \
  -p 7001:7001 \
  -v deimos_data:/var/lib/deimos \
  deimos:latest
```

### 三节点集群部署

使用 Docker Compose 启动分布式集群：

```bash
# 启动 3 节点集群
docker-compose up -d

# 查看集群状态
docker-compose ps

# 查看日志
docker-compose logs -f

# 查看特定节点日志
docker-compose logs -f deimos1

# 停止集群
docker-compose down
```

## 📋 配置说明

### 环境变量

| 变量名 | 描述 | 默认值 |
|--------|------|--------|
| `DEIMOS_NAME` | 节点名称 | `node1` |
| `DEIMOS_LOG_LEVEL` | 日志级别 | `info` |

### 端口说明

| 端口 | 用途 | 协议 |
|------|------|------|
| `4001` | 客户端 API | HTTP |
| `7001` | 节点间通信 | HTTP |

### 数据持久化

数据存储在 `/var/lib/deimos` 目录，建议挂载 volume：

```bash
-v deimos_data:/var/lib/deimos
```

## 🔧 自定义配置

### 修改集群配置

编辑 `docker-compose.yml` 文件中的 `bootstrap-config` 参数：

```yaml
command: [
  "deimos",
  "-name", "node1",
  "-listen-client-urls", "http://0.0.0.0:4001",
  "-advertise-client-urls", "http://deimos1:4001",
  "-listen-peer-urls", "http://0.0.0.0:7001", 
  "-advertise-peer-urls", "http://deimos1:7001",
  "-bootstrap-config", "node1=http://deimos1:7001,node2=http://deimos2:7002,node3=http://deimos3:7003"
]
```

### 自定义 Dockerfile

如果需要添加额外的工具或配置：

```dockerfile
FROM deimos:latest

# 安装额外工具
USER root
RUN apk add --no-cache curl jq

# 切换回非 root 用户
USER deimos
```

## 🧪 测试集群

### 健康检查

```bash
# 检查所有节点状态
curl http://localhost:4001/machines
curl http://localhost:4002/machines  
curl http://localhost:4003/machines

# 测试 API - 写入数据
curl -X PUT http://localhost:4001/keys/test -d value="hello"

# 从不同节点读取数据（验证集群同步）
curl http://localhost:4001/keys/test
curl http://localhost:4002/keys/test
curl http://localhost:4003/keys/test

# 测试分布式锁
curl -X PUT http://localhost:4001/keys/locks/mylock -d value="client1" -d prevExist=false
```

### 使用客户端

```bash
# 进入容器
docker exec -it deimos-node1 sh

# 或者使用外部客户端连接
# endpoints: http://localhost:4001,http://localhost:4002,http://localhost:4003
```

## 📊 监控和日志

### 查看日志

```bash
# 查看所有节点日志
docker-compose logs

# 查看特定节点日志
docker-compose logs deimos1

# 实时跟踪日志
docker-compose logs -f deimos1
```

### 健康检查

Docker Compose 配置了健康检查，可以通过以下命令查看：

```bash
docker-compose ps
```

健康状态会显示为：
- `healthy` - 节点正常
- `unhealthy` - 节点异常
- `starting` - 节点启动中

## 🔒 安全配置

### 网络隔离

默认配置创建了独立的 Docker 网络 `deimos-cluster`，节点间通信被隔离。

### 用户权限

容器内使用非 root 用户 `deimos` (UID: 1000) 运行服务。

### 防火墙配置

生产环境建议：
- 只暴露必要的客户端端口 (4001-4003)
- 限制节点间通信端口 (7001-7003) 的访问
- 使用 TLS 加密通信

## 🚨 故障排除

### 常见问题

1. **节点无法启动**
   ```bash
   # 检查端口占用
   netstat -tulpn | grep :4001
   
   # 检查数据目录权限
   docker exec deimos-node1 ls -la /var/lib/deimos
   ```

2. **集群无法形成**
   ```bash
   # 检查网络连通性
   docker exec deimos-node1 ping deimos2
   
   # 检查配置
   docker-compose config
   ```

3. **数据丢失**
   ```bash
   # 检查 volume 挂载
   docker volume ls
   docker volume inspect deimos1_data
   ```

### 调试模式

启用详细日志：

```yaml
environment:
  - DEIMOS_LOG_LEVEL=debug
```

## 📚 更多资源

- [Deimos 官方文档](../README.md)
- [API 参考](../docs/)
- [客户端库](../Deimos-client/)

## 🤝 贡献

欢迎提交 Issue 和 Pull Request 来改进 Docker 配置！

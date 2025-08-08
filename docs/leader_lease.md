# Deimos Leader Lease 机制

## 概述

Leader Lease是Deimos中实现的一种优化机制，允许Leader节点在lease有效期内直接处理读请求，而无需通过Raft共识。这大大提高了读操作的性能。

## 工作原理

### 基本概念

- **Lease Duration**: Lease的有效期，默认为心跳间隔的3倍
- **Lease Renewal**: Leader定期续约lease，通常在剩余时间为1/3时续约
- **Lease Revocation**: 当节点不再是Leader时，立即撤销lease

### 生命周期

```
节点成为Leader → 获取Lease → 定期续约 → 失去Leader身份 → 撤销Lease
```

## 使用方式

### 1. 自动Lease管理

Deimos会自动管理Leader Lease，无需手动干预：

```go
// 启动节点时，Lease机制自动启用
n := raft.StartNode(id, peers, election, heartbeat)
```

### 2. 读取操作优化

GET请求会自动利用Lease机制：

```bash
# 如果当前节点有有效lease，直接返回结果
curl http://127.0.0.1:4001/keys/mykey

# 强制使用共识读取
curl http://127.0.0.1:4001/keys/mykey?quorum=true
```

### 3. 编程接口

```go
// 检查是否可以使用lease读取
if server.CanReadWithLease() {
    // 直接读取，无需共识
    result := store.Get(key)
} else {
    // 使用共识读取
    result := consensusRead(key)
}
```

## 配置参数

### Lease Duration

Lease持续时间通过心跳间隔自动计算：

```go
leaseDuration := heartbeatTimeout * 3
```

### 自定义配置

```go
// 创建自定义lease
lease := raft.NewLeaderLease(5 * time.Second)
```

## 性能优势

### 读取延迟对比

| 操作类型 | 延迟 | 说明 |
|----------|------|------|
| Lease读取 | ~1ms | 直接本地读取 |
| 共识读取 | ~10-50ms | 需要网络往返 |
| 普通GET | ~1ms | 本地读取，无一致性保证 |

### 吞吐量提升

- **Lease读取**: 可达数万QPS
- **共识读取**: 受限于网络和共识性能

## 一致性保证

### Lease读取的一致性

- **线性一致性**: 在lease有效期内保证
- **单调读**: 保证读取的数据不会回退
- **读写一致性**: 写操作后的读取能看到最新数据

### 故障场景

1. **网络分区**: Lease会自动过期，避免脑裂
2. **时钟偏移**: 内置时钟偏移容忍度
3. **Leader切换**: 新Leader获取新lease

## 监控和调试

### 检查Lease状态

```go
// 检查是否有有效lease
hasLease := server.HasValidLease()

// 检查是否可以lease读取
canRead := server.CanReadWithLease()
```

### 日志监控

```bash
# 查看lease相关日志
grep -i "lease\|leader" deimos.log
```

## 最佳实践

### 1. 合理设置心跳间隔

```bash
# 推荐配置
--heartbeat-timeout 100ms  # 心跳间隔
# Lease duration = 300ms (自动计算)
```

### 2. 监控Lease健康状态

```go
// 定期检查lease状态
go func() {
    ticker := time.NewTicker(time.Second)
    for range ticker.C {
        if !server.HasValidLease() && isLeader() {
            log.Warn("Leader without valid lease")
        }
    }
}()
```

### 3. 处理Lease失效

```go
func handleRead(key string) (string, error) {
    if server.CanReadWithLease() {
        return fastRead(key)
    } else {
        return consensusRead(key)
    }
}
```

## 故障排除

### 常见问题

1. **Lease频繁失效**
   - 检查网络延迟
   - 调整心跳间隔
   - 检查系统时钟

2. **读取性能未提升**
   - 确认节点是Leader
   - 检查lease状态
   - 验证请求路由

3. **一致性问题**
   - 检查时钟同步
   - 验证lease配置
   - 查看网络分区情况

### 调试命令

```bash
# 检查节点状态
curl http://127.0.0.1:4001/raft/status

# 强制共识读取进行对比
curl http://127.0.0.1:4001/keys/test?quorum=true
```

## 注意事项

1. **时钟依赖**: Lease机制依赖系统时钟，确保时钟同步
2. **网络分区**: 在网络分区时，lease会自动失效
3. **性能权衡**: Lease提高读性能，但增加了复杂性
4. **兼容性**: 与现有QGET机制完全兼容

## 示例代码

### 基本使用

```go
package main

import (
    "context"
    "fmt"
    "github.com/marsevilspirit/deimos/server"
)

func main() {
    // 创建服务器
    s := server.NewDeimosServer()
    
    // 处理读请求
    ctx := context.Background()
    req := server.Request{
        Method: "GET",
        Path:   "/mykey",
    }
    
    resp, err := s.Do(ctx, req)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }
    
    fmt.Printf("Value: %s\n", resp.Event.Node.Value)
}
```

### 高级配置

```go
// 自定义lease配置
func createCustomNode() raft.Node {
    // 使用较短的心跳间隔以获得更快的lease续约
    heartbeat := 50 // 50ms
    election := 500 // 500ms
    
    return raft.StartNode(1, []int64{1, 2, 3}, election, heartbeat)
}
```

通过以上实现，Deimos现在具备了完整的Leader Lease机制，可以显著提高读操作的性能，同时保持强一致性保证。

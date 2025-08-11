# Raft Leader Election Optimization

## 问题描述

在之前的版本中，Deimos的Raft leader选举存在以下问题：

1. **选举超时没有随机化**：所有节点都在完全相同的时间触发选举，容易导致选举冲突
2. **硬编码的超时值**：选举超时和心跳超时都是硬编码的，无法根据网络环境调整
3. **选举超时过短**：默认选举超时只有1秒，在网络延迟较高时容易产生不必要的选举

## 解决方案

### 1. 选举超时随机化

在 `raft/raft.go` 中实现了选举超时随机化：

```go
// Add randomization to election timeout to avoid split votes
// Randomize election timeout between [electionTimeout, 2*electionTimeout)
randomizedElectionTimeout := r.electionTimeout + rand.Intn(r.electionTimeout)
if r.elapsed > randomizedElectionTimeout {
    // 触发选举
}
```

这样可以避免多个节点同时触发选举，减少选举冲突。

### 2. 可配置的超时参数

添加了以下命令行参数：

- `--election-timeout`: 选举超时（以tick为单位，默认15）
- `--heartbeat-timeout`: 心跳超时（以tick为单位，默认1）
- `--tick-interval`: Raft tick间隔（默认100ms）

### 3. 优化的默认值

- 选举超时：从10个tick增加到15个tick（1.5秒）
- 心跳超时：保持1个tick（100ms）
- 租约时间：心跳超时的3倍（300ms）

## 使用方法

### 基本启动

```bash
./deimos --name node1 --bootstrap-config "node1=http://localhost:2380"
```

### 自定义超时配置

```bash
./deimos \
  --name node1 \
  --election-timeout 20 \
  --heartbeat-timeout 2 \
  --tick-interval 200ms \
  --bootstrap-config "node1=http://localhost:2380"
```

## 性能影响

- **选举成功率提升**：随机化减少了选举冲突，提高了选举成功率
- **网络稳定性**：更长的选举超时减少了网络抖动导致的频繁选举
- **可配置性**：用户可以根据网络环境调整超时参数

## 注意事项

1. 选举超时不应该设置得太短，建议至少是网络往返时间的2-3倍
2. 心跳超时应该足够短，以便快速检测到leader故障
3. 租约时间应该大于心跳超时，但小于选举超时

## 相关代码

- `raft/raft.go`: 选举超时随机化实现
- `main.go`: 命令行参数和配置
- `raft/lease.go`: Leader租约机制

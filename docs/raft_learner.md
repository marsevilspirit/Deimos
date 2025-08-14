# Raft Learner 功能

## 概述

Raft Learner 是 Deimos 中实现的一个新功能，它允许节点以无投票权成员的身份加入集群，直到赶上领导者的日志。这减少了集群扩展时的可用性差距。

## 背景

在传统的 Raft 实现中，当新节点加入集群时，它需要：
1. 接收所有历史日志条目
2. 等待日志同步完成
3. 然后才能参与投票和决策

这个过程可能导致集群在扩展期间出现可用性问题，因为新节点在同步期间无法贡献到法定人数（quorum）。

## Learner 节点特性

### 1. 无投票权
- Learner 节点不能参与领导者选举
- Learner 节点不能投票
- Learner 节点不计入法定人数计算

### 2. 日志同步
- Learner 节点接收来自领导者的所有日志条目
- Learner 节点可以处理读请求（如果领导者有有效的租约）
- Learner 节点参与心跳和日志复制

### 3. 自动提升
- 当 Learner 节点赶上领导者的日志时，可以自动提升为有投票权的成员
- 提升后，节点将参与投票和法定人数计算

## 实现细节

### 状态定义

```go
const (
    StateFollower StateType = iota
    StateCandidate
    StateLeader
    StateLearner  // 新增的 Learner 状态
)
```

### 数据结构

```go
type raft struct {
    // ... 其他字段 ...
    learners map[int64]bool  // 跟踪 Learner 节点
}
```

### 关键函数

#### 添加 Learner 节点
```go
func (r *raft) addLearner(id int64) {
    r.setProgress(id, 0, r.raftLog.lastIndex()+1)
    r.learners[id] = true
    r.pendingConf = false
}
```

#### 提升 Learner 节点
```go
func (r *raft) promoteLearner(id int64) {
    if r.learners[id] {
        delete(r.learners, id)
        // 更新新提升节点的进度
        if pr, exists := r.prs[id]; exists {
            pr.next = r.raftLog.lastIndex() + 1
        }
    }
}
```

#### 法定人数计算
```go
func (r *raft) q() int {
    votingNodes := 0
    for id := range r.prs {
        if !r.learners[id] {
            votingNodes++
        }
    }
    return votingNodes/2 + 1
}
```

## 使用场景

### 1. 集群扩展
```go
// 添加新的 Learner 节点
cluster.addLearner(3)

// 节点 3 将以 Learner 身份加入集群
// 不会影响现有的法定人数计算
```

### 2. 灾难恢复
```go
// 从快照恢复的节点可以作为 Learner 加入
// 快速同步日志，然后提升为有投票权的成员
```

### 3. 滚动升级
```go
// 在升级过程中，新版本节点可以作为 Learner 加入
// 确保集群稳定性
```

## 配置变更

### 添加 Learner 节点
```go
// 创建配置变更条目
confChange := pb.ConfChange{
    Type: pb.ConfChangeAddLearner,
    NodeID: newNodeID,
}

// 应用到 Raft 日志
r.Step(pb.Message{
    Type: msgProp,
    Entries: []pb.Entry{{
        Type: pb.EntryConfChange,
        Data: confChange.Marshal(),
    }},
})
```

### 提升 Learner 节点
```go
// 检查 Learner 是否可以提升
if r.shouldPromoteLearner(learnerID) {
    r.promoteLearner(learnerID)
}
```

## 监控和调试

### 获取 Learner 状态
```go
// 检查节点是否为 Learner
if r.isLearner(nodeID) {
    fmt.Printf("Node %d is a learner\n", nodeID)
}

// 获取所有 Learner 节点
learnerNodes := r.getLearnerNodes()

// 获取所有有投票权的节点
votingNodes := r.getVotingNodes()
```

### 日志同步状态
```go
// 检查 Learner 是否可以提升
if r.shouldPromoteLearner(learnerID) {
    fmt.Printf("Learner %d is ready for promotion\n", learnerID)
}
```

## 最佳实践

### 1. 合理使用
- 不要过度使用 Learner 节点
- Learner 节点主要用于临时状态，不应该长期存在

### 2. 监控提升
- 监控 Learner 节点的日志同步进度
- 及时提升已同步的 Learner 节点

### 3. 配置管理
- 在配置变更中明确指定节点类型
- 记录 Learner 节点的加入和提升历史

## 测试

运行 Learner 功能测试：
```bash
go test ./raft -v -run TestLearner
```

## 总结

Raft Learner 功能为 Deimos 提供了更灵活的集群管理能力，通过减少集群扩展时的可用性差距，提高了系统的稳定性和可扩展性。Learner 节点可以安全地加入集群，在同步完成后自动提升为有投票权的成员，这是一个优雅的解决方案。

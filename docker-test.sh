#!/bin/bash

# Deimos Docker 测试脚本

set -e

echo "🚀 开始测试 Deimos Docker 部署..."

# 构建镜像
echo "📦 构建 Docker 镜像..."
docker build -t deimos:latest .

# 启动集群
echo "🔧 启动 Deimos 集群..."
docker-compose up -d

# 等待集群启动
echo "⏳ 等待集群启动（10秒）..."
sleep 10

# 检查容器状态
echo "📊 检查容器状态..."
docker-compose ps

# 测试集群
echo "🧪 测试集群功能..."

# 检查节点状态
echo "检查节点 1..."
curl -f http://localhost:4001/machines || echo "❌ 节点 1 不可用"

echo "检查节点 2..."
curl -f http://localhost:4002/machines || echo "❌ 节点 2 不可用"

echo "检查节点 3..."
curl -f http://localhost:4003/machines || echo "❌ 节点 3 不可用"

# 测试数据写入和读取
echo "📝 测试数据写入..."
curl -X PUT http://localhost:4001/keys/test -d value="docker-test" || echo "❌ 写入失败"

echo "📖 测试数据读取..."
result1=$(curl -s http://localhost:4001/keys/test | grep "docker-test" || echo "")
result2=$(curl -s http://localhost:4002/keys/test | grep "docker-test" || echo "")
result3=$(curl -s http://localhost:4003/keys/test | grep "docker-test" || echo "")

if [[ -n "$result1" && -n "$result2" && -n "$result3" ]]; then
    echo "✅ 集群数据同步正常"
else
    echo "❌ 集群数据同步异常"
fi

# 测试分布式锁
echo "🔒 测试分布式锁..."
lock_result=$(curl -s -X PUT http://localhost:4001/keys/locks/test-lock -d value="test-client" -d prevExist=false)
if echo "$lock_result" | grep -q "test-client"; then
    echo "✅ 分布式锁功能正常"
else
    echo "❌ 分布式锁功能异常"
fi

echo "🎉 测试完成！"
echo ""
echo "📋 集群信息："
echo "  - 节点 1: http://localhost:4001"
echo "  - 节点 2: http://localhost:4002" 
echo "  - 节点 3: http://localhost:4003"
echo ""
echo "🛑 停止集群: docker-compose down"
echo "📊 查看日志: docker-compose logs -f"

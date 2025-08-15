#!/bin/bash

# 生成store protobuf Go代码的脚本

set -e

# 检查protoc是否安装
if ! command -v protoc &> /dev/null; then
    echo "错误: protoc 未安装，请先安装 Protocol Buffers 编译器"
    echo "安装方法: https://grpc.io/docs/protoc-installation/"
    exit 1
fi

# 检查go插件是否安装
if ! command -v protoc-gen-go &> /dev/null; then
    echo "错误: protoc-gen-go 未安装，请运行: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"
    exit 1
fi

# 检查gogo插件是否安装
if ! command -v protoc-gen-gogo &> /dev/null; then
    echo "错误: protoc-gen-gogo 未安装，请运行: go install github.com/gogo/protobuf/protoc-gen-gogo@latest"
    exit 1
fi

echo "开始生成 store protobuf Go 代码..."

# 创建输出目录
mkdir -p store/storepb

# 生成Go代码
protoc \
    --proto_path=. \
    --go_out=. \
    --gogo_out=. \
    store/store.proto

echo "store protobuf Go 代码生成完成！"
echo "输出文件: store/storepb/store.pb.go"

# 检查生成的文件
if [ -f "store/storepb/store.pb.go" ]; then
    echo "✅ 成功生成 store.pb.go"
    echo "文件大小: $(du -h store/storepb/store.pb.go | cut -f1)"
else
    echo "❌ 生成失败，未找到 store.pb.go 文件"
    exit 1
fi

echo "完成！"

# Deimos 开发指南

## 开发环境设置

### 1. 系统要求

- Go 1.21 或更高版本
- Git
- Make (可选，但推荐)

### 2. 克隆项目

```bash
git clone https://github.com/your-username/deimos.git
cd deimos
```

### 3. 安装依赖

```bash
# 安装 Go 依赖
go mod download

# 安装开发工具
make ci-install
```

### 4. 验证安装

```bash
# 运行测试
make test

# 运行 lint 检查
make lint

# 运行完整的 CI 流程
make ci
```

## 开发工作流

### 1. 代码质量检查

在提交代码之前，请运行以下检查：

```bash
# 格式化代码
make fmt

# 运行 go vet
make vet

# 运行 lint 检查
make lint

# 运行测试
make test

# 运行完整检查
make check
```

### 2. 使用 Pre-commit Hooks

安装 pre-commit hooks 来自动检查代码质量：

```bash
# 安装 pre-commit
pip install pre-commit

# 安装 hooks
pre-commit install

# 手动运行所有 hooks
pre-commit run --all-files
```

### 3. 提交前检查清单

- [ ] 代码已格式化 (`make fmt`)
- [ ] 通过了 go vet 检查 (`make vet`)
- [ ] 通过了 lint 检查 (`make lint`)
- [ ] 所有测试通过 (`make test`)
- [ ] 代码覆盖率没有下降
- [ ] 更新了相关文档

## 代码规范

### 1. Go 代码规范

- 遵循 [Effective Go](https://golang.org/doc/effective_go.html) 指南
- 使用 `gofmt` 格式化代码
- 导入包时使用 `goimports`
- 遵循 Go 命名约定

### 2. 测试规范

- 为所有新功能编写测试
- 测试覆盖率应保持在 80% 以上
- 使用表驱动测试模式
- 为边界情况编写测试

### 3. 文档规范

- 为所有导出的函数和类型添加注释
- 遵循 [Go 注释规范](https://golang.org/doc/effective_go.html#commentary)
- 更新 README 和相关文档

## CI/CD 流程

### 1. GitHub Actions

项目使用 GitHub Actions 进行持续集成，包括：

- **测试**: 在多个 Go 版本上运行测试
- **Lint**: 使用 golangci-lint 检查代码质量
- **构建**: 在多个平台上构建项目
- **安全扫描**: 使用 gosec 进行安全扫描

### 2. 本地 CI 运行

在本地运行完整的 CI 流程：

```bash
make ci
```

这将执行：
1. 安装依赖
2. 格式化代码
3. 运行 go vet
4. 运行 lint 检查
5. 运行测试（包括 race detector）
6. 生成覆盖率报告

### 3. 覆盖率报告

生成覆盖率报告：

```bash
make test-cover
```

这将在浏览器中打开覆盖率报告。

## 调试和故障排除

### 1. 常见问题

#### Lint 错误

如果遇到 lint 错误，可以：

```bash
# 查看具体的 lint 错误
make lint

# 自动修复一些简单问题
golangci-lint run --fix
```

#### 测试失败

如果测试失败：

```bash
# 运行特定测试
go test -v ./raft -run TestLearner

# 运行测试并查看详细输出
go test -v -race ./...
```

#### 构建问题

如果构建失败：

```bash
# 清理并重新构建
make clean
make build

# 检查 Go 版本
go version
```

### 2. 性能分析

使用 Go 内置的性能分析工具：

```bash
# CPU 性能分析
go test -cpuprofile=cpu.prof -bench=.

# 内存性能分析
go test -memprofile=mem.prof -bench=.

# 分析结果
go tool pprof cpu.prof
go tool pprof mem.prof
```

## 贡献指南

### 1. 创建 Pull Request

1. Fork 项目
2. 创建功能分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

### 2. 代码审查

- 所有代码更改都需要通过代码审查
- 确保 CI 检查通过
- 响应审查者的反馈

### 3. 提交消息规范

使用清晰的提交消息：

```
feat: add new learner functionality

- Implement Raft learner state
- Add learner promotion logic
- Update tests and documentation

Closes #123
```

提交类型：
- `feat`: 新功能
- `fix`: 错误修复
- `docs`: 文档更新
- `style`: 代码格式更改
- `refactor`: 代码重构
- `test`: 测试相关
- `chore`: 构建过程或辅助工具的变动

## 发布流程

### 1. 版本管理

使用语义化版本控制：

- `MAJOR.MINOR.PATCH`
- 例如：`1.0.0`, `1.1.0`, `1.1.1`

### 2. 发布检查清单

- [ ] 所有测试通过
- [ ] 代码覆盖率符合要求
- [ ] 文档已更新
- [ ] 版本号已更新
- [ ] CHANGELOG 已更新
- [ ] 标签已创建

### 3. 创建发布

```bash
# 创建标签
git tag -a v1.0.0 -m "Release version 1.0.0"

# 推送标签
git push origin v1.0.0
```

## 获取帮助

- 查看 [Issues](https://github.com/your-username/deimos/issues)
- 查看 [Discussions](https://github.com/your-username/deimos/discussions)
- 联系维护者

## 许可证

本项目采用 [MIT 许可证](LICENSE)。

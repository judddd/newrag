# NewRAG Search MCP Server 使用指南

## 快速开始

### 1. 安装依赖
```bash
cd newrag-mcp
npm install
npm run build
```

### 2. 确保配置文件就绪
确保父目录有 `config.yaml` 文件，包含 embedding 和 Elasticsearch 配置。

### 3. 启动服务
```bash
./start.sh
# 或
npm start
```

## 在 NewChat/Cursor 中配置

### HTTP Streamable 模式（推荐）

在 MCP 管理页面创建 Token 后，复制完整配置：

```json
{
  "mcpServers": {
    "newrag": {
      "initialTimeout": 30,
      "transport": "streamable",
      "url": "http://localhost:3001/mcp",
      "headers": {
        "Authorization": "Bearer YOUR_TOKEN_HERE"
      }
    }
  }
}
```

### Stdio 模式（本地调试）

```json
{
  "mcpServers": {
    "newrag-search": {
      "command": "node",
      "args": ["/path/to/newrag-mcp/dist/index.js"],
      "env": {
        "ES_URL": "http://localhost:9200"
      }
    }
  }
}
```

## 工具使用示例

### 1. 智能混合搜索 (hybrid_search)

**简单查询:**
```
请使用混合搜索查找: "如何配置 Kubernetes 集群"
```

大模型会自动调用：
```json
{
  "tool": "hybrid_search",
  "query": "如何配置 Kubernetes 集群",
  "size": 5
}
```

**指定索引和过滤:**
```
在 aiops_knowledge_base 索引中搜索 "日志分析" 相关内容，只显示相关度 > 0.7 的结果
```

大模型会调用：
```json
{
  "tool": "hybrid_search",
  "query": "日志分析",
  "index": "aiops_knowledge_base",
  "size": 10,
  "min_score": 0.7
}
```

### 2. 纯关键词搜索 (keyword_search)

**快速关键词查询:**
```
使用关键词搜索查找: "配置文件"
```

大模型会调用：
```json
{
  "tool": "keyword_search",
  "query": "配置文件",
  "size": 10
}
```

### 3. 获取完整文档 (get_document_chunks)

**根据ID获取文档:**
```
获取文档ID为 123 的完整内容
```

大模型会调用：
```json
{
  "tool": "get_document_chunks",
  "document_id": 123
}
```

## 工作原理

### 混合搜索流程

```
用户查询: "如何排查网络问题？"
    ↓
[1] MCP Server 接收查询
    ↓
[2] 调用 Embedding API (从 config.yaml 读取配置)
    POST http://localhost:1234/v1/embeddings
    {
      "model": "text-embedding-qwen3-embedding-4b",
      "input": "如何排查网络问题？"
    }
    ↓
[3] 获取向量 [0.123, -0.456, 0.789, ...]
    ↓
[4] 构建混合查询
    - 向量搜索 (cosine similarity)
    - BM25 关键词搜索
    - 按权重合并 (默认 7:3)
    ↓
[5] 执行 Elasticsearch 查询
    ↓
[6] 返回高亮结果给大模型
```

### 权重调整

在 `config.yaml` 中调整搜索权重：

```yaml
elasticsearch:
  hybrid_search:
    vector_weight: 0.7   # 向量搜索权重 (语义理解)
    bm25_weight: 0.3     # BM25 关键词权重 (精确匹配)
```

**建议配置:**
- **语义优先** (适合问答): vector_weight=0.8, bm25_weight=0.2
- **均衡模式** (默认): vector_weight=0.7, bm25_weight=0.3  
- **精确优先** (适合查找): vector_weight=0.5, bm25_weight=0.5

## 常见问题

### Q: 搜索结果相关度不高？
A: 调整 config.yaml 中的权重配置，或使用 min_score 参数过滤低相关度结果。

### Q: Embedding 生成失败？
A: 检查 LM Studio 是否运行，embedding 模型是否已加载。

### Q: 想要更精确的搜索？
A: 使用 keyword_search 工具进行 BM25 关键词搜索。

### Q: 如何搜索特定字段？
A: 使用 keyword_search，它会自动在多个重要字段中搜索。

### Q: 如何获取特定文档的完整内容？
A: 使用 get_document_chunks，传入 document_id。

## 最佳实践

1. **语义搜索用 hybrid_search**: 自然语言问题，需要语义理解
2. **快速查找用 keyword_search**: 精确关键词匹配，速度快
3. **完整文档用 get_document_chunks**: 根据ID获取所有页面
4. **调整权重优化结果**: 根据实际效果微调 vector_weight 和 bm25_weight
5. **使用 min_score 过滤**: 避免返回无关结果

## 性能提示

- hybrid_search 需要 embedding，速度较慢但准确
- keyword_search 无需 embedding，速度快
- 建议 size 参数不超过 50

## 调试

### 启用详细日志
```bash
NODE_ENV=development npm start
```

### 测试 Embedding API
```bash
curl -X POST http://localhost:1234/v1/embeddings \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer lm-studio" \
  -d '{
    "model": "text-embedding-qwen3-embedding-4b",
    "input": "测试文本"
  }'
```

### 测试 Elasticsearch
```bash
curl http://localhost:9200/aiops_knowledge_base/_search?pretty \
  -H "Content-Type: application/json" \
  -d '{"query": {"match_all": {}}, "size": 1}'
```

## 支持

如有问题，请检查：
1. config.yaml 配置是否正确
2. LM Studio 是否运行且模型已加载
3. Elasticsearch 是否可访问
4. 查看控制台错误日志


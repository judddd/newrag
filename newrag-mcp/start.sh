#!/bin/bash

# NewRAG Search MCP Server 启动脚本
# 注意: 请确保在父目录配置好 config.yaml

echo "🚀 Starting NewRAG Search MCP Server..."
echo ""

# 检查配置文件
if [ ! -f "../config.yaml" ]; then
    echo "⚠️  Warning: config.yaml not found in parent directory"
    echo "   The server will start but embedding generation may fail"
    echo ""
fi

# 自动从父目录 .env 加载 JWT_SECRET（如果未设置）
if [ -z "$JWT_SECRET" ] && [ -f "../.env" ]; then
    JWT_VAL=$(grep -E '^JWT_SECRET=' "../.env" | cut -d'=' -f2-)
    if [ -n "$JWT_VAL" ]; then
        export JWT_SECRET="$JWT_VAL"
        echo "✓ JWT_SECRET loaded from ../.env"
    fi
fi

# 启动服务器
echo ""
echo "Starting server..."
echo ""

npm start


#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/newrag-mcp"

# 自动从项目根目录 .env 加载 JWT_SECRET（如果未设置）
if [ -z "$JWT_SECRET" ] && [ -f "$SCRIPT_DIR/.env" ]; then
    JWT_VAL=$(grep -E '^JWT_SECRET=' "$SCRIPT_DIR/.env" | cut -d'=' -f2-)
    if [ -n "$JWT_VAL" ]; then
        export JWT_SECRET="$JWT_VAL"
        echo "✓ JWT_SECRET loaded from .env"
    fi
fi

export MCP_TRANSPORT=http
export MCP_HTTP_PORT=3001
export MCP_HTTP_HOST=localhost

echo "Starting NewRAG MCP Server..."
node dist/index.js



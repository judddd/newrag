#!/bin/bash

cd /Users/ablatazmat/Downloads/newrag/newrag-mcp

export MCP_TRANSPORT=http
export MCP_HTTP_PORT=3001
export MCP_HTTP_HOST=localhost

echo "Starting NewRAG MCP Server..."
node dist/index.js



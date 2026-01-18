#!/bin/bash
# MCP Memory wrapper для Antigravity

# Встановлюємо environment variables явно
export HOME="/Users/rsiromskyi"
export USER="rsiromskyi"
export HF_HOME="/Users/rsiromskyi/.cache/huggingface"
export TRANSFORMERS_CACHE="/Users/rsiromskyi/.cache/huggingface"
export HF_HUB_OFFLINE="1"
export TRANSFORMERS_OFFLINE="1"

# Запускаємо MCP сервер
exec /Users/rsiromskyi/mcp-memory/venv/bin/python -m mcp_memory_service.server "$@"

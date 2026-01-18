#!/bin/bash
# Quick Import Script - imports migrated data using Claude Code session
# This file should be read by Claude Code and executed via MCP tools

echo "==================================================================="
echo "Імпорт мігрованих даних в MCP Memory"
echo "==================================================================="
echo ""

MAIN_FILE="$HOME/mcp-memory/migration/main_memory_migrated.jsonl"
FIXARO_FILE="$HOME/mcp-memory/migration/fixaro_memory_migrated.jsonl"

echo "Файл 1: $MAIN_FILE"
echo "Файл 2: $FIXARO_FILE"
echo ""

# Count lines
MAIN_COUNT=$(wc -l < "$MAIN_FILE" 2>/dev/null || echo "0")
FIXARO_COUNT=$(wc -l < "$FIXARO_FILE" 2>/dev/null || echo "0")
TOTAL_COUNT=$((MAIN_COUNT + FIXARO_COUNT))

echo "Main memory: $MAIN_COUNT entities"
echo "Fixaro memory: $FIXARO_COUNT entities"
echo "Загалом: $TOTAL_COUNT entities"
echo ""

echo "==================================================================="
echo "ІНСТРУКЦІЇ ДЛЯ CLAUDE CODE:"
echo "==================================================================="
echo ""
echo "Прочитай обидва JSONL файли і для кожного запису виконай:"
echo ""
echo "mcp__memory__store_memory("
echo "  content=item['content'],"
echo "  metadata={'tags': item['tags'], 'type': item['type']}"
echo ")"
echo ""
echo "Імпортуй по 20 записів за раз, показуючи прогрес:"
echo "  Progress: 20/717 (3%)"
echo "  Progress: 40/717 (6%)"
echo "  ..."
echo ""
echo "Після завершення перевір через:"
echo "  mcp__memory__search_by_tag(['project:fixaro'])"
echo ""

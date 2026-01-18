#!/usr/bin/env python3
"""
Batch Import Script - быстрый импорт всех мигрированных данных
Использует прямой доступ к MCP Memory Service API
"""
import json
import os
import sys
from pathlib import Path
import asyncio
from typing import List, Dict

# Add MCP Memory Service to path
sys.path.insert(0, str(Path.home() / "mcp-memory"))

try:
    from mcp_memory_service.storage import get_storage_backend
    from mcp_memory_service.config import get_config
except ImportError as e:
    print(f"❌ Error: Cannot import MCP Memory Service: {e}")
    print("Make sure you're in the correct virtual environment")
    sys.exit(1)

def load_jsonl_file(filepath: Path) -> List[Dict]:
    """Load migrated JSONL file"""
    entities = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                entity = json.loads(line.strip())
                entities.append(entity)
            except json.JSONDecodeError as e:
                print(f"⚠️  Warning: Line {line_num} in {filepath.name} - {e}")
                continue
    return entities

async def batch_store_memories(storage, entities: List[Dict], batch_size: int = 50):
    """Store memories in batches"""
    total = len(entities)
    imported = 0
    failed = 0

    for i in range(0, total, batch_size):
        batch = entities[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total + batch_size - 1) // batch_size

        print(f"\n📦 Batch {batch_num}/{total_batches} ({len(batch)} entities)...")

        for idx, entity in enumerate(batch, 1):
            try:
                content = entity.get('content', '')
                tags = entity.get('tags', '').split(',') if isinstance(entity.get('tags'), str) else entity.get('tags', [])
                entity_type = entity.get('type', 'note')

                # Store memory
                await storage.store_memory(
                    content=content,
                    metadata={
                        'tags': tags,
                        'type': entity_type
                    }
                )

                imported += 1

                # Progress indicator
                if idx % 10 == 0:
                    print(f"  ✓ {idx}/{len(batch)} в текущем batch...")

            except Exception as e:
                failed += 1
                print(f"  ❌ Error storing entity {i+idx}: {e}")
                continue

        print(f"  ✅ Batch {batch_num} complete: {len(batch)} entities stored")

    return imported, failed

async def main():
    print("=" * 80)
    print("Batch Import - Быстрый импорт мигрированных данных")
    print("=" * 80)

    # Paths
    migration_dir = Path.home() / "mcp-memory" / "migration"
    fixaro_file = migration_dir / "fixaro_memory_migrated.jsonl"
    main_file = migration_dir / "main_memory_migrated.jsonl"

    # Check files exist
    if not fixaro_file.exists():
        print(f"❌ Error: {fixaro_file} not found")
        sys.exit(1)
    if not main_file.exists():
        print(f"❌ Error: {main_file} not found")
        sys.exit(1)

    # Load configuration
    print("\n[1] Loading MCP Memory Service configuration...")
    config = get_config()
    print(f"  ✓ Backend: {config.storage_backend}")
    print(f"  ✓ SQLite Path: {config.sqlite_path}")
    if config.storage_backend == "hybrid":
        print(f"  ✓ Cloudflare Account: {config.cloudflare_account_id[:8]}...")
        print(f"  ✓ Cloudflare D1 Database: {config.cloudflare_d1_database_id[:8]}...")
        print(f"  ✓ Cloudflare Vectorize: {config.cloudflare_vectorize_index}")

    # Initialize storage backend
    print("\n[2] Initializing storage backend...")
    storage = await get_storage_backend()
    print("  ✓ Storage backend initialized")

    # Load data
    print("\n[3] Loading migrated data files...")
    fixaro_entities = load_jsonl_file(fixaro_file)
    main_entities = load_jsonl_file(main_file)
    total_entities = len(fixaro_entities) + len(main_entities)

    print(f"  ✓ Fixaro memory: {len(fixaro_entities)} entities")
    print(f"  ✓ Main memory: {len(main_entities)} entities")
    print(f"  ✓ Total: {total_entities} entities")

    # Ask for confirmation
    print("\n" + "=" * 80)
    print(f"Ready to import {total_entities} entities to MCP Memory Service")
    print("=" * 80)
    response = input("\nПродолжить? (yes/no): ")

    if response.lower() not in ['yes', 'y', 'да', 'д']:
        print("❌ Import cancelled")
        sys.exit(0)

    # Import Fixaro entities
    print("\n" + "=" * 80)
    print("ИМПОРТ FIXARO MEMORY (project:fixaro)")
    print("=" * 80)
    fixaro_imported, fixaro_failed = await batch_store_memories(storage, fixaro_entities, batch_size=50)

    # Import Main entities
    print("\n" + "=" * 80)
    print("ИМПОРТ MAIN MEMORY (project:global)")
    print("=" * 80)
    main_imported, main_failed = await batch_store_memories(storage, main_entities, batch_size=50)

    # Summary
    total_imported = fixaro_imported + main_imported
    total_failed = fixaro_failed + main_failed

    print("\n" + "=" * 80)
    print("🎉 ИМПОРТ ЗАВЕРШЕН!")
    print("=" * 80)
    print(f"\n✅ Успешно импортировано: {total_imported}/{total_entities}")
    print(f"  - Fixaro: {fixaro_imported}/{len(fixaro_entities)}")
    print(f"  - Main: {main_imported}/{len(main_entities)}")

    if total_failed > 0:
        print(f"\n⚠️  Ошибки: {total_failed}/{total_entities}")

    print(f"\n📊 Success rate: {(total_imported/total_entities)*100:.1f}%")

    # Next steps
    print("\n" + "=" * 80)
    print("Следующие шаги:")
    print("=" * 80)
    print("1. Проверка импорта:")
    print("   В Claude Code: search_by_tag(['project:fixaro'])")
    print("   В Claude Code: search_by_tag(['project:global'])")
    print("\n2. Тестирование семантического поиска:")
    print("   В Claude Code: retrieve_memory('что я делал на проекте fixaro')")
    print("\n3. Cloudflare синхронизация (если hybrid backend):")
    print("   Автоматически в фоне каждые 5 минут")

    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n❌ Импорт прерван пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

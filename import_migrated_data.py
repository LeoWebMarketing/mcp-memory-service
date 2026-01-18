#!/usr/bin/env python3
"""
Import Migrated Data to MCP Memory
Imports JSONL files to MCP Memory Service using direct storage backend.
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime

# Add mcp-memory to path
sys.path.insert(0, str(Path(__file__).parent))

# Set environment variables for hybrid backend
os.environ.setdefault('MCP_MEMORY_STORAGE_BACKEND', 'hybrid')
os.environ.setdefault('CLOUDFLARE_API_TOKEN', 'Ryf13yjx5lMuZ8HCpFGLpmlf41tMTNmr-kU97ZSO')
os.environ.setdefault('CLOUDFLARE_ACCOUNT_ID', 'efe45bcdd6bf7ac89e0fe72ca5abddcf')
os.environ.setdefault('CLOUDFLARE_D1_DATABASE_ID', '0edda636-40e8-4db2-8b4b-122a2738adba')
os.environ.setdefault('CLOUDFLARE_VECTORIZE_INDEX', 'mcp-memory-index')

try:
    from mcp_memory_service.storage.hybrid_backend import HybridBackend
    from mcp_memory_service.storage.sqlite_vec_backend import SQLiteVecBackend
except ImportError:
    print("Error: Could not import mcp_memory_service")
    print("Make sure you're in the venv:")
    print("  cd ~/mcp-memory && source venv/bin/activate")
    sys.exit(1)

def import_jsonl(file_path: str, backend, batch_size: int = 50):
    """Import JSONL file to MCP Memory."""
    print(f"\n📁 Importing: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        items = [json.loads(line) for line in f if line.strip()]

    print(f"   Found {len(items)} items")

    imported = 0
    skipped = 0
    errors = 0

    for i, item in enumerate(items, 1):
        try:
            # Store memory
            result = backend.store(
                content=item['content'],
                metadata={
                    'tags': item['tags'],
                    'memory_type': item['type'],
                    'original_name': item.get('original_name', ''),
                    'original_type': item.get('original_type', ''),
                    'migrated_at': datetime.now().isoformat()
                }
            )

            if result:
                imported += 1
                if i % batch_size == 0:
                    print(f"   Progress: {i}/{len(items)} ({imported} imported, {skipped} skipped, {errors} errors)")
            else:
                skipped += 1

        except Exception as e:
            errors += 1
            print(f"   Error importing item {i}: {e}")
            continue

    print(f"   ✅ Complete: {imported} imported, {skipped} skipped, {errors} errors")
    return imported, skipped, errors

def main():
    print("=" * 80)
    print("Import Migrated Data to MCP Memory")
    print("=" * 80)

    # Initialize backend
    print("\n🔧 Initializing storage backend...")
    try:
        # Use SQLite-vec backend for initial import (faster)
        backend = SQLiteVecBackend()
        print("   ✅ SQLite-vec backend initialized")
    except Exception as e:
        print(f"   ❌ Error initializing backend: {e}")
        sys.exit(1)

    # Import files
    migration_dir = Path.home() / 'mcp-memory/migration'
    main_file = migration_dir / 'main_memory_migrated.jsonl'
    fixaro_file = migration_dir / 'fixaro_memory_migrated.jsonl'

    total_imported = 0
    total_skipped = 0
    total_errors = 0

    # Import main memory
    if main_file.exists():
        imported, skipped, errors = import_jsonl(str(main_file), backend)
        total_imported += imported
        total_skipped += skipped
        total_errors += errors
    else:
        print(f"\n⚠️  File not found: {main_file}")

    # Import Fixaro memory
    if fixaro_file.exists():
        imported, skipped, errors = import_jsonl(str(fixaro_file), backend)
        total_imported += imported
        total_skipped += skipped
        total_errors += errors
    else:
        print(f"\n⚠️  File not found: {fixaro_file}")

    # Final statistics
    print("\n" + "=" * 80)
    print("📊 Import Summary")
    print("=" * 80)
    print(f"Total imported: {total_imported}")
    print(f"Total skipped: {total_skipped}")
    print(f"Total errors: {total_errors}")
    print()

    if total_imported > 0:
        print("✅ Import complete!")
        print("\n🔍 Test searches:")
        print("   # All Fixaro entries")
        print("   mcp__memory__search_by_tag(['project:fixaro'])")
        print()
        print("   # All deployments")
        print("   mcp__memory__search_by_tag(['category:deployment'])")
        print()
        print("   # Semantic search")
        print("   mcp__memory__retrieve_memory('Fixaro deployment configuration')")
        print()

if __name__ == '__main__':
    main()

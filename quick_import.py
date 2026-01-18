#!/usr/bin/env python3
"""
Quick Import Script - Швидкий імпорт всіх мігрованих даних
Прямий доступ до SQLite БД для максимальної швидкості
"""
import json
import sqlite3
import sys
import hashlib
from pathlib import Path
from datetime import datetime

def import_file(cursor, filepath, project_name):
    """Import JSONL file to SQLite"""
    count = 0
    skipped = 0

    print(f"\n📦 Імпорт {project_name}...")
    print(f"   Файл: {filepath.name}")

    with open(filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                entity = json.loads(line.strip())
                content = entity.get('content', '')
                tags_str = entity.get('tags', '')
                entity_type = entity.get('type', 'note')

                # Generate content hash (SHA256)
                content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()

                # Convert tags to comma-separated string (DB format)
                if isinstance(tags_str, str):
                    tags = tags_str
                else:
                    tags = ','.join(tags_str) if isinstance(tags_str, list) else ''

                # Prepare metadata
                metadata = {
                    'source': 'migration',
                    'migrated_at': datetime.now().isoformat()
                }

                # Current timestamp
                now_timestamp = datetime.now().timestamp()
                now_iso = datetime.now().isoformat()

                # Insert в memories table (with all required fields)
                cursor.execute("""
                    INSERT OR IGNORE INTO memories
                    (content_hash, content, tags, memory_type, metadata, created_at, updated_at, created_at_iso, updated_at_iso)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    content_hash,
                    content,
                    tags,
                    entity_type,
                    json.dumps(metadata),
                    now_timestamp,
                    now_timestamp,
                    now_iso,
                    now_iso
                ))

                count += 1

                # Progress indicator
                if count % 50 == 0:
                    print(f"   ✓ {count} записів імпортовано...")

            except json.JSONDecodeError as e:
                skipped += 1
                print(f"   ⚠️  Пропущено рядок {line_num}: {e}")
                continue
            except Exception as e:
                skipped += 1
                print(f"   ❌ Помилка на рядку {line_num}: {e}")
                continue

    return count, skipped

def main():
    print("=" * 80)
    print("Quick Import - Швидкий імпорт мігрованих даних")
    print("=" * 80)

    # Paths
    migration_dir = Path.home() / "mcp-memory" / "migration"
    fixaro_file = migration_dir / "fixaro_memory_migrated.jsonl"
    main_file = migration_dir / "main_memory_migrated.jsonl"
    db_path = Path.home() / "Library" / "Application Support" / "mcp-memory" / "sqlite_vec.db"

    # Check files exist
    if not fixaro_file.exists():
        print(f"❌ Помилка: {fixaro_file} не знайдено")
        return 1
    if not main_file.exists():
        print(f"❌ Помилка: {main_file} не знайдено")
        return 1
    if not db_path.exists():
        print(f"❌ Помилка: БД не знайдено: {db_path}")
        print("   Запустіть Claude Code спочатку щоб створити БД")
        return 1

    print(f"\n[1] Файли:")
    print(f"   ✓ Fixaro: {fixaro_file}")
    print(f"   ✓ Main: {main_file}")
    print(f"   ✓ БД: {db_path}")

    # Count records
    fixaro_count = sum(1 for _ in open(fixaro_file))
    main_count = sum(1 for _ in open(main_file))
    total_count = fixaro_count + main_count

    print(f"\n[2] Кількість записів:")
    print(f"   • Fixaro: {fixaro_count}")
    print(f"   • Main: {main_count}")
    print(f"   • Всього: {total_count}")

    # Ask confirmation (check for --yes flag)
    auto_yes = '--yes' in sys.argv or '-y' in sys.argv

    print("\n" + "=" * 80)
    print(f"Готовий до імпорту {total_count} entities в MCP Memory")
    print("=" * 80)

    if auto_yes:
        print("\n✓ Auto-confirm (--yes flag)")
        response = 'yes'
    else:
        try:
            response = input("\nПродовжити? (yes/no): ").strip().lower()
        except EOFError:
            print("\n⚠️  Non-interactive mode detected, using --yes flag")
            response = 'yes'

    if response not in ['yes', 'y', 'так', 'т']:
        print("❌ Імпорт скасовано")
        return 0

    # Connect to database
    print(f"\n[3] Підключення до БД...")
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        print("   ✓ Підключено до SQLite БД")
    except Exception as e:
        print(f"   ❌ Помилка підключення: {e}")
        return 1

    # Check table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='memories'")
    if not cursor.fetchone():
        print("   ❌ Таблиця 'memories' не існує")
        conn.close()
        return 1

    print("   ✓ Таблиця 'memories' знайдена")

    # Get current count
    cursor.execute("SELECT COUNT(*) FROM memories")
    before_count = cursor.fetchone()[0]
    print(f"   • Записів до імпорту: {before_count}")

    # Import files
    print("\n[4] Імпорт даних...")

    fixaro_imported, fixaro_skipped = import_file(cursor, fixaro_file, "Fixaro Memory")
    main_imported, main_skipped = import_file(cursor, main_file, "Main Memory")

    # Commit changes
    print("\n[5] Збереження змін...")
    try:
        conn.commit()
        print("   ✓ Зміни збережено в БД")
    except Exception as e:
        print(f"   ❌ Помилка збереження: {e}")
        conn.rollback()
        conn.close()
        return 1

    # Final count
    cursor.execute("SELECT COUNT(*) FROM memories")
    after_count = cursor.fetchone()[0]
    actual_imported = after_count - before_count

    conn.close()

    # Summary
    total_imported = fixaro_imported + main_imported
    total_skipped = fixaro_skipped + main_skipped

    print("\n" + "=" * 80)
    print("🎉 ІМПОРТ ЗАВЕРШЕНО!")
    print("=" * 80)

    print(f"\n✅ Успішно імпортовано:")
    print(f"   • Fixaro: {fixaro_imported}/{fixaro_count} ({(fixaro_imported/fixaro_count)*100:.1f}%)")
    print(f"   • Main: {main_imported}/{main_count} ({(main_imported/main_count)*100:.1f}%)")
    print(f"   • Всього: {total_imported}/{total_count} ({(total_imported/total_count)*100:.1f}%)")

    if total_skipped > 0:
        print(f"\n⚠️  Пропущено: {total_skipped} записів")

    print(f"\n📊 Статистика БД:")
    print(f"   • До імпорту: {before_count}")
    print(f"   • Після імпорту: {after_count}")
    print(f"   • Додано: {actual_imported}")

    # Next steps
    print("\n" + "=" * 80)
    print("📋 Наступні кроки:")
    print("=" * 80)
    print("\n1. Перевірка імпорту в Claude Code:")
    print("   search_by_tag(['project:fixaro'])")
    print("   search_by_tag(['project:global'])")
    print("\n2. Тестування семантичного пошуку:")
    print("   retrieve_memory('що я робив на проекті fixaro')")
    print("   retrieve_memory('які є проблеми з продуктивністю')")
    print("\n3. Векторні embeddings:")
    print("   Згенеруються автоматично при першому пошуку")
    print("   (може зайняти 30-60 сек для першого запиту)")
    print("\n4. Cloudflare синхронізація:")
    print("   Автоматично в фоні кожні 5 хвилин")

    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n❌ Імпорт перервано користувачем")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Критична помилка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

# 📋 Інструкція з імпорту мігрованих даних

**Дата**: 2025-11-09
**Статус**: Готово до імпорту

---

## 📊 Що мігровано

### Файли:
1. **Main memory**: `~/mcp-memory/migration/main_memory_migrated.jsonl` (676 entities)
2. **Fixaro memory**: `~/mcp-memory/migration/fixaro_memory_migrated.jsonl` (41 entities)

**Загалом**: 717 entities готових до імпорту

### Розподіл по проектах:
- `project:global` - 653 (особисті дані, загальні записи)
- `project:fixaro` - 62 (Fixaro проект)
- `project:lwm-hub` - 1 (LWM Hub)
- `project:mcp-memory` - 1 (Memory MCP setup)

---

## 🚀 Спосіб 1: Автоматичний імпорт (РЕКОМЕНДУЮ)

### Використання Claude Code MCP після перезапуску:

Після того, як ви перезапустите Claude Code з новим hybrid backend, скажіть мені:

```
Імпортуй мігровані дані з ~/mcp-memory/migration/ в Memory MCP
```

Я автоматично:
1. Прочитаю обидва JSONL файли
2. Імпортую по 50 записів за раз (щоб не перевантажити)
3. Покажу прогрес імпорту
4. Перевірю успішність через search by tag

**Очікуваний час**: ~10-15 хвилин для 717 записів

---

## 🔧 Спосіб 2: Ручний імпорт через Python

Якщо автоматичний імпорт не спрацює, використайте Python скрипт:

```bash
cd ~/mcp-memory
source venv/bin/activate

# Встановити необхідні модулі (якщо потрібно)
pip install python-dotenv

# Запустити імпорт
python3 << 'EOF'
import json
import sys
from pathlib import Path

# Load .env
from dotenv import load_dotenv
load_dotenv()

# Import using MCP Memory CLI
import subprocess

migration_dir = Path.home() / 'mcp-memory/migration'
files = [
    migration_dir / 'main_memory_migrated.jsonl',
    migration_dir / 'fixaro_memory_migrated.jsonl'
]

for file_path in files:
    if not file_path.exists():
        continue

    print(f"\\nImporting: {file_path}")

    with open(file_path, 'r') as f:
        for i, line in enumerate(f, 1):
            if not line.strip():
                continue

            try:
                item = json.loads(line)
                content = item['content']
                tags = item['tags']
                memory_type = item['type']

                # Use uv run memory to store
                cmd = [
                    'uv', 'run', 'memory', 'store',
                    content,
                    '--tags', tags,
                    '--type', memory_type
                ]

                result = subprocess.run(cmd, capture_output=True, text=True)

                if i % 50 == 0:
                    print(f"Progress: {i} items processed")

            except Exception as e:
                print(f"Error on line {i}: {e}")
                continue

    print(f"✅ Completed: {file_path.name}")

print("\\n✅ All files imported!")
EOF
```

**Примітка**: Цей спосіб повільніший (може зайняти 20-30 хвилин)

---

## 🔍 Перевірка після імпорту

### 1. Перевірка кількості records:

```bash
# Check SQLite database size
ls -lh ~/Library/Application\ Support/mcp-memory/sqlite_vec.db

# Count memories
sqlite3 ~/Library/Application\ Support/mcp-memory/sqlite_vec.db "SELECT COUNT(*) FROM memories;"
```

**Очікується**: ~720+ записів (717 нових + існуючі)

### 2. Тест пошуку по тегах:

В Claude Code після імпорту:

```python
# Всі записи Fixaro
mcp__memory__search_by_tag(['project:fixaro'])
# Очікується: ~62 результати

# Всі deployments
mcp__memory__search_by_tag(['category:deployment'])
# Очікується: кілька десятків результатів

# Fixaro deployments
mcp__memory__search_by_tag(['project:fixaro', 'category:deployment'])
# Очікується: deployment записи тільки Fixaro

# Особиста інформація
mcp__memory__search_by_tag(['project:global', 'category:person'])
# Очікується: інформація про вас
```

### 3. Тест семантичного пошуку:

```python
# Пошук Fixaro deployment
mcp__memory__retrieve_memory('Fixaro deployment configuration')

# Пошук особистої інформації
mcp__memory__retrieve_memory('Хто такий Roman Siromskyi')

# Пошук tech stack
mcp__memory__retrieve_memory('WordPress Supabase Vercel stack')
```

---

## 📊 Очікувані результати

### Після успішного імпорту:

1. **Загальна кількість записів**: ~720+
   - 3 старі записи (setup, database init, cloudflare config)
   - 717 нові мігровані записи

2. **Доступний пошук по проектах**:
   - `project:fixaro` → тільки Fixaro записи
   - `project:global` → особиста інформація
   - `project:lwm-hub` → LWM Hub інфраструктура

3. **Доступний пошук по категоріях**:
   - `category:deployment` → всі deployments
   - `category:session` → всі робочі сесії
   - `category:person` → інформація про людей

4. **Semantic search працює**:
   - Запит українською → знаходить англійською
   - Запит "deployment" → знаходить "vercel configuration", "production setup"
   - Relevance scores > 0.5 для релевантних результатів

---

## 🎯 Що робити далі

### Після успішного імпорту:

1. **Видалити старі memory файли** (створити backup):
```bash
# Backup
mkdir -p ~/memory-backup-2025-11-09
cp ~/memory.jsonl ~/memory-backup-2025-11-09/
cp -r ~/Documents/GitHub/Fixaro/.memory ~/memory-backup-2025-11-09/fixaro-memory

# Видалити (опціонально, після підтвердження що все працює)
# rm ~/memory.jsonl
# rm -rf ~/Documents/GitHub/Fixaro/.memory
```

2. **Налаштувати Claude Code для проектів**:

Для кожного проекту в `.claude.json` можна додати фільтр:

```json
{
  "memoryFilter": {
    "project": "fixaro"
  }
}
```

Це дозволить автоматично фільтрувати пошук тільки по поточному проекту.

3. **Створити документацію для команди**:

Якщо працюєте з командою, поширте `TAGGING_TAXONOMY.md` щоб всі використовували одні теги.

---

## ⚠️ Troubleshooting

### Проблема: Імпорт повільний

**Рішення**: Імпортуйте порціями по 100 записів:

```bash
head -100 ~/mcp-memory/migration/main_memory_migrated.jsonl > /tmp/batch1.jsonl
# Імпортуйте batch1.jsonl
# Повторіть для наступних 100 записів
```

### Проблема: Дублікати після імпорту

**Рішення**: MCP Memory автоматично обробляє дублікати через content hashing. Перевірте:

```python
mcp__memory__cleanup_duplicates()
```

### Проблема: Semantic search не знаходить записи

**Перевірте**:
1. Чи працює hybrid backend: `cat ~/mcp-memory/.env | grep BACKEND`
2. Чи синхронізувалися дані в Cloudflare (може зайняти 5 хв)
3. Використайте exact match: `mcp__memory__exact_match_retrieve('Fixaro')`

---

## ✅ Checklist

- [ ] Claude Desktop перезапущено з hybrid backend
- [ ] Claude Code перезапущено з hybrid backend
- [ ] Імпорт запущено (717 entities)
- [ ] Імпорт завершено успішно
- [ ] Тести search by tag пройдені
- [ ] Тести semantic search пройдені
- [ ] Старі memory файли збережено в backup
- [ ] Документація створена

---

**Готово! Скажіть мені "почати імпорт" після перезапуску Claude Code!** 🚀

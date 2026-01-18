# Memory Tagging Taxonomy

Система тегування для організації пам'яті по проектах та категоріях.

## 📋 Структура тегів

### 1. **Project Tags** (обов'язковий для кожного запису)

Формат: `project:{project_name}`

**Доступні проекти**:
- `project:fixaro` - Fixaro WordPress SaaS платформа
- `project:lwm-hub` - LWM Hub (German VPS, Hetzner)
- `project:mcp-memory` - MCP Memory Service setup
- `project:global` - Загальні записи (особиста інформація, налаштування)

**Приклади**:
```
project:fixaro - всі записи Fixaro проекту
project:lwm-hub - LWM Hub інфраструктура
project:global - особиста інформація, преференції
```

### 2. **Category Tags** (типи інформації)

Формат: `category:{category_name}`

**Основні категорії**:
- `category:person` - Інформація про людей (вас, команду)
- `category:deployment` - Деплой, CI/CD, інфраструктура
- `category:development` - Розробка, код, workflow
- `category:task` - Завдання, todo, milestones
- `category:configuration` - Конфіги, credentials, налаштування
- `category:session` - Сесії роботи, досягнення
- `category:documentation` - Документація, guides
- `category:architecture` - Архітектура, tech stack
- `category:bug-fix` - Виправлення багів
- `category:feature` - Нові фічі, функціонал

### 3. **Tech Stack Tags** (технології)

Formат: `tech:{technology}`

**Приклади**:
- `tech:wordpress` - WordPress
- `tech:supabase` - Supabase
- `tech:vercel` - Vercel
- `tech:cloudflare` - Cloudflare
- `tech:react` - React
- `tech:typescript` - TypeScript
- `tech:postgresql` - PostgreSQL

### 4. **Status Tags** (статус)

Формат: `status:{status}`

**Можливі статуси**:
- `status:active` - Активний проект/завдання
- `status:completed` - Завершено
- `status:archived` - Архівовано
- `status:in-progress` - В процесі
- `status:blocked` - Заблоковано

### 5. **Date Tags** (для швидкого пошуку по датах)

Формат: `date:{year}-{month}` або `date:{year}-{month}-{day}`

**Приклади**:
- `date:2025-11` - Листопад 2025
- `date:2025-11-09` - Конкретна дата

---

## 🔍 Приклади використання

### Запис 1: Інформація про вас (загальний)
```json
{
  "content": "Roman Siromskyi - Developer, Ukrainian, works remotely",
  "tags": "project:global,category:person,status:active,date:2025-11",
  "type": "person"
}
```

**Пошук**:
- Всі особисті дані: `project:global AND category:person`
- Активні записи: `status:active`

---

### Запис 2: Fixaro deployment (проект)
```json
{
  "content": "Vercel deployment configured for Fixaro with aliases",
  "tags": "project:fixaro,category:deployment,tech:vercel,tech:wordpress,status:completed,date:2025-10",
  "type": "deployment_setup"
}
```

**Пошук**:
- Всі Fixaro записи: `project:fixaro`
- Deployment в Fixaro: `project:fixaro AND category:deployment`
- Всі Vercel configs: `tech:vercel`

---

### Запис 3: LWM Hub infrastructure (інший проект)
```json
{
  "content": "German VPS server on Hetzner with VPN infrastructure",
  "tags": "project:lwm-hub,category:infrastructure,tech:hetzner,status:active,date:2025-11",
  "type": "infrastructure"
}
```

**Пошук**:
- Всі LWM Hub записи: `project:lwm-hub`
- Infrastructure по всіх проектах: `category:infrastructure`

---

### Запис 4: Fixaro session work (сесія роботи)
```json
{
  "content": "Session 2025-10-29: Fixed hero section, updated git workflow, deployed to Vercel",
  "tags": "project:fixaro,category:session,category:development,status:completed,date:2025-10-29",
  "type": "work_session"
}
```

**Пошук**:
- Всі сесії Fixaro: `project:fixaro AND category:session`
- Сесії по всіх проектах: `category:session`
- Конкретна дата: `date:2025-10-29`

---

## 🎯 Правила тегування

### Обов'язкові теги для кожного запису:

1. **Project tag** (один) - `project:{name}`
2. **Category tag** (один або більше) - `category:{type}`
3. **Date tag** (один) - `date:{year}-{month}` або `date:{year}-{month}-{day}`

### Опціональні теги:

4. **Tech stack** (якщо релевантно) - `tech:{technology}`
5. **Status** (якщо релевантно) - `status:{status}`

---

## 🔎 Патерни пошуку

### По проекту:
```python
# Всі записи Fixaro
mcp__memory__search_by_tag(tags=["project:fixaro"])

# Deployment записи Fixaro
mcp__memory__search_by_tag(tags=["project:fixaro", "category:deployment"])
```

### По категорії (через всі проекти):
```python
# Всі deployment записи
mcp__memory__search_by_tag(tags=["category:deployment"])

# Всі сесії роботи
mcp__memory__search_by_tag(tags=["category:session"])
```

### По технології:
```python
# Всі записи з Vercel
mcp__memory__search_by_tag(tags=["tech:vercel"])

# Vercel в Fixaro
mcp__memory__search_by_tag(tags=["project:fixaro", "tech:vercel"])
```

### По даті:
```python
# Всі записи за листопад 2025
mcp__memory__search_by_tag(tags=["date:2025-11"])

# Fixaro записи за жовтень
mcp__memory__search_by_tag(tags=["project:fixaro", "date:2025-10"])
```

### Комбінований пошук:
```python
# Активні Fixaro tasks
mcp__memory__search_by_tag(tags=["project:fixaro", "category:task", "status:active"])

# Завершені deployments за останній місяць
mcp__memory__search_by_tag(tags=["category:deployment", "status:completed", "date:2025-11"])
```

---

## 📊 Переваги цієї системи

1. **Ізоляція проектів**: `project:{name}` тег дозволяє працювати тільки з одним проектом
2. **Кросс-проектний пошук**: `category:{type}` дозволяє знайти всі deployment по всіх проектах
3. **Технологічний стек**: `tech:{name}` дозволяє знайти всі записи по конкретній технології
4. **Часові рамки**: `date:{yyyy-mm}` дозволяє фільтрувати по датах
5. **Гнучкість**: Комбінування тегів для точного пошуку

---

## 🆕 Додавання нових проектів

Коли починаєте новий проект:

1. Додайте новий `project:{name}` тег
2. Використовуйте існуючі `category:*` теги
3. Додайте релевантні `tech:*` теги
4. Завжди включайте `date:*` тег

**Приклад нового проекту**:
```json
{
  "content": "New project XYZ - React SaaS application",
  "tags": "project:xyz,category:architecture,tech:react,tech:typescript,status:active,date:2025-11",
  "type": "project"
}
```

---

## 📝 Міграція існуючих даних

### Автоматичне визначення проектів:

**По ключовим словам в імені/контенті**:
- "Fixaro", "fixaro" → `project:fixaro`
- "LWM_Hub", "lwm-hub", "Hetzner" → `project:lwm-hub`
- "Memory_MCP", "mcp-memory" → `project:mcp-memory`
- "Roman", "preferences", особиста інфо → `project:global`

**По entityType**:
- `entityType: "person"` → `category:person`
- `entityType: "task"` → `category:task`
- `entityType: "project"` → `category:architecture`
- `entityType: "deployment"` → `category:deployment`
- `entityType: "session"` → `category:session`

**По технологіям в observations**:
- "Vercel" → `tech:vercel`
- "Supabase" → `tech:supabase`
- "WordPress" → `tech:wordpress`
- "Cloudflare" → `tech:cloudflare`

---

**Готово! Система тегування створена!** 🎯

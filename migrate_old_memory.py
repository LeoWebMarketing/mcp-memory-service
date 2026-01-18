#!/usr/bin/env python3
"""
Memory Migration Script
Migrates data from old Memory MCP (JSONL entities) to new MCP Memory Service with automatic tagging.
"""

import json
import os
import sys
import re
from datetime import datetime
from typing import Dict, List, Set
from pathlib import Path

# Add mcp-memory to path
sys.path.insert(0, str(Path(__file__).parent))

def detect_project(name: str, observations: List[str], entity_type: str) -> str:
    """Detect project from entity name and observations."""
    content = f"{name} {' '.join(observations)} {entity_type}".lower()

    # Project detection patterns
    if any(keyword in content for keyword in ['fixaro', 'fixaro-']):
        return 'fixaro'
    if any(keyword in content for keyword in ['lwm_hub', 'lwm-hub', 'hetzner', 'german vps']):
        return 'lwm-hub'
    if any(keyword in content for keyword in ['memory_mcp', 'mcp-memory', 'doobidoo']):
        return 'mcp-memory'
    if any(keyword in content for keyword in ['roman', 'siromskyi', 'preferences', 'personal']):
        return 'global'

    # Default to global for unclear projects
    return 'global'

def detect_category(entity_type: str, name: str, observations: List[str]) -> List[str]:
    """Detect category tags from entity type and content."""
    categories = []
    content = f"{name} {' '.join(observations)}".lower()

    # Direct entity type mapping
    type_mapping = {
        'person': 'person',
        'task': 'task',
        'project': 'architecture',
        'deployment': 'deployment',
        'session': 'session',
        'work_session': 'session',
        'documentation': 'documentation',
        'configuration': 'configuration',
        'infrastructure': 'infrastructure',
        'feature': 'feature',
        'milestone': 'feature',
        'architecture': 'architecture',
        'implementation': 'development',
        'verification': 'development',
    }

    if entity_type and entity_type.lower() in type_mapping:
        categories.append(type_mapping[entity_type.lower()])

    # Content-based detection
    if any(keyword in content for keyword in ['deploy', 'deployment', 'vercel', 'production']):
        if 'deployment' not in categories:
            categories.append('deployment')

    if any(keyword in content for keyword in ['session', 'work session', 'completed today']):
        if 'session' not in categories:
            categories.append('session')

    if any(keyword in content for keyword in ['bug', 'fix', 'fixed', 'error', 'issue']):
        if 'bug-fix' not in categories:
            categories.append('bug-fix')

    if any(keyword in content for keyword in ['task', 'todo', 'milestone', 'checklist']):
        if 'task' not in categories:
            categories.append('task')

    if any(keyword in content for keyword in ['config', 'credential', 'setup', 'environment']):
        if 'configuration' not in categories:
            categories.append('configuration')

    # Default to development if no category detected
    if not categories:
        categories.append('development')

    return categories

def detect_tech_stack(observations: List[str]) -> List[str]:
    """Detect technology stack from observations."""
    tech_tags = []
    content = ' '.join(observations).lower()

    tech_keywords = {
        'wordpress': 'wordpress',
        'supabase': 'supabase',
        'vercel': 'vercel',
        'cloudflare': 'cloudflare',
        'react': 'react',
        'typescript': 'typescript',
        'postgresql': 'postgresql',
        'postgres': 'postgresql',
        'hetzner': 'hetzner',
        'orioledb': 'orioledb',
        'd1': 'cloudflare-d1',
        'vectorize': 'cloudflare-vectorize',
    }

    for keyword, tag in tech_keywords.items():
        if keyword in content:
            tech_tags.append(f'tech:{tag}')

    return tech_tags

def detect_status(observations: List[str], name: str) -> str:
    """Detect status from observations."""
    content = f"{name} {' '.join(observations)}".lower()

    if any(keyword in content for keyword in ['completed', 'done', 'finished', 'deployed']):
        return 'status:completed'
    if any(keyword in content for keyword in ['in progress', 'working on', 'developing']):
        return 'status:in-progress'
    if any(keyword in content for keyword in ['blocked', 'waiting', 'pending']):
        return 'status:blocked'
    if any(keyword in content for keyword in ['archived', 'deprecated', 'old']):
        return 'status:archived'

    # Default to active
    return 'status:active'

def extract_date(observations: List[str], name: str) -> str:
    """Extract date from observations or name."""
    content = f"{name} {' '.join(observations)}"

    # Try to find date patterns (YYYY-MM-DD or YYYY-MM)
    date_patterns = [
        r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
        r'(\d{4}-\d{2})',        # YYYY-MM
        r'session[_-](\d{4}-\d{2}-\d{2})',  # session_YYYY-MM-DD
    ]

    for pattern in date_patterns:
        match = re.search(pattern, content)
        if match:
            date_str = match.group(1)
            # Return YYYY-MM format
            if len(date_str) == 7:  # YYYY-MM
                return f'date:{date_str}'
            elif len(date_str) == 10:  # YYYY-MM-DD
                return f'date:{date_str[:7]}'  # Truncate to YYYY-MM

    # Default to current month if no date found
    return f'date:{datetime.now().strftime("%Y-%m")}'

def determine_memory_type(entity_type: str, categories: List[str]) -> str:
    """Determine memory type based on entity type and categories."""
    # Use first category as type, fallback to entity_type
    if categories:
        category = categories[0]
        if category == 'person':
            return 'reference'
        elif category in ['deployment', 'configuration']:
            return 'configuration'
        elif category == 'session':
            return 'session'
        elif category == 'task':
            return 'implementation'
        elif category == 'documentation':
            return 'documentation'
        elif category == 'bug-fix':
            return 'fix'
        elif category == 'feature':
            return 'feature'
        elif category == 'architecture':
            return 'architecture'
        else:
            return 'note'

    # Fallback to entity_type
    if entity_type:
        return entity_type.lower()

    return 'note'

def migrate_entity(entity: Dict, source_label: str = "") -> Dict:
    """Migrate a single entity to new format with automatic tagging."""
    name = entity.get('name', 'Unknown')
    observations = entity.get('observations', [])
    entity_type = entity.get('entityType', '')

    # Detect project
    project = detect_project(name, observations, entity_type)

    # Detect categories
    categories = detect_category(entity_type, name, observations)

    # Detect tech stack
    tech_tags = detect_tech_stack(observations)

    # Detect status
    status = detect_status(observations, name)

    # Extract date
    date_tag = extract_date(observations, name)

    # Build content
    content_parts = [name]
    if observations:
        content_parts.extend(observations)
    content = '. '.join(content_parts)

    # Build tags
    tags = [f'project:{project}']
    tags.extend([f'category:{cat}' for cat in categories])
    tags.extend(tech_tags)
    tags.append(status)
    tags.append(date_tag)

    # Add source label if provided
    if source_label:
        tags.append(f'source:{source_label}')

    # Determine memory type
    memory_type = determine_memory_type(entity_type, categories)

    return {
        'content': content,
        'tags': ','.join(tags),
        'type': memory_type,
        'original_name': name,
        'original_type': entity_type or 'unknown'
    }

def load_jsonl(file_path: str) -> List[Dict]:
    """Load entities from JSONL file."""
    entities = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entity = json.loads(line)
                    if entity.get('type') == 'entity':
                        entities.append(entity)
                except json.JSONDecodeError as e:
                    print(f"Warning: Could not parse line: {e}")
                    continue
    return entities

def save_migration_preview(migrated_data: List[Dict], output_path: str):
    """Save migrated data preview as JSONL."""
    with open(output_path, 'w', encoding='utf-8') as f:
        for item in migrated_data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')

def main():
    print("=" * 80)
    print("Memory Migration Script")
    print("=" * 80)
    print()

    # File paths
    main_memory = Path.home() / 'memory.jsonl'
    fixaro_memory = Path.home() / 'Documents/GitHub/Fixaro/.memory/fixaro-memory.jsonl'
    output_dir = Path.home() / 'mcp-memory/migration'

    # Create output directory
    output_dir.mkdir(exist_ok=True)

    # Migrate main memory
    print(f"📁 Loading main memory: {main_memory}")
    if main_memory.exists():
        main_entities = load_jsonl(str(main_memory))
        print(f"   Found {len(main_entities)} entities")

        print("🔄 Migrating main memory...")
        main_migrated = [migrate_entity(e, 'main-memory') for e in main_entities]

        main_output = output_dir / 'main_memory_migrated.jsonl'
        save_migration_preview(main_migrated, str(main_output))
        print(f"   ✅ Saved to: {main_output}")
        print()
    else:
        print(f"   ⚠️  File not found: {main_memory}")
        main_migrated = []

    # Migrate Fixaro memory
    print(f"📁 Loading Fixaro memory: {fixaro_memory}")
    if fixaro_memory.exists():
        fixaro_entities = load_jsonl(str(fixaro_memory))
        print(f"   Found {len(fixaro_entities)} entities")

        print("🔄 Migrating Fixaro memory...")
        fixaro_migrated = [migrate_entity(e, 'fixaro-memory') for e in fixaro_entities]

        fixaro_output = output_dir / 'fixaro_memory_migrated.jsonl'
        save_migration_preview(fixaro_migrated, str(fixaro_output))
        print(f"   ✅ Saved to: {fixaro_output}")
        print()
    else:
        print(f"   ⚠️  File not found: {fixaro_memory}")
        fixaro_migrated = []

    # Statistics
    print("=" * 80)
    print("📊 Migration Statistics")
    print("=" * 80)
    print(f"Main memory entities: {len(main_migrated)}")
    print(f"Fixaro memory entities: {len(fixaro_migrated)}")
    print(f"Total entities: {len(main_migrated) + len(fixaro_migrated)}")
    print()

    # Project distribution
    all_migrated = main_migrated + fixaro_migrated
    projects = {}
    for item in all_migrated:
        project_tags = [t for t in item['tags'].split(',') if t.startswith('project:')]
        for tag in project_tags:
            project = tag.split(':')[1]
            projects[project] = projects.get(project, 0) + 1

    print("📦 Project distribution:")
    for project, count in sorted(projects.items(), key=lambda x: x[1], reverse=True):
        print(f"   {project}: {count} entities")
    print()

    # Next steps
    print("=" * 80)
    print("🚀 Next Steps")
    print("=" * 80)
    print("1. Review migrated data:")
    print(f"   cat {output_dir}/main_memory_migrated.jsonl | jq")
    print(f"   cat {output_dir}/fixaro_memory_migrated.jsonl | jq")
    print()
    print("2. Import to MCP Memory (run import script)")
    print("   python ~/mcp-memory/import_migrated_data.py")
    print()
    print("3. Test semantic search with filtering:")
    print("   # Search Fixaro only")
    print("   mcp__memory__search_by_tag(['project:fixaro'])")
    print()
    print("   # Search all deployments")
    print("   mcp__memory__search_by_tag(['category:deployment'])")
    print()

if __name__ == '__main__':
    main()

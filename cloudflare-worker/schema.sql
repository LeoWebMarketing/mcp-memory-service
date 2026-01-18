-- MCP Memory Service - D1 Database Schema

CREATE TABLE IF NOT EXISTS memories (
  id TEXT PRIMARY KEY,
  content TEXT NOT NULL,
  tags TEXT, -- JSON array as string
  created_at INTEGER NOT NULL,
  updated_at INTEGER DEFAULT (strftime('%s', 'now') * 1000)
);

CREATE INDEX IF NOT EXISTS idx_created_at ON memories(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tags ON memories(tags);

# Copyright 2024 Heinrich Krupp
# Memory MCP Supercharge - Phase 3: Query Understanding
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""
Query understanding without AI - regex and rules based.

Parses natural language queries to extract:
- Project names (freel, mind-space, fixaro, etc.)
- Memory types (task, error, decision, etc.)
- Time expressions (last week, 3 days ago, etc.)
- Status indicators (completed, pending, etc.)
- Abbreviation expansions (wp → wordpress, etc.)
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple, Union, Callable
from dataclasses import dataclass


@dataclass
class ParsedQuery:
    """Result of query parsing."""
    original: str
    cleaned: str
    filters: Dict[str, str]
    time_filter: Optional[Dict[str, Any]]
    expanded_terms: List[str]
    intent: str


class QueryParser:
    """Parse and understand search queries without AI."""

    # Known projects (can be extended dynamically from Memory MCP tags)
    PROJECTS = {
        "freel": ["freel", "freel.ee", "freelee"],
        "mind-space": ["mind-space", "mindspace", "mind space", "healing", "healing-space"],
        "fixaro": ["fixaro"],
        "mcp-memory": ["mcp-memory", "memory-mcp", "mcp memory", "memory mcp"],
    }

    # Memory type indicators
    TYPE_INDICATORS = {
        "task": ["task", "todo", "завдання", "implement", "add", "create", "build", "make"],
        "error": ["error", "bug", "issue", "problem", "fix", "broken", "fail", "failed", "помилка"],
        "decision": ["decision", "decided", "chose", "рішення", "вибрали", "architecture"],
        "session": ["session", "сесія", "progress", "working on"],
        "config": ["config", "configuration", "setup", "settings", "налаштування"],
        "deployment": ["deploy", "deployment", "release", "publish", "деплой"],
        "documentation": ["doc", "docs", "documentation", "readme", "документація"],
        # NEW: User-requested types for better organization
        "client-chat": ["client", "клієнт", "chat with client", "чат з клієнтом", "customer", "замовник", "call", "дзвінок", "meeting", "зустріч"],
        "update": ["update", "апдейт", "оновлення", "changelog", "changes", "зміни", "news", "новини", "progress update"],
        "session-log": ["session log", "лог сесії", "what i did", "що робив", "work log", "робочий лог", "activity", "активність", "daily log", "щоденник"],
    }

    # Status indicators
    STATUS_INDICATORS = {
        "completed": ["done", "completed", "finished", "завершено", "ready", "resolved"],
        "in-progress": ["working", "in progress", "started", "почато", "wip"],
        "pending": ["pending", "todo", "planned", "заплановано", "queued"],
        "blocked": ["blocked", "stuck", "waiting", "чекає"],
    }

    # Time expression patterns (relative)
    # Each tuple: (regex_pattern, result_or_callable)
    # Result can be dict or callable that takes match and returns dict
    TIME_PATTERNS: List[Tuple[str, Union[Dict[str, Any], Callable[[Any], Dict[str, Any]]]]] = [
        (r"\btoday\b", {"days": 0}),
        (r"\byesterday\b", {"days": 1}),
        (r"\blast\s*week\b", {"days": 7}),
        (r"\bthis\s*week\b", {"days": 7}),
        (r"\blast\s*month\b", {"days": 30}),
        (r"\bthis\s*month\b", {"days": 30}),
        (r"\blast\s*year\b", {"days": 365}),
        (r"(\d+)\s*days?\s*ago\b", lambda m: {"days": int(m.group(1))}),
        (r"(\d+)\s*weeks?\s*ago\b", lambda m: {"days": int(m.group(1)) * 7}),
        (r"(\d+)\s*months?\s*ago\b", lambda m: {"days": int(m.group(1)) * 30}),
        # Ukrainian patterns
        (r"\bсьогодні\b", {"days": 0}),
        (r"\bвчора\b", {"days": 1}),
        (r"\bцього\s*тижня\b", {"days": 7}),
        (r"\bминулого\s*тижня\b", {"days": 7}),
        (r"\bцього\s*місяця\b", {"days": 30}),
        (r"\bминулого\s*місяця\b", {"days": 30}),
    ]

    # Abbreviation expansions
    EXPANSIONS = {
        "wp": "wordpress",
        "cf": "cloudflare",
        "js": "javascript",
        "ts": "typescript",
        "py": "python",
        "db": "database",
        "api": "api endpoint",
        "ui": "user interface",
        "ux": "user experience",
        "ssh": "ssh connection server",
        "ftp": "ftp file transfer",
        "dns": "dns domain",
        "ssl": "ssl https certificate",
        "mcp": "model context protocol",
        "d1": "cloudflare d1 database",
        "kv": "key value store",
    }

    def parse(self, query: str) -> ParsedQuery:
        """
        Parse a search query and extract structured information.

        Args:
            query: Raw search query

        Returns:
            ParsedQuery with filters, time, expansions
        """
        original = query
        query_lower = query.lower()
        filters: Dict[str, str] = {}
        time_filter: Optional[Dict[str, Any]] = None
        expanded_terms: List[str] = []

        # 1. Detect project
        project, query_lower = self._extract_project(query_lower)
        if project:
            filters["project"] = project

        # 2. Detect type
        memory_type = self._detect_type(query_lower)
        if memory_type:
            filters["type"] = memory_type

        # 3. Detect status
        status = self._detect_status(query_lower)
        if status:
            filters["status"] = status

        # 4. Extract time filter
        time_filter, query_lower = self._extract_time(query_lower)

        # 5. Expand abbreviations
        expanded_terms = self._expand_abbreviations(query_lower)

        # 6. Clean query (remove filter terms)
        cleaned = self._clean_query(query_lower, filters)

        # 7. Detect intent
        intent = self._detect_intent(query_lower, filters)

        return ParsedQuery(
            original=original,
            cleaned=cleaned,
            filters=filters,
            time_filter=time_filter,
            expanded_terms=expanded_terms,
            intent=intent
        )

    def _extract_project(self, query: str) -> Tuple[Optional[str], str]:
        """Extract project name from query."""
        for project_id, aliases in self.PROJECTS.items():
            for alias in aliases:
                # Match as whole word
                pattern = r'\b' + re.escape(alias) + r'\b'
                if re.search(pattern, query, re.IGNORECASE):
                    # Remove from query
                    query = re.sub(pattern, '', query, flags=re.IGNORECASE)
                    return project_id, query.strip()
        return None, query

    def _detect_type(self, query: str) -> Optional[str]:
        """Detect memory type from query terms."""
        for type_name, indicators in self.TYPE_INDICATORS.items():
            for indicator in indicators:
                pattern = r'\b' + re.escape(indicator) + r'\b'
                if re.search(pattern, query, re.IGNORECASE):
                    return type_name
        return None

    def _detect_status(self, query: str) -> Optional[str]:
        """Detect status from query terms."""
        for status, indicators in self.STATUS_INDICATORS.items():
            for indicator in indicators:
                pattern = r'\b' + re.escape(indicator) + r'\b'
                if re.search(pattern, query, re.IGNORECASE):
                    return status
        return None

    def _extract_time(self, query: str) -> Tuple[Optional[Dict[str, Any]], str]:
        """Extract time filter from query."""
        for pattern, result in self.TIME_PATTERNS:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                # Get time filter - result is either a dict or callable
                time_filter: Dict[str, Any]
                if callable(result):
                    time_filter = dict(result(match))
                else:
                    time_filter = dict(result)

                # Calculate actual date using timezone-aware datetime
                days = int(time_filter.get("days", 0))
                time_filter["start_date"] = (
                    datetime.now(timezone.utc) - timedelta(days=days)
                ).strftime("%Y-%m-%d")

                # Remove from query
                query = re.sub(pattern, '', query, flags=re.IGNORECASE)
                return time_filter, query.strip()

        return None, query

    def _expand_abbreviations(self, query: str) -> List[str]:
        """Expand known abbreviations."""
        expanded = []
        words = query.lower().split()

        for word in words:
            # Clean word from punctuation
            clean_word = re.sub(r'[^\w]', '', word)
            if clean_word in self.EXPANSIONS:
                expanded.append(self.EXPANSIONS[clean_word])

        return expanded

    def _clean_query(self, query: str, filters: Dict[str, str]) -> str:
        """Remove filter-related terms from query."""
        # Remove type indicators if type was detected
        if "type" in filters:
            for indicator in self.TYPE_INDICATORS.get(filters["type"], []):
                pattern = r'\b' + re.escape(indicator) + r'\b'
                query = re.sub(pattern, '', query, flags=re.IGNORECASE)

        # Remove status indicators if status was detected
        if "status" in filters:
            for indicator in self.STATUS_INDICATORS.get(filters["status"], []):
                pattern = r'\b' + re.escape(indicator) + r'\b'
                query = re.sub(pattern, '', query, flags=re.IGNORECASE)

        # Clean up whitespace
        return ' '.join(query.split())

    def _detect_intent(self, query: str, filters: Dict[str, str]) -> str:
        """Detect search intent."""
        query_lower = query.lower()

        if "error" in query_lower or filters.get("type") == "error":
            return "find_error"
        if "how to" in query_lower or "як" in query_lower:
            return "find_howto"
        if filters.get("status") == "pending":
            return "find_todo"
        if "recent" in query_lower or "last" in query_lower or "latest" in query_lower:
            return "find_recent"
        if "all" in query_lower:
            return "find_all"
        return "general_search"

    def add_project(self, project_id: str, aliases: List[str]) -> None:
        """
        Dynamically add a project to the parser.

        Args:
            project_id: Unique project identifier
            aliases: List of names/aliases that refer to this project
        """
        self.PROJECTS[project_id] = aliases

    def load_projects_from_tags(self, tags: List[str]) -> int:
        """
        Load projects from Memory MCP tags.

        Args:
            tags: List of tags like "project:freel", "project:mind-space"

        Returns:
            Number of projects added
        """
        added = 0
        for tag in tags:
            if tag.startswith("project:"):
                project_name = tag.split(":", 1)[1]
                if project_name not in self.PROJECTS:
                    # Add as both ID and alias variants
                    self.PROJECTS[project_name] = [
                        project_name,
                        project_name.replace("-", " "),
                        project_name.replace("-", ""),
                    ]
                    added += 1
        return added


# Singleton instance for convenience
query_parser = QueryParser()

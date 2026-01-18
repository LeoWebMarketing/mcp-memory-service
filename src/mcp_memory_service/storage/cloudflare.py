# Copyright 2024 Heinrich Krupp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Cloudflare storage backend for MCP Memory Service.
Provides cloud-native storage using Vectorize, D1, and R2.
"""

import json
import logging
import hashlib
import asyncio
import time
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone, timedelta
import httpx

from .base import MemoryStorage
from ..models.memory import Memory, MemoryQueryResult
from ..utils.hashing import generate_content_hash
from ..config import CLOUDFLARE_MAX_CONTENT_LENGTH

logger = logging.getLogger(__name__)

class CloudflareStorage(MemoryStorage):
    """Cloudflare-based storage backend using Vectorize, D1, and R2."""

    # Content length limit from configuration
    _MAX_CONTENT_LENGTH = CLOUDFLARE_MAX_CONTENT_LENGTH

    @property
    def max_content_length(self) -> Optional[int]:
        """Maximum content length: 800 chars (BGE model 512 token limit)."""
        return self._MAX_CONTENT_LENGTH

    @property
    def supports_chunking(self) -> bool:
        """Cloudflare backend supports content chunking with metadata linking."""
        return True

    def __init__(self,
                 api_token: str,
                 account_id: str,
                 vectorize_index: str,
                 d1_database_id: str,
                 r2_bucket: Optional[str] = None,
                 embedding_model: str = "@cf/baai/bge-base-en-v1.5",
                 large_content_threshold: int = 1024 * 1024,  # 1MB
                 max_retries: int = 3,
                 base_delay: float = 1.0):
        """
        Initialize Cloudflare storage backend.

        Args:
            api_token: Cloudflare API token
            account_id: Cloudflare account ID
            vectorize_index: Vectorize index name
            d1_database_id: D1 database ID
            r2_bucket: Optional R2 bucket for large content
            embedding_model: Workers AI embedding model
            large_content_threshold: Size threshold for R2 storage
            max_retries: Maximum retry attempts for API calls
            base_delay: Base delay for exponential backoff
        """
        self.api_token = api_token
        self.account_id = account_id
        self.vectorize_index = vectorize_index
        self.d1_database_id = d1_database_id
        self.r2_bucket = r2_bucket
        self.embedding_model = embedding_model
        self.large_content_threshold = large_content_threshold
        self.max_retries = max_retries
        self.base_delay = base_delay

        # API endpoints
        self.base_url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
        self.vectorize_url = f"{self.base_url}/vectorize/v2/indexes/{vectorize_index}"
        self.d1_url = f"{self.base_url}/d1/database/{d1_database_id}"
        self.ai_url = f"{self.base_url}/ai/run/{embedding_model}"

        if r2_bucket:
            self.r2_url = f"{self.base_url}/r2/buckets/{r2_bucket}/objects"

        # HTTP client with connection pooling
        self.client = None
        self._initialized = False

        # Embedding cache for performance
        self._embedding_cache = {}
        self._cache_max_size = 1000

        # Auto-linking configuration (v9.0)
        self.auto_link_enabled = True  # Enable auto-linking for new memories
        self.auto_link_threshold = 0.80  # Minimum similarity to create edge
        self.auto_link_max_edges = 3  # Max edges per new memory

    # ========== Structured Metadata Helpers ==========

    def _extract_tag_value(self, tags_str: str, prefix: str) -> Optional[str]:
        """Extract value from tag with given prefix (e.g., 'project:' -> 'freel')."""
        if not tags_str:
            return None
        for tag in tags_str.split(","):
            tag = tag.strip()
            if tag.startswith(f"{prefix}:"):
                return tag.split(":", 1)[1]
        return None

    def _extract_project_from_tags(self, tags_str: str) -> Optional[str]:
        """Extract project name from tags string."""
        return self._extract_tag_value(tags_str, "project")

    def _extract_status_from_tags(self, tags_str: str) -> Optional[str]:
        """Extract status from tags string."""
        return self._extract_tag_value(tags_str, "status")

    def _extract_type_from_tags(self, tags_str: str) -> Optional[str]:
        """Extract type from tags string."""
        return self._extract_tag_value(tags_str, "type")

    def _extract_priority_from_tags(self, tags_str: str) -> Optional[str]:
        """Extract priority from tags string."""
        return self._extract_tag_value(tags_str, "priority")

    def _extract_category_from_tags(self, tags_str: str) -> Optional[str]:
        """Extract category from tags string."""
        return self._extract_tag_value(tags_str, "category")

    def _extract_tech_from_tags(self, tags_str: str) -> Optional[str]:
        """Extract tech stack from tags string."""
        return self._extract_tag_value(tags_str, "tech")

    def _has_tag(self, tags_str: str, tag_name: str) -> bool:
        """Check if a specific tag exists."""
        if not tags_str:
            return False
        return tag_name in [t.strip() for t in tags_str.split(",")]

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client with connection pooling."""
        if self.client is None:
            headers = {
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json"
            }
            self.client = httpx.AsyncClient(
                headers=headers,
                timeout=httpx.Timeout(30.0),
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5)
            )
        return self.client
    
    async def _retry_request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Make HTTP request with exponential backoff retry logic."""
        client = await self._get_client()
        
        for attempt in range(self.max_retries + 1):
            try:
                response = await client.request(method, url, **kwargs)
                
                # Handle rate limiting
                if response.status_code == 429:
                    if attempt < self.max_retries:
                        delay = self.base_delay * (2 ** attempt)
                        logger.warning(f"Rate limited, retrying in {delay}s (attempt {attempt + 1}/{self.max_retries + 1})")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        raise httpx.HTTPError(f"Rate limited after {self.max_retries} retries")
                
                # Handle server errors
                if response.status_code >= 500:
                    if attempt < self.max_retries:
                        delay = self.base_delay * (2 ** attempt)
                        logger.warning(f"Server error {response.status_code}, retrying in {delay}s")
                        await asyncio.sleep(delay)
                        continue
                
                response.raise_for_status()
                return response
                
            except (httpx.NetworkError, httpx.TimeoutException) as e:
                if attempt < self.max_retries:
                    delay = self.base_delay * (2 ** attempt)
                    logger.warning(f"Network error: {e}, retrying in {delay}s")
                    await asyncio.sleep(delay)
                    continue
                raise
        
        raise httpx.HTTPError(f"Failed after {self.max_retries} retries")
    
    async def _generate_embedding(self, text: str) -> List[float]:
        """Generate embedding using Workers AI or cache."""
        # Truncate text to fit model's context window
        # all-MiniLM-L6-v2 has ~512 token limit (~1500 chars safely)
        MAX_EMBEDDING_CHARS = 1500
        truncated_text = text[:MAX_EMBEDDING_CHARS] if len(text) > MAX_EMBEDDING_CHARS else text

        # Check cache first (use truncated text for cache key)
        text_hash = hashlib.sha256(truncated_text.encode()).hexdigest()
        if text_hash in self._embedding_cache:
            return self._embedding_cache[text_hash]

        try:
            # Use Workers AI to generate embedding
            payload = {"text": [truncated_text]}
            response = await self._retry_request("POST", self.ai_url, json=payload)
            result = response.json()
            
            if result.get("success") and "result" in result:
                embedding = result["result"]["data"][0]
                
                # Cache the embedding (with size limit)
                if len(self._embedding_cache) >= self._cache_max_size:
                    # Remove oldest entry (simple FIFO)
                    oldest_key = next(iter(self._embedding_cache))
                    del self._embedding_cache[oldest_key]
                
                self._embedding_cache[text_hash] = embedding
                return embedding
            else:
                raise ValueError(f"Workers AI embedding failed: {result}")
                
        except Exception as e:
            logger.error(f"Failed to generate embedding with Workers AI: {e}")
            # TODO: Implement fallback to local sentence-transformers
            raise ValueError(f"Embedding generation failed: {e}")
    
    async def initialize(self) -> None:
        """Initialize the Cloudflare storage backend."""
        if self._initialized:
            return
        
        logger.info("Initializing Cloudflare storage backend...")
        
        try:
            # Initialize D1 database schema
            await self._initialize_d1_schema()
            
            # Verify Vectorize index exists
            await self._verify_vectorize_index()
            
            # Verify R2 bucket if configured
            if self.r2_bucket:
                await self._verify_r2_bucket()
            
            self._initialized = True
            logger.info("Cloudflare storage backend initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Cloudflare storage: {e}")
            raise
    
    async def _initialize_d1_schema(self) -> None:
        """Initialize D1 database schema."""
        schema_sql = """
        -- Memory metadata table
        CREATE TABLE IF NOT EXISTS memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_hash TEXT UNIQUE NOT NULL,
            content TEXT NOT NULL,
            memory_type TEXT,
            created_at REAL NOT NULL,
            created_at_iso TEXT NOT NULL,
            updated_at REAL,
            updated_at_iso TEXT,
            metadata_json TEXT,
            vector_id TEXT UNIQUE,
            content_size INTEGER DEFAULT 0,
            r2_key TEXT,
            -- Decay/Reinforcement fields (v9.0)
            strength REAL DEFAULT 1.0,
            access_count INTEGER DEFAULT 0,
            last_accessed TEXT
        );
        
        -- Tags table
        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
        
        -- Memory-tag relationships
        CREATE TABLE IF NOT EXISTS memory_tags (
            memory_id INTEGER,
            tag_id INTEGER,
            PRIMARY KEY (memory_id, tag_id),
            FOREIGN KEY (memory_id) REFERENCES memories(id) ON DELETE CASCADE,
            FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
        );
        
        -- Indexes for performance
        CREATE INDEX IF NOT EXISTS idx_memories_content_hash ON memories(content_hash);
        CREATE INDEX IF NOT EXISTS idx_memories_created_at ON memories(created_at);
        CREATE INDEX IF NOT EXISTS idx_memories_vector_id ON memories(vector_id);
        CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(name);

        -- === GRAPH RELATIONS (v9.0) ===
        -- Memory edges table for graph relations between memories
        CREATE TABLE IF NOT EXISTS memory_edges (
            id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
            source_hash TEXT NOT NULL,
            target_hash TEXT NOT NULL,
            edge_type TEXT NOT NULL,
            weight REAL DEFAULT 1.0,
            metadata_json TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(source_hash, target_hash, edge_type),
            FOREIGN KEY (source_hash) REFERENCES memories(content_hash) ON DELETE CASCADE,
            FOREIGN KEY (target_hash) REFERENCES memories(content_hash) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_edges_source ON memory_edges(source_hash);
        CREATE INDEX IF NOT EXISTS idx_edges_target ON memory_edges(target_hash);
        CREATE INDEX IF NOT EXISTS idx_edges_type ON memory_edges(edge_type);

        -- === SAFE ARCHIVE (v9.0) ===
        -- Archive table for soft-deleted memories (consolidation safety net)
        CREATE TABLE IF NOT EXISTS memory_archive (
            id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
            original_content_hash TEXT NOT NULL,
            content TEXT NOT NULL,
            memory_type TEXT,
            metadata_json TEXT,
            tags_json TEXT,
            archived_at TEXT DEFAULT (datetime('now')),
            archived_reason TEXT,
            consolidated_into TEXT,
            restored BOOLEAN DEFAULT FALSE,
            restored_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_archive_hash ON memory_archive(original_content_hash);
        CREATE INDEX IF NOT EXISTS idx_archive_consolidated ON memory_archive(consolidated_into);
        """
        
        payload = {"sql": schema_sql}
        response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
        result = response.json()
        
        if not result.get("success"):
            raise ValueError(f"Failed to initialize D1 schema: {result}")
    
    async def _verify_vectorize_index(self) -> None:
        """Verify Vectorize index exists and is accessible."""
        try:
            response = await self._retry_request("GET", f"{self.vectorize_url}")
            result = response.json()
            
            if not result.get("success"):
                raise ValueError(f"Vectorize index not accessible: {result}")
                
            logger.info(f"Vectorize index verified: {self.vectorize_index}")
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ValueError(f"Vectorize index '{self.vectorize_index}' not found")
            raise
    
    async def _verify_r2_bucket(self) -> None:
        """Verify R2 bucket exists and is accessible."""
        try:
            # Try to list objects (empty list is fine)
            response = await self._retry_request("GET", f"{self.r2_url}?max-keys=1")
            logger.info(f"R2 bucket verified: {self.r2_bucket}")
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ValueError(f"R2 bucket '{self.r2_bucket}' not found")
            raise
    
    async def store(self, memory: Memory) -> Tuple[bool, str]:
        """Store a memory in Cloudflare storage."""
        try:
            # Generate embedding for the content
            embedding = await self._generate_embedding(memory.content)
            
            # Determine storage strategy based on content size
            content_size = len(memory.content.encode('utf-8'))
            use_r2 = self.r2_bucket and content_size > self.large_content_threshold
            
            # Store large content in R2 if needed
            r2_key = None
            stored_content = memory.content
            
            if use_r2:
                r2_key = f"content/{memory.content_hash}.txt"
                await self._store_r2_content(r2_key, memory.content)
                stored_content = f"[R2 Content: {r2_key}]"  # Placeholder in D1
            
            # Store vector in Vectorize with enhanced metadata for filtering
            vector_id = memory.content_hash
            tags_str = ",".join(memory.tags) if memory.tags else ""
            created_at_iso = memory.created_at_iso or datetime.now().isoformat()

            vector_metadata = {
                # === Core fields ===
                "content_hash": memory.content_hash,
                "memory_type": memory.memory_type or "standard",
                "tags": tags_str,
                "created_at": created_at_iso,
                "date": created_at_iso[:10] if created_at_iso else None,  # YYYY-MM-DD

                # === Work/Projects ===
                "project": self._extract_tag_value(tags_str, "project"),
                "type": memory.memory_type or self._extract_tag_value(tags_str, "type"),
                "status": self._extract_tag_value(tags_str, "status"),
                "priority": self._extract_tag_value(tags_str, "priority"),
                "category": self._extract_tag_value(tags_str, "category"),
                "tech": self._extract_tag_value(tags_str, "tech"),

                # === Life Areas (Personal Knowledge Base) ===
                "area": self._extract_tag_value(tags_str, "area"),  # health, fitness, nutrition, finance, learning, work, personal
                "person": self._extract_tag_value(tags_str, "person"),  # trainer, doctor, friend, family
                "goal": self._extract_tag_value(tags_str, "goal"),  # weight-loss, muscle-gain, learn-spanish
                "metric": self._extract_tag_value(tags_str, "metric"),  # weight, calories, reps, sleep-hours
                "location": self._extract_tag_value(tags_str, "location"),  # gym, home, office, outdoor
                "mood": self._extract_tag_value(tags_str, "mood"),  # good, tired, energetic, stressed
                "energy": self._extract_tag_value(tags_str, "energy"),  # high, medium, low
                "source": self._extract_tag_value(tags_str, "source"),  # book, article, conversation, podcast, video
                "recurring": self._extract_tag_value(tags_str, "recurring"),  # daily, weekly, monthly
                "duration": self._extract_tag_value(tags_str, "duration"),  # 30min, 1h, 2h
                "cost": self._extract_tag_value(tags_str, "cost"),  # for financial tracking

                # === Boolean flags for quick filtering ===
                "is_important": self._has_tag(tags_str, "important"),
                "is_private": self._has_tag(tags_str, "private"),
                "is_task": self._has_tag(tags_str, "type:task") or memory.memory_type == "task",
                "is_error": self._has_tag(tags_str, "type:error") or memory.memory_type == "error",
                "is_decision": self._has_tag(tags_str, "type:decision") or memory.memory_type == "decision",
                "requires_action": self._has_tag(tags_str, "requires-action") or self._has_tag(tags_str, "action-required"),
                "is_health": self._has_tag(tags_str, "area:health") or self._has_tag(tags_str, "area:fitness") or self._has_tag(tags_str, "area:nutrition"),
                "is_financial": self._has_tag(tags_str, "area:finance") or self._has_tag(tags_str, "category:expense"),
            }

            await self._store_vectorize_vector(vector_id, embedding, vector_metadata)

            # Store metadata in D1
            await self._store_d1_memory(memory, vector_id, content_size, r2_key, stored_content)

            logger.info(f"Successfully stored memory: {memory.content_hash}")

            # Auto-link to similar memories if enabled
            if getattr(self, 'auto_link_enabled', True):
                await self._auto_link_memory(memory.content_hash, memory.content)

            return True, f"Memory stored successfully (vector_id: {vector_id})"

        except Exception as e:
            logger.error(f"Failed to store memory {memory.content_hash}: {e}")
            return False, f"Storage failed: {str(e)}"

    async def _auto_link_memory(self, content_hash: str, content: str) -> None:
        """
        Automatically create edges to similar memories after storing.

        This runs asynchronously after a memory is stored and creates
        'related_to' edges to semantically similar memories.
        Non-blocking: errors are logged but don't affect the store operation.
        """
        try:
            # Skip short content
            if len(content) < 50:
                return

            # Find similar memories
            similar_results = await self.search(
                query=content,
                n_results=self.auto_link_max_edges + 1
            )

            edges_created = 0
            for result in similar_results:
                # Skip self
                if result.memory.content_hash == content_hash:
                    continue

                # Check threshold
                if result.relevance_score < self.auto_link_threshold:
                    continue

                # Limit edges
                if edges_created >= self.auto_link_max_edges:
                    break

                # Check if edge already exists
                if await self.edge_exists(content_hash, result.memory.content_hash):
                    continue

                # Create edge
                success, _ = await self.link_memories(
                    source_hash=content_hash,
                    target_hash=result.memory.content_hash,
                    edge_type="related_to",
                    weight=result.relevance_score,
                    metadata={
                        "auto_generated": True,
                        "on_store": True,
                        "similarity": result.relevance_score
                    }
                )

                if success:
                    edges_created += 1
                    logger.debug(f"Auto-linked {content_hash[:8]} -> {result.memory.content_hash[:8]} (sim={result.relevance_score:.2f})")

            if edges_created > 0:
                logger.info(f"Auto-linked memory {content_hash[:8]} to {edges_created} similar memories")

        except Exception as e:
            # Log but don't fail - auto-linking is non-critical
            logger.warning(f"Auto-link failed for {content_hash[:8]}: {e}")

    async def _store_vectorize_vector(self, vector_id: str, embedding: List[float], metadata: Dict[str, Any]) -> None:
        """Store vector in Vectorize."""
        # Try without namespace first to isolate the issue
        vector_data = {
            "id": vector_id,
            "values": embedding,
            "metadata": metadata
        }
        
        # Convert to NDJSON format as required by the HTTP API
        import json
        ndjson_content = json.dumps(vector_data) + "\n"
        
        try:
            # Send as raw NDJSON data with correct Content-Type header
            client = await self._get_client()
            
            # Override headers for this specific request
            headers = {
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/x-ndjson"
            }
            
            response = await client.post(
                f"{self.vectorize_url}/upsert",
                content=ndjson_content.encode("utf-8"),
                headers=headers
            )
            
            # Log response status for debugging (avoid logging headers/body for security)
            logger.info(f"Vectorize response status: {response.status_code}")
            response_text = response.text
            if response.status_code != 200:
                # Only log response body on errors, and truncate to avoid credential exposure
                truncated_response = response_text[:200] + "..." if len(response_text) > 200 else response_text
                logger.warning(f"Vectorize error response (truncated): {truncated_response}")
            
            if response.status_code != 200:
                raise ValueError(f"HTTP {response.status_code}: {response_text}")
            
            result = response.json()
            if not result.get("success"):
                raise ValueError(f"Failed to store vector: {result}")
                
        except Exception as e:
            # Add more detailed error logging
            logger.error(f"Vectorize insert failed: {e}")
            logger.error(f"Vector data was: {vector_data}")
            logger.error(f"NDJSON content: {ndjson_content.strip()}")
            logger.error(f"URL was: {self.vectorize_url}/upsert")
            raise ValueError(f"Failed to store vector: {e}")
    
    async def _store_d1_memory(self, memory: Memory, vector_id: str, content_size: int, r2_key: Optional[str], stored_content: str) -> None:
        """Store memory metadata in D1."""
        # Insert memory record
        insert_sql = """
        INSERT INTO memories (
            content_hash, content, memory_type, created_at, created_at_iso,
            updated_at, updated_at_iso, metadata_json, vector_id, content_size, r2_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        now = time.time()
        now_iso = datetime.now().isoformat()
        
        params = [
            memory.content_hash,
            stored_content,
            memory.memory_type,
            memory.created_at or now,
            memory.created_at_iso or now_iso,
            memory.updated_at or now,
            memory.updated_at_iso or now_iso,
            json.dumps(memory.metadata) if memory.metadata else None,
            vector_id,
            content_size,
            r2_key
        ]
        
        payload = {"sql": insert_sql, "params": params}
        response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
        result = response.json()
        
        if not result.get("success"):
            raise ValueError(f"Failed to store memory in D1: {result}")
        
        # Store tags if present
        if memory.tags:
            memory_id = result["result"][0]["meta"]["last_row_id"]
            await self._store_d1_tags(memory_id, memory.tags)
    
    async def _store_d1_tags(self, memory_id: int, tags: List[str]) -> None:
        """Store tags for a memory in D1."""
        for tag in tags:
            # Insert tag if not exists
            tag_sql = "INSERT OR IGNORE INTO tags (name) VALUES (?)"
            payload = {"sql": tag_sql, "params": [tag]}
            await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            
            # Link tag to memory
            link_sql = """
            INSERT INTO memory_tags (memory_id, tag_id)
            SELECT ?, id FROM tags WHERE name = ?
            """
            payload = {"sql": link_sql, "params": [memory_id, tag]}
            await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
    
    async def _store_r2_content(self, key: str, content: str) -> None:
        """Store content in R2."""
        response = await self._retry_request(
            "PUT", 
            f"{self.r2_url}/{key}",
            content=content.encode('utf-8'),
            headers={"Content-Type": "text/plain"}
        )
        
        if response.status_code not in [200, 201]:
            raise ValueError(f"Failed to store content in R2: {response.status_code}")
    
    async def retrieve(self, query: str, n_results: int = 5) -> List[MemoryQueryResult]:
        """Retrieve memories by semantic search."""
        try:
            # Generate query embedding
            query_embedding = await self._generate_embedding(query)
            
            # Search Vectorize (without namespace for now)
            search_payload = {
                "vector": query_embedding,
                "topK": n_results,
                "returnMetadata": "all",
                "returnValues": False
            }
            
            response = await self._retry_request("POST", f"{self.vectorize_url}/query", json=search_payload)
            result = response.json()
            
            if not result.get("success"):
                raise ValueError(f"Vectorize query failed: {result}")
            
            matches = result.get("result", {}).get("matches", [])
            
            # Convert to MemoryQueryResult objects
            results = []
            for match in matches:
                memory = await self._load_memory_from_match(match)
                if memory:
                    query_result = MemoryQueryResult(
                        memory=memory,
                        relevance_score=match.get("score", 0.0)
                    )
                    results.append(query_result)
            
            logger.info(f"Retrieved {len(results)} memories for query")
            return results
            
        except Exception as e:
            logger.error(f"Failed to retrieve memories: {e}")
            return []
    
    async def _load_memory_from_match(self, match: Dict[str, Any]) -> Optional[Memory]:
        """Load full memory from Vectorize match."""
        try:
            vector_id = match.get("id")
            metadata = match.get("metadata", {})
            content_hash = metadata.get("content_hash")
            
            if not content_hash:
                logger.warning(f"No content_hash in vector metadata: {vector_id}")
                return None
            
            # Load from D1
            sql = "SELECT * FROM memories WHERE content_hash = ?"
            payload = {"sql": sql, "params": [content_hash]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()
            
            if not result.get("success") or not result.get("result", [{}])[0].get("results"):
                logger.warning(f"Memory not found in D1: {content_hash}")
                return None
            
            row = result["result"][0]["results"][0]
            
            # Load content from R2 if needed
            content = row["content"]
            if row.get("r2_key") and content.startswith("[R2 Content:"):
                content = await self._load_r2_content(row["r2_key"])
            
            # Load tags
            tags = await self._load_memory_tags(row["id"])
            
            # Reconstruct Memory object
            memory = Memory(
                content=content,
                content_hash=content_hash,
                tags=tags,
                memory_type=row.get("memory_type"),
                metadata=json.loads(row["metadata_json"]) if row.get("metadata_json") else {},
                created_at=row.get("created_at"),
                created_at_iso=row.get("created_at_iso"),
                updated_at=row.get("updated_at"),
                updated_at_iso=row.get("updated_at_iso")
            )
            
            return memory
            
        except Exception as e:
            logger.error(f"Failed to load memory from match: {e}")
            return None
    
    async def _load_r2_content(self, r2_key: str) -> str:
        """Load content from R2."""
        response = await self._retry_request("GET", f"{self.r2_url}/{r2_key}")
        return response.text
    
    async def _load_memory_tags(self, memory_id: int) -> List[str]:
        """Load tags for a memory from D1."""
        sql = """
        SELECT t.name FROM tags t
        JOIN memory_tags mt ON t.id = mt.tag_id
        WHERE mt.memory_id = ?
        """
        payload = {"sql": sql, "params": [memory_id]}
        response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
        result = response.json()
        
        if result.get("success") and result.get("result", [{}])[0].get("results"):
            return [row["name"] for row in result["result"][0]["results"]]
        
        return []
    
    async def search_by_tag(self, tags: List[str], time_start: Optional[float] = None) -> List[Memory]:
        """Search memories by tags with optional time filtering.

        Args:
            tags: List of tags to search for
            time_start: Optional Unix timestamp (in seconds) to filter memories created after this time

        Returns:
            List of Memory objects matching the tag criteria and time filter
        """
        try:
            if not tags:
                return []

            # Build SQL query for tag search
            placeholders = ",".join(["?"] * len(tags))
            params = list(tags)
            where_conditions = [f"t.name IN ({placeholders})"]

            # Add time filter if provided
            if time_start is not None:
                where_conditions.append("m.created_at >= ?")
                params.append(time_start)

            sql = (
                "SELECT DISTINCT m.* FROM memories m "
                "JOIN memory_tags mt ON m.id = mt.memory_id "
                "JOIN tags t ON mt.tag_id = t.id "
                f"WHERE {' AND '.join(where_conditions)} "
                "ORDER BY m.created_at DESC"
            )

            payload = {"sql": sql, "params": params}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if not result.get("success"):
                raise ValueError(f"D1 tag search failed: {result}")

            memories = []
            if result.get("result", [{}])[0].get("results"):
                for row in result["result"][0]["results"]:
                    memory = await self._load_memory_from_row(row)
                    if memory:
                        memories.append(memory)

            logger.info(f"Found {len(memories)} memories with tags: {tags}")
            return memories

        except Exception as e:
            logger.error(f"Failed to search by tags: {e}")
            return []
    
    async def _load_memory_from_row(self, row: Dict[str, Any]) -> Optional[Memory]:
        """Load memory from D1 row data."""
        try:
            # Load content from R2 if needed
            content = row["content"]
            if row.get("r2_key") and content.startswith("[R2 Content:"):
                content = await self._load_r2_content(row["r2_key"])
            
            # Load tags
            tags = await self._load_memory_tags(row["id"])
            
            memory = Memory(
                content=content,
                content_hash=row["content_hash"],
                tags=tags,
                memory_type=row.get("memory_type"),
                metadata=json.loads(row["metadata_json"]) if row.get("metadata_json") else {},
                created_at=row.get("created_at"),
                created_at_iso=row.get("created_at_iso"),
                updated_at=row.get("updated_at"),
                updated_at_iso=row.get("updated_at_iso")
            )
            
            return memory
            
        except Exception as e:
            logger.error(f"Failed to load memory from row: {e}")
            return None
    
    async def delete(self, content_hash: str) -> Tuple[bool, str]:
        """Delete a memory by its hash."""
        try:
            # Find memory in D1
            sql = "SELECT * FROM memories WHERE content_hash = ?"
            payload = {"sql": sql, "params": [content_hash]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()
            
            if not result.get("success") or not result.get("result", [{}])[0].get("results"):
                return False, f"Memory not found: {content_hash}"
            
            row = result["result"][0]["results"][0]
            memory_id = row["id"]
            vector_id = row.get("vector_id")
            r2_key = row.get("r2_key")
            
            # Delete from Vectorize
            if vector_id:
                await self._delete_vectorize_vector(vector_id)
            
            # Delete from R2 if present
            if r2_key:
                await self._delete_r2_content(r2_key)
            
            # Delete from D1 (tags will be cascade deleted)
            delete_sql = "DELETE FROM memories WHERE id = ?"
            payload = {"sql": delete_sql, "params": [memory_id]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()
            
            if not result.get("success"):
                raise ValueError(f"Failed to delete from D1: {result}")
            
            logger.info(f"Successfully deleted memory: {content_hash}")
            return True, "Memory deleted successfully"

        except Exception as e:
            logger.error(f"Failed to delete memory {content_hash}: {e}")
            return False, f"Deletion failed: {str(e)}"

    async def get_by_hash(self, content_hash: str) -> Optional[Memory]:
        """Get a memory by its content hash using direct O(1) D1 lookup."""
        try:
            # Query D1 for the memory
            sql = "SELECT * FROM memories WHERE content_hash = ?"
            payload = {"sql": sql, "params": [content_hash]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if not result.get("success") or not result.get("result", [{}])[0].get("results"):
                return None

            row = result["result"][0]["results"][0]

            # Load content from R2 if needed
            content = row["content"]
            if row.get("r2_key") and content.startswith("[R2 Content:"):
                content = await self._load_r2_content(row["r2_key"])

            # Load tags
            tags = await self._load_memory_tags(row["id"])

            # Construct Memory object
            memory = Memory(
                content=content,
                content_hash=content_hash,
                tags=tags,
                memory_type=row.get("memory_type"),
                metadata=json.loads(row["metadata_json"]) if row.get("metadata_json") else {},
                created_at=row.get("created_at"),
                created_at_iso=row.get("created_at_iso"),
                updated_at=row.get("updated_at"),
                updated_at_iso=row.get("updated_at_iso")
            )

            return memory

        except Exception as e:
            logger.error(f"Failed to get memory by hash {content_hash}: {e}")
            return None

    async def _delete_vectorize_vector(self, vector_id: str) -> None:
        """Delete vector from Vectorize."""
        # Correct endpoint uses underscores, not hyphens
        payload = {"ids": [vector_id]}

        response = await self._retry_request("POST", f"{self.vectorize_url}/delete_by_ids", json=payload)
        result = response.json()

        if not result.get("success"):
            logger.warning(f"Failed to delete vector from Vectorize: {result}")

    async def delete_vectors_by_ids(self, vector_ids: List[str]) -> Dict[str, Any]:
        """Delete multiple vectors from Vectorize by their IDs."""
        payload = {"ids": vector_ids}
        response = await self._retry_request(
            "POST",
            f"{self.vectorize_url}/delete_by_ids",
            json=payload
        )
        return response.json()

    async def _delete_r2_content(self, r2_key: str) -> None:
        """Delete content from R2."""
        try:
            response = await self._retry_request("DELETE", f"{self.r2_url}/{r2_key}")
            if response.status_code not in [200, 204, 404]:  # 404 is fine if already deleted
                logger.warning(f"Failed to delete R2 content: {response.status_code}")
        except Exception as e:
            logger.warning(f"Failed to delete R2 content {r2_key}: {e}")
    
    async def delete_by_tag(self, tag: str) -> Tuple[int, str]:
        """Delete memories by tag."""
        try:
            # Find memories with the tag
            memories = await self.search_by_tag([tag])
            
            deleted_count = 0
            for memory in memories:
                success, _ = await self.delete(memory.content_hash)
                if success:
                    deleted_count += 1
            
            logger.info(f"Deleted {deleted_count} memories with tag: {tag}")
            return deleted_count, f"Deleted {deleted_count} memories"
            
        except Exception as e:
            logger.error(f"Failed to delete by tag {tag}: {e}")
            return 0, f"Deletion failed: {str(e)}"
    
    async def cleanup_duplicates(self) -> Tuple[int, str]:
        """Remove duplicate memories based on content hash."""
        try:
            # Find duplicates in D1
            sql = """
            SELECT content_hash, COUNT(*) as count, MIN(id) as keep_id
            FROM memories
            GROUP BY content_hash
            HAVING COUNT(*) > 1
            """
            
            payload = {"sql": sql}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()
            
            if not result.get("success"):
                raise ValueError(f"Failed to find duplicates: {result}")
            
            duplicate_groups = result.get("result", [{}])[0].get("results", [])
            
            total_deleted = 0
            for group in duplicate_groups:
                content_hash = group["content_hash"]
                keep_id = group["keep_id"]
                
                # Delete all except the first one
                delete_sql = "DELETE FROM memories WHERE content_hash = ? AND id != ?"
                payload = {"sql": delete_sql, "params": [content_hash, keep_id]}
                response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
                result = response.json()
                
                if result.get("success") and result.get("result", [{}])[0].get("meta"):
                    deleted = result["result"][0]["meta"].get("changes", 0)
                    total_deleted += deleted
            
            logger.info(f"Cleaned up {total_deleted} duplicate memories")
            return total_deleted, f"Removed {total_deleted} duplicates"
            
        except Exception as e:
            logger.error(f"Failed to cleanup duplicates: {e}")
            return 0, f"Cleanup failed: {str(e)}"
    
    async def update_memory_metadata(self, content_hash: str, updates: Dict[str, Any], preserve_timestamps: bool = True) -> Tuple[bool, str]:
        """Update memory metadata without recreating the entry."""
        try:
            # Build update SQL
            update_fields = []
            params = []
            
            if "metadata" in updates:
                update_fields.append("metadata_json = ?")
                params.append(json.dumps(updates["metadata"]))
            
            if "memory_type" in updates:
                update_fields.append("memory_type = ?")
                params.append(updates["memory_type"])
            
            if "tags" in updates:
                # Handle tags separately - they require relational updates
                pass
            
            # Always update updated_at timestamp
            if not preserve_timestamps or "updated_at" not in updates:
                update_fields.append("updated_at = ?")
                update_fields.append("updated_at_iso = ?")
                now = time.time()
                now_iso = datetime.now().isoformat()
                params.extend([now, now_iso])
            
            if not update_fields:
                return True, "No updates needed"
            
            # Update memory record
            sql = f"UPDATE memories SET {', '.join(update_fields)} WHERE content_hash = ?"
            params.append(content_hash)
            
            payload = {"sql": sql, "params": params}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()
            
            if not result.get("success"):
                raise ValueError(f"Failed to update memory: {result}")
            
            # Handle tag updates if provided
            if "tags" in updates:
                await self._update_memory_tags(content_hash, updates["tags"])
            
            logger.info(f"Successfully updated memory metadata: {content_hash}")
            return True, "Memory metadata updated successfully"
            
        except Exception as e:
            logger.error(f"Failed to update memory metadata {content_hash}: {e}")
            return False, f"Update failed: {str(e)}"
    
    async def _update_memory_tags(self, content_hash: str, new_tags: List[str]) -> None:
        """Update tags for a memory."""
        # Get memory ID
        sql = "SELECT id FROM memories WHERE content_hash = ?"
        payload = {"sql": sql, "params": [content_hash]}
        response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
        result = response.json()
        
        if not result.get("success") or not result.get("result", [{}])[0].get("results"):
            raise ValueError(f"Memory not found: {content_hash}")
        
        memory_id = result["result"][0]["results"][0]["id"]
        
        # Delete existing tag relationships
        delete_sql = "DELETE FROM memory_tags WHERE memory_id = ?"
        payload = {"sql": delete_sql, "params": [memory_id]}
        await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
        
        # Add new tags
        if new_tags:
            await self._store_d1_tags(memory_id, new_tags)
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        try:
            # Calculate timestamp for memories from last 7 days
            week_ago = time.time() - (7 * 24 * 60 * 60)

            # Get memory count and size from D1
            sql = f"""
            SELECT
                COUNT(*) as total_memories,
                SUM(content_size) as total_content_size,
                COUNT(DISTINCT vector_id) as total_vectors,
                COUNT(r2_key) as r2_stored_count,
                (SELECT COUNT(*) FROM tags) as unique_tags,
                (SELECT COUNT(*) FROM memories WHERE created_at >= {week_ago}) as memories_this_week
            FROM memories
            """

            payload = {"sql": sql}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if result.get("success") and result.get("result", [{}])[0].get("results"):
                stats = result["result"][0]["results"][0]

                return {
                    "total_memories": stats.get("total_memories", 0),
                    "unique_tags": stats.get("unique_tags", 0),
                    "memories_this_week": stats.get("memories_this_week", 0),
                    "total_content_size_bytes": stats.get("total_content_size", 0),
                    "total_vectors": stats.get("total_vectors", 0),
                    "r2_stored_count": stats.get("r2_stored_count", 0),
                    "storage_backend": "cloudflare",
                    "vectorize_index": self.vectorize_index,
                    "d1_database": self.d1_database_id,
                    "r2_bucket": self.r2_bucket,
                    "status": "operational"
                }

            return {
                "total_memories": 0,
                "unique_tags": 0,
                "memories_this_week": 0,
                "storage_backend": "cloudflare",
                "status": "operational"
            }

        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {
                "total_memories": 0,
                "unique_tags": 0,
                "memories_this_week": 0,
                "storage_backend": "cloudflare",
                "status": "error",
                "error": str(e)
            }
    
    async def get_all_tags(self) -> List[str]:
        """Get all unique tags in the storage."""
        try:
            sql = "SELECT name FROM tags ORDER BY name"
            payload = {"sql": sql}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()
            
            if result.get("success") and result.get("result", [{}])[0].get("results"):
                return [row["name"] for row in result["result"][0]["results"]]
            
            return []
            
        except Exception as e:
            logger.error(f"Failed to get all tags: {e}")
            return []
    
    async def get_recent_memories(self, n: int = 10) -> List[Memory]:
        """Get n most recent memories."""
        try:
            sql = "SELECT * FROM memories ORDER BY created_at DESC LIMIT ?"
            payload = {"sql": sql, "params": [n]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            memories = []
            if result.get("success") and result.get("result", [{}])[0].get("results"):
                for row in result["result"][0]["results"]:
                    memory = await self._load_memory_from_row(row)
                    if memory:
                        memories.append(memory)

            logger.info(f"Retrieved {len(memories)} recent memories")
            return memories

        except Exception as e:
            logger.error(f"Failed to get recent memories: {e}")
            return []

    async def get_largest_memories(self, n: int = 10) -> List[Memory]:
        """Get n largest memories by content length."""
        try:
            sql = "SELECT * FROM memories ORDER BY LENGTH(content) DESC LIMIT ?"
            payload = {"sql": sql, "params": [n]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            memories = []
            if result.get("success") and result.get("result", [{}])[0].get("results"):
                for row in result["result"][0]["results"]:
                    memory = await self._load_memory_from_row(row)
                    if memory:
                        memories.append(memory)

            logger.info(f"Retrieved {len(memories)} largest memories")
            return memories

        except Exception as e:
            logger.error(f"Failed to get largest memories: {e}")
            return []

    async def get_memory_timestamps(self, days: Optional[int] = None) -> List[float]:
        """
        Get memory creation timestamps only, without loading full memory objects.

        This is an optimized method for analytics that only needs timestamps,
        avoiding the overhead of loading full memory content and embeddings.

        Args:
            days: Optional filter to only get memories from last N days

        Returns:
            List of Unix timestamps (float) in descending order (newest first)
        """
        try:
            if days is not None:
                cutoff = datetime.now(timezone.utc) - timedelta(days=days)
                cutoff_timestamp = cutoff.timestamp()

                sql = "SELECT created_at FROM memories WHERE created_at >= ? ORDER BY created_at DESC"
                payload = {"sql": sql, "params": [cutoff_timestamp]}
            else:
                sql = "SELECT created_at FROM memories ORDER BY created_at DESC"
                payload = {"sql": sql, "params": []}

            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            timestamps = []
            if result.get("success") and result.get("result", [{}])[0].get("results"):
                for row in result["result"][0]["results"]:
                    if row.get("created_at") is not None:
                        timestamps.append(float(row["created_at"]))

            logger.info(f"Retrieved {len(timestamps)} memory timestamps")
            return timestamps

        except Exception as e:
            logger.error(f"Failed to get memory timestamps: {e}")
            return []

    def sanitized(self, tags):
        """Sanitize and normalize tags to a JSON string.

        This method provides compatibility with the storage backend interface.
        """
        if tags is None:
            return json.dumps([])
        
        # If we get a string, split it into an array
        if isinstance(tags, str):
            tags = [tag.strip() for tag in tags.split(",") if tag.strip()]
        # If we get an array, use it directly
        elif isinstance(tags, list):
            tags = [str(tag).strip() for tag in tags if str(tag).strip()]
        else:
            return json.dumps([])
                
        # Return JSON string representation of the array
        return json.dumps(tags)
    
    async def recall(self, query: Optional[str] = None, n_results: int = 5, start_timestamp: Optional[float] = None, end_timestamp: Optional[float] = None) -> List[MemoryQueryResult]:
        """
        Retrieve memories with combined time filtering and optional semantic search.

        Args:
            query: Optional semantic search query. If None, only time filtering is applied.
            n_results: Maximum number of results to return.
            start_timestamp: Optional start time for filtering.
            end_timestamp: Optional end time for filtering.

        Returns:
            List of MemoryQueryResult objects.
        """
        try:
            # Build time filtering WHERE clause for D1
            time_conditions = []
            params = []

            if start_timestamp is not None:
                time_conditions.append("created_at >= ?")
                params.append(float(start_timestamp))

            if end_timestamp is not None:
                time_conditions.append("created_at <= ?")
                params.append(float(end_timestamp))

            time_where = " AND ".join(time_conditions) if time_conditions else ""

            logger.info(f"Recall - Time filtering conditions: {time_where}, params: {params}")

            # Determine search strategy
            if query and query.strip():
                # Combined semantic search with time filtering
                logger.info(f"Recall - Using semantic search with query: '{query}'")

                try:
                    # Generate query embedding
                    query_embedding = await self._generate_embedding(query)

                    # Search Vectorize with semantic query
                    search_payload = {
                        "vector": query_embedding,
                        "topK": n_results,
                        "returnMetadata": "all",
                        "returnValues": False
                    }

                    # Add time filtering to vectorize metadata if specified
                    if time_conditions:
                        # Note: Vectorize metadata filtering capabilities may be limited
                        # We'll filter after retrieval for now
                        logger.info("Recall - Time filtering will be applied post-retrieval from Vectorize")

                    response = await self._retry_request("POST", f"{self.vectorize_url}/query", json=search_payload)
                    result = response.json()

                    if not result.get("success"):
                        raise ValueError(f"Vectorize query failed: {result}")

                    matches = result.get("result", {}).get("matches", [])

                    # Convert matches to MemoryQueryResult objects with time filtering
                    results = []
                    for match in matches:
                        memory = await self._load_memory_from_match(match)
                        if memory:
                            # Apply time filtering if needed
                            if start_timestamp is not None and memory.created_at and memory.created_at < start_timestamp:
                                continue
                            if end_timestamp is not None and memory.created_at and memory.created_at > end_timestamp:
                                continue

                            query_result = MemoryQueryResult(
                                memory=memory,
                                relevance_score=match.get("score", 0.0)
                            )
                            results.append(query_result)

                    logger.info(f"Recall - Retrieved {len(results)} memories with semantic search and time filtering")
                    return results[:n_results]  # Ensure we don't exceed n_results

                except Exception as e:
                    logger.error(f"Recall - Semantic search failed, falling back to time-based search: {e}")
                    # Fall through to time-based search

            # Time-based search only (or fallback)
            logger.info(f"Recall - Using time-based search only")

            # Build D1 query for time-based retrieval
            if time_where:
                sql = f"SELECT * FROM memories WHERE {time_where} ORDER BY created_at DESC LIMIT ?"
                params.append(n_results)
            else:
                # No time filters, get most recent
                sql = "SELECT * FROM memories ORDER BY created_at DESC LIMIT ?"
                params = [n_results]

            payload = {"sql": sql, "params": params}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if not result.get("success"):
                raise ValueError(f"D1 query failed: {result}")

            # Convert D1 results to MemoryQueryResult objects
            results = []
            if result.get("result", [{}])[0].get("results"):
                for row in result["result"][0]["results"]:
                    memory = await self._load_memory_from_row(row)
                    if memory:
                        # For time-based search without semantic query, use timestamp as relevance
                        relevance_score = memory.created_at or 0.0
                        query_result = MemoryQueryResult(
                            memory=memory,
                            relevance_score=relevance_score
                        )
                        results.append(query_result)

            logger.info(f"Recall - Retrieved {len(results)} memories with time-based search")
            return results

        except Exception as e:
            logger.error(f"Recall failed: {e}")
            return []

    async def get_all_memories(self, limit: int = None, offset: int = 0, memory_type: Optional[str] = None, tags: Optional[List[str]] = None) -> List[Memory]:
        """
        Get all memories in storage ordered by creation time (newest first).

        Args:
            limit: Maximum number of memories to return (None for all)
            offset: Number of memories to skip (for pagination)
            memory_type: Optional filter by memory type
            tags: Optional filter by tags (matches ANY of the provided tags)

        Returns:
            List of Memory objects ordered by created_at DESC, optionally filtered by type and tags
        """
        try:
            # Build SQL query with optional memory_type and tags filters
            sql = "SELECT * FROM memories"
            params = []
            where_conditions = []

            # Add memory_type filter if specified
            if memory_type is not None:
                where_conditions.append("memory_type = ?")
                params.append(memory_type)

            # Add tags filter if specified (using LIKE for tag matching)
            if tags and len(tags) > 0:
                tag_conditions = " OR ".join(["tags LIKE ?" for _ in tags])
                where_conditions.append(f"({tag_conditions})")
                params.extend([f"%{tag}%" for tag in tags])

            # Apply WHERE clause if we have any conditions
            if where_conditions:
                sql += " WHERE " + " AND ".join(where_conditions)

            sql += " ORDER BY created_at DESC"

            if limit is not None:
                sql += " LIMIT ?"
                params.append(limit)

            if offset > 0:
                sql += " OFFSET ?"
                params.append(offset)

            payload = {"sql": sql, "params": params}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if not result.get("success"):
                raise ValueError(f"D1 query failed: {result}")

            memories = []
            if result.get("result", [{}])[0].get("results"):
                for row in result["result"][0]["results"]:
                    memory = await self._load_memory_from_row(row)
                    if memory:
                        memories.append(memory)

            logger.debug(f"Retrieved {len(memories)} memories from D1")
            return memories

        except Exception as e:
            logger.error(f"Error getting all memories: {str(e)}")
            return []

    def _row_to_memory(self, row: Dict[str, Any]) -> Memory:
        """Convert D1 row to Memory object without loading tags (for bulk operations)."""
        # Load content from R2 if needed
        content = row["content"]
        if row.get("r2_key") and content.startswith("[R2 Content:"):
            # For bulk operations, we don't load R2 content to avoid additional requests
            # Just keep the placeholder
            pass

        return Memory(
            content=content,
            content_hash=row["content_hash"],
            tags=[],  # Skip tag loading for bulk operations
            memory_type=row.get("memory_type"),
            metadata=json.loads(row["metadata_json"]) if row.get("metadata_json") else {},
            created_at=row.get("created_at"),
            created_at_iso=row.get("created_at_iso"),
            updated_at=row.get("updated_at"),
            updated_at_iso=row.get("updated_at_iso")
        )

    async def get_all_memories_bulk(
        self,
        include_tags: bool = False
    ) -> List[Memory]:
        """
        Efficiently load all memories from D1.

        If include_tags=False, skips N+1 tag queries for better performance.
        Useful for maintenance scripts that don't need tag information.

        Args:
            include_tags: Whether to load tags for each memory (slower but complete)

        Returns:
            List of Memory objects ordered by created_at DESC
        """
        query = "SELECT * FROM memories ORDER BY created_at DESC"
        payload = {"sql": query}
        response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
        result = response.json()

        if not result.get("success"):
            raise ValueError(f"D1 query failed: {result}")

        memories = []
        if result.get("result", [{}])[0].get("results"):
            for row in result["result"][0]["results"]:
                if include_tags:
                    memory = await self._load_memory_from_row(row)
                else:
                    memory = self._row_to_memory(row)
                if memory:
                    memories.append(memory)

        logger.debug(f"Bulk loaded {len(memories)} memories from D1")
        return memories

    async def get_all_memories_cursor(self, limit: int = None, cursor: float = None, memory_type: Optional[str] = None, tags: Optional[List[str]] = None) -> List[Memory]:
        """
        Get all memories using cursor-based pagination to avoid D1 OFFSET limitations.

        This method uses timestamp-based cursors instead of OFFSET, which is more efficient
        and avoids Cloudflare D1's OFFSET limitations that cause 400 Bad Request errors.

        Args:
            limit: Maximum number of memories to return (None for all)
            cursor: Timestamp cursor for pagination (created_at value from last result)
            memory_type: Optional filter by memory type
            tags: Optional filter by tags (matches ANY of the provided tags)

        Returns:
            List of Memory objects ordered by created_at DESC, starting after cursor
        """
        try:
            # Build SQL query with cursor-based pagination
            sql = "SELECT * FROM memories"
            params = []
            where_conditions = []

            # Add cursor condition (timestamp-based pagination)
            if cursor is not None:
                where_conditions.append("created_at < ?")
                params.append(cursor)

            # Add memory_type filter if specified
            if memory_type is not None:
                where_conditions.append("memory_type = ?")
                params.append(memory_type)

            # Add tags filter if specified (using LIKE for tag matching)
            if tags and len(tags) > 0:
                tag_conditions = " OR ".join(["tags LIKE ?" for _ in tags])
                where_conditions.append(f"({tag_conditions})")
                params.extend([f"%{tag}%" for tag in tags])

            # Apply WHERE clause if we have any conditions
            if where_conditions:
                sql += " WHERE " + " AND ".join(where_conditions)

            sql += " ORDER BY created_at DESC"

            if limit is not None:
                sql += " LIMIT ?"
                params.append(limit)

            payload = {"sql": sql, "params": params}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if not result.get("success"):
                raise ValueError(f"D1 query failed: {result}")

            memories = []
            if result.get("result", [{}])[0].get("results"):
                for row in result["result"][0]["results"]:
                    memory = await self._load_memory_from_row(row)
                    if memory:
                        memories.append(memory)

            logger.debug(f"Retrieved {len(memories)} memories from D1 with cursor-based pagination")
            return memories

        except Exception as e:
            logger.error(f"Error getting memories with cursor: {str(e)}")
            return []

    async def count_all_memories(self, memory_type: Optional[str] = None, tags: Optional[List[str]] = None) -> int:
        """
        Get total count of memories in storage.

        Args:
            memory_type: Optional filter by memory type
            tags: Optional filter by tags (memories matching ANY of the tags)

        Returns:
            Total number of memories, optionally filtered by type and/or tags
        """
        try:
            # Build query with filters
            conditions = []
            params = []

            if memory_type is not None:
                conditions.append('memory_type = ?')
                params.append(memory_type)

            if tags:
                # Filter by tags - match ANY tag (OR logic)
                tag_conditions = ' OR '.join(['tags LIKE ?' for _ in tags])
                conditions.append(f'({tag_conditions})')
                # Add each tag with wildcards for LIKE matching
                for tag in tags:
                    params.append(f'%{tag}%')

            # Build final query
            if conditions:
                sql = 'SELECT COUNT(*) as count FROM memories WHERE ' + ' AND '.join(conditions)
            else:
                sql = 'SELECT COUNT(*) as count FROM memories'

            payload = {"sql": sql, "params": params}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if not result.get("success"):
                raise ValueError(f"D1 query failed: {result}")

            if result.get("result", [{}])[0].get("results"):
                count = result["result"][0]["results"][0].get("count", 0)
                return int(count)

            return 0

        except Exception as e:
            logger.error(f"Error counting memories: {str(e)}")
            return 0

    # =========================================================================
    # GRAPH RELATIONS (v9.0) - Memory edges for relationships between memories
    # =========================================================================

    async def _ensure_graph_schema(self) -> None:
        """Ensure graph-related tables exist (migration for existing DBs)."""
        migration_sql = """
        -- Add decay fields to memories if not exist (safe ALTER - SQLite ignores if exists)
        ALTER TABLE memories ADD COLUMN strength REAL DEFAULT 1.0;
        ALTER TABLE memories ADD COLUMN access_count INTEGER DEFAULT 0;
        ALTER TABLE memories ADD COLUMN last_accessed TEXT;
        """
        # SQLite doesn't have IF NOT EXISTS for ALTER, so we try and ignore errors
        try:
            payload = {"sql": migration_sql}
            await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
        except Exception:
            pass  # Columns may already exist

    async def link_memories(
        self,
        source_hash: str,
        target_hash: str,
        edge_type: str,
        weight: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:
        """
        Create a directed edge between two memories.

        Args:
            source_hash: Content hash of source memory
            target_hash: Content hash of target memory
            edge_type: Type of relationship (depends_on, blocks, related_to, parent_of, references, derived_from)
            weight: Strength of relationship (0.0 to 1.0)
            metadata: Optional JSON metadata for the edge

        Returns:
            Tuple of (success, message or edge_id)
        """
        try:
            import uuid
            edge_id = str(uuid.uuid4())
            metadata_json = json.dumps(metadata) if metadata else None

            sql = """
            INSERT INTO memory_edges (id, source_hash, target_hash, edge_type, weight, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_hash, target_hash, edge_type) DO UPDATE SET
                weight = excluded.weight,
                metadata_json = excluded.metadata_json
            """

            payload = {"sql": sql, "params": [edge_id, source_hash, target_hash, edge_type, weight, metadata_json]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if not result.get("success"):
                return False, f"Failed to create edge: {result}"

            logger.info(f"Created edge: {source_hash[:8]} --{edge_type}--> {target_hash[:8]}")
            return True, edge_id

        except Exception as e:
            logger.error(f"Error creating edge: {str(e)}")
            return False, str(e)

    async def unlink_memories(
        self,
        source_hash: str,
        target_hash: str,
        edge_type: Optional[str] = None
    ) -> Tuple[bool, int]:
        """
        Remove edge(s) between two memories.

        Args:
            source_hash: Content hash of source memory
            target_hash: Content hash of target memory
            edge_type: Specific edge type to remove (None = all edges between these memories)

        Returns:
            Tuple of (success, count of removed edges)
        """
        try:
            if edge_type:
                sql = "DELETE FROM memory_edges WHERE source_hash = ? AND target_hash = ? AND edge_type = ?"
                params = [source_hash, target_hash, edge_type]
            else:
                sql = "DELETE FROM memory_edges WHERE source_hash = ? AND target_hash = ?"
                params = [source_hash, target_hash]

            payload = {"sql": sql, "params": params}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if not result.get("success"):
                return False, 0

            # D1 doesn't return affected rows easily, assume success
            logger.info(f"Removed edges between {source_hash[:8]} and {target_hash[:8]}")
            return True, 1

        except Exception as e:
            logger.error(f"Error removing edge: {str(e)}")
            return False, 0

    async def get_related(
        self,
        content_hash: str,
        edge_type: Optional[str] = None,
        direction: str = "both",
        depth: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Get memories related to the given memory through edges.

        Args:
            content_hash: Content hash of the memory to find relations for
            edge_type: Filter by edge type (None = all types)
            direction: 'outgoing', 'incoming', or 'both'
            depth: How many levels to traverse (default 1, max 5)

        Returns:
            List of related memories with edge information
        """
        try:
            depth = min(depth, 5)  # Cap depth for performance
            results = []
            visited = set([content_hash])

            # Current level hashes to explore
            current_level = [content_hash]

            for level in range(depth):
                next_level = []

                for hash_to_explore in current_level:
                    # Build query based on direction
                    if direction == "outgoing":
                        sql = "SELECT * FROM memory_edges WHERE source_hash = ?"
                    elif direction == "incoming":
                        sql = "SELECT * FROM memory_edges WHERE target_hash = ?"
                    else:  # both
                        sql = "SELECT * FROM memory_edges WHERE source_hash = ? OR target_hash = ?"

                    params = [hash_to_explore]
                    if direction == "both":
                        params.append(hash_to_explore)

                    if edge_type:
                        sql += " AND edge_type = ?"
                        params.append(edge_type)

                    payload = {"sql": sql, "params": params}
                    response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
                    result = response.json()

                    if result.get("success") and result.get("result", [{}])[0].get("results"):
                        for edge in result["result"][0]["results"]:
                            # Determine the related hash (the one that's not hash_to_explore)
                            related_hash = edge["target_hash"] if edge["source_hash"] == hash_to_explore else edge["source_hash"]

                            if related_hash not in visited:
                                visited.add(related_hash)
                                next_level.append(related_hash)

                                # Fetch the actual memory
                                memory = await self.get_by_hash(related_hash)
                                if memory:
                                    results.append({
                                        "memory": memory.to_dict(),
                                        "edge": {
                                            "id": edge["id"],
                                            "type": edge["edge_type"],
                                            "weight": edge["weight"],
                                            "direction": "outgoing" if edge["source_hash"] == content_hash else "incoming",
                                            "depth": level + 1
                                        }
                                    })

                current_level = next_level
                if not current_level:
                    break

            return results

        except Exception as e:
            logger.error(f"Error getting related memories: {str(e)}")
            return []

    async def get_memory_graph(
        self,
        project: Optional[str] = None,
        include_completed: bool = True,
        max_nodes: int = 100
    ) -> Dict[str, Any]:
        """
        Get a full graph view of memories for visualization.

        Args:
            project: Filter by project tag
            include_completed: Include completed tasks
            max_nodes: Maximum number of nodes to return

        Returns:
            Dict with 'nodes' and 'edges' for graph visualization
        """
        try:
            # Get memories (nodes)
            if project:
                memories = await self.search_by_tag([f"project:{project}"])
            else:
                memories = await self.get_recent_memories(max_nodes)

            if not include_completed:
                memories = [m for m in memories if "status:completed" not in (m.metadata.tags or [])]

            memories = memories[:max_nodes]

            # Get content hashes of our nodes
            node_hashes = {m.content_hash for m in memories}

            # Get all edges between these nodes
            placeholders = ",".join(["?" for _ in node_hashes])
            sql = f"""
            SELECT * FROM memory_edges
            WHERE source_hash IN ({placeholders}) AND target_hash IN ({placeholders})
            """
            params = list(node_hashes) + list(node_hashes)

            payload = {"sql": sql, "params": params}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            edges = []
            if result.get("success") and result.get("result", [{}])[0].get("results"):
                edges = result["result"][0]["results"]

            return {
                "nodes": [
                    {
                        "id": m.content_hash,
                        "content": m.content[:100] + "..." if len(m.content) > 100 else m.content,
                        "type": m.metadata.memory_type,
                        "tags": m.metadata.tags
                    }
                    for m in memories
                ],
                "edges": [
                    {
                        "source": e["source_hash"],
                        "target": e["target_hash"],
                        "type": e["edge_type"],
                        "weight": e["weight"]
                    }
                    for e in edges
                ],
                "stats": {
                    "node_count": len(memories),
                    "edge_count": len(edges)
                }
            }

        except Exception as e:
            logger.error(f"Error getting memory graph: {str(e)}")
            return {"nodes": [], "edges": [], "stats": {"node_count": 0, "edge_count": 0}}

    async def find_path(
        self,
        from_hash: str,
        to_hash: str,
        max_depth: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Find the shortest path between two memories through edges.

        Args:
            from_hash: Starting memory content hash
            to_hash: Target memory content hash
            max_depth: Maximum path length to search

        Returns:
            List of memories forming the path (empty if no path found)
        """
        try:
            from collections import deque

            # BFS to find shortest path
            queue = deque([(from_hash, [from_hash])])
            visited = {from_hash}

            while queue:
                current_hash, path = queue.popleft()

                if len(path) > max_depth:
                    continue

                # Get all connected memories
                sql = """
                SELECT target_hash as related FROM memory_edges WHERE source_hash = ?
                UNION
                SELECT source_hash as related FROM memory_edges WHERE target_hash = ?
                """
                params = [current_hash, current_hash]

                payload = {"sql": sql, "params": params}
                response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
                result = response.json()

                if result.get("success") and result.get("result", [{}])[0].get("results"):
                    for row in result["result"][0]["results"]:
                        related = row["related"]

                        if related == to_hash:
                            # Found the target!
                            full_path = path + [related]
                            # Fetch all memories in path
                            path_memories = []
                            for hash_val in full_path:
                                memory = await self.get_by_hash(hash_val)
                                if memory:
                                    path_memories.append(memory.to_dict())
                            return path_memories

                        if related not in visited:
                            visited.add(related)
                            queue.append((related, path + [related]))

            return []  # No path found

        except Exception as e:
            logger.error(f"Error finding path: {str(e)}")
            return []

    # =========================================================================
    # DECAY/REINFORCEMENT (v9.0) - Track memory importance over time
    # =========================================================================

    async def reinforce_memory(self, content_hash: str) -> bool:
        """
        Reinforce a memory by incrementing access count and updating last_accessed.
        Called automatically when a memory is retrieved.

        Args:
            content_hash: Content hash of the memory to reinforce

        Returns:
            True if reinforcement was successful
        """
        try:
            from datetime import datetime
            now = datetime.utcnow().isoformat()

            sql = """
            UPDATE memories
            SET access_count = COALESCE(access_count, 0) + 1,
                last_accessed = ?,
                strength = CASE
                    WHEN strength IS NULL THEN 1.0
                    WHEN strength < 2.0 THEN strength + 0.1
                    ELSE strength
                END
            WHERE content_hash = ?
            """

            payload = {"sql": sql, "params": [now, content_hash]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            return result.get("success", False)

        except Exception as e:
            logger.error(f"Error reinforcing memory: {str(e)}")
            return False

    async def get_weak_memories(
        self,
        threshold: float = 0.5,
        min_age_days: int = 30,
        limit: int = 50
    ) -> List[Memory]:
        """
        Get memories with low strength score for review (NOT auto-deletion).
        This is INFORMATIONAL only - user decides what to do with results.

        Args:
            threshold: Maximum strength to include (default 0.5)
            min_age_days: Only include memories older than this (default 30)
            limit: Maximum results to return

        Returns:
            List of weak memories for user review
        """
        try:
            from datetime import datetime, timedelta
            cutoff = (datetime.utcnow() - timedelta(days=min_age_days)).timestamp()

            sql = """
            SELECT * FROM memories
            WHERE (strength IS NULL OR strength < ?)
            AND created_at < ?
            ORDER BY strength ASC, access_count ASC
            LIMIT ?
            """

            payload = {"sql": sql, "params": [threshold, cutoff, limit]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            memories = []
            if result.get("success") and result.get("result", [{}])[0].get("results"):
                for row in result["result"][0]["results"]:
                    memory = await self._load_memory_from_row(row)
                    if memory:
                        memories.append(memory)

            return memories

        except Exception as e:
            logger.error(f"Error getting weak memories: {str(e)}")
            return []

    # =========================================================================
    # PROACTIVE RETRIEVAL (v9.0) - Auto-load relevant context
    # =========================================================================

    async def get_session_context(
        self,
        project: Optional[str] = None,
        recent_count: int = 10,
        important_tags: List[str] = None
    ) -> Dict[str, Any]:
        """
        Get comprehensive session context for project start.
        Combines multiple queries into one efficient call.

        Args:
            project: Project name to filter by
            recent_count: Number of recent items to include
            important_tags: Tags to include as important

        Returns:
            Dict with categorized memories for session context
        """
        try:
            if important_tags is None:
                important_tags = ["important", "pinned"]

            context = {
                "in_progress_tasks": [],
                "recent_memories": [],
                "important": [],
                "blockers": [],
                "recent_errors": [],
                "recent_decisions": []
            }

            # In-progress tasks
            in_progress = await self.search_by_tag(["status:in-progress"])
            if project:
                in_progress = [m for m in in_progress if f"project:{project}" in (m.metadata.tags or [])]
            context["in_progress_tasks"] = [m.to_dict() for m in in_progress[:10]]

            # Important memories
            important = await self.search_by_tag(important_tags)
            if project:
                important = [m for m in important if f"project:{project}" in (m.metadata.tags or [])]
            context["important"] = [m.to_dict() for m in important[:10]]

            # Recent memories for project
            if project:
                recent = await self.search_by_tag([f"project:{project}"])
                context["recent_memories"] = [m.to_dict() for m in recent[:recent_count]]
            else:
                recent = await self.get_recent_memories(recent_count)
                context["recent_memories"] = [m.to_dict() for m in recent]

            # Blockers
            blockers = await self.search_by_tag(["status:blocked"])
            if project:
                blockers = [m for m in blockers if f"project:{project}" in (m.metadata.tags or [])]
            context["blockers"] = [m.to_dict() for m in blockers[:5]]

            # Recent errors
            errors = await self.search_by_tag(["type:error"])
            if project:
                errors = [m for m in errors if f"project:{project}" in (m.metadata.tags or [])]
            context["recent_errors"] = [m.to_dict() for m in errors[:5]]

            # Recent decisions
            decisions = await self.search_by_tag(["type:decision"])
            if project:
                decisions = [m for m in decisions if f"project:{project}" in (m.metadata.tags or [])]
            context["recent_decisions"] = [m.to_dict() for m in decisions[:5]]

            return context

        except Exception as e:
            logger.error(f"Error getting session context: {str(e)}")
            return {
                "in_progress_tasks": [],
                "recent_memories": [],
                "important": [],
                "blockers": [],
                "recent_errors": [],
                "recent_decisions": []
            }

    async def get_project_summary(self, project: str) -> Dict[str, Any]:
        """
        Get summary statistics for a project.

        Args:
            project: Project name

        Returns:
            Dict with project summary statistics
        """
        try:
            all_memories = await self.search_by_tag([f"project:{project}"])

            # Count by status
            status_counts = {}
            type_counts = {}

            for m in all_memories:
                tags = m.metadata.tags or []

                # Count statuses
                for tag in tags:
                    if tag.startswith("status:"):
                        status = tag.split(":")[1]
                        status_counts[status] = status_counts.get(status, 0) + 1

                # Count types
                mem_type = m.metadata.memory_type or "unknown"
                type_counts[mem_type] = type_counts.get(mem_type, 0) + 1

            return {
                "project": project,
                "total_memories": len(all_memories),
                "by_status": status_counts,
                "by_type": type_counts,
                "last_activity": all_memories[0].metadata.created_at_iso if all_memories else None
            }

        except Exception as e:
            logger.error(f"Error getting project summary: {str(e)}")
            return {"project": project, "error": str(e)}

    # =========================================================================
    # SAFE CONSOLIDATION (v9.0) - Merge duplicates with archive safety net
    # =========================================================================

    async def find_duplicate_memories(
        self,
        similarity_threshold: float = 0.95,
        max_results: int = 50,
        batch_size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Find semantically similar memories that might be duplicates.
        READ-ONLY: Does not modify or delete anything.

        Args:
            similarity_threshold: Minimum similarity to consider duplicate (default 0.95)
            max_results: Maximum duplicate pairs to return
            batch_size: Batch size for processing

        Returns:
            List of duplicate pairs with similarity scores
        """
        try:
            duplicates = []

            # Get recent memories to check
            memories = await self.get_recent_memories(batch_size * 2)

            for i, mem1 in enumerate(memories):
                if len(duplicates) >= max_results:
                    break

                for mem2 in memories[i+1:]:
                    if len(duplicates) >= max_results:
                        break

                    # Simple content-based similarity check first
                    if abs(len(mem1.content) - len(mem2.content)) > len(mem1.content) * 0.2:
                        continue  # Content length too different

                    # Check semantic similarity
                    try:
                        emb1 = await self._generate_embedding(mem1.content)
                        emb2 = await self._generate_embedding(mem2.content)

                        # Cosine similarity
                        dot = sum(a * b for a, b in zip(emb1, emb2))
                        norm1 = sum(a * a for a in emb1) ** 0.5
                        norm2 = sum(b * b for b in emb2) ** 0.5
                        similarity = dot / (norm1 * norm2) if norm1 * norm2 > 0 else 0

                        if similarity >= similarity_threshold:
                            duplicates.append({
                                "memory1": {
                                    "hash": mem1.content_hash,
                                    "content_preview": mem1.content[:100],
                                    "created": mem1.metadata.created_at_iso,
                                    "tags": mem1.metadata.tags
                                },
                                "memory2": {
                                    "hash": mem2.content_hash,
                                    "content_preview": mem2.content[:100],
                                    "created": mem2.metadata.created_at_iso,
                                    "tags": mem2.metadata.tags
                                },
                                "similarity": round(similarity, 4)
                            })
                    except Exception:
                        continue

            return duplicates

        except Exception as e:
            logger.error(f"Error finding duplicates: {str(e)}")
            return []

    async def preview_consolidation(
        self,
        memory_hashes: List[str],
        merge_strategy: str = "newest"
    ) -> Dict[str, Any]:
        """
        Preview what consolidation would do WITHOUT executing it.
        DRY-RUN only - no data is modified.

        Args:
            memory_hashes: List of content hashes to consider for merging
            merge_strategy: 'newest', 'oldest', 'combine_tags'

        Returns:
            Preview of what would happen
        """
        try:
            memories = []
            for hash_val in memory_hashes:
                mem = await self.get_by_hash(hash_val)
                if mem:
                    memories.append(mem)

            if len(memories) < 2:
                return {"error": "Need at least 2 memories to consolidate"}

            # Sort by creation date
            memories.sort(key=lambda m: m.metadata.created_at or 0)

            if merge_strategy == "newest":
                keep = memories[-1]
                archive = memories[:-1]
            elif merge_strategy == "oldest":
                keep = memories[0]
                archive = memories[1:]
            else:  # combine_tags
                keep = memories[-1]
                archive = memories[:-1]

            # Combine tags
            all_tags = set()
            for m in memories:
                all_tags.update(m.metadata.tags or [])

            return {
                "strategy": merge_strategy,
                "keep": {
                    "hash": keep.content_hash,
                    "content_preview": keep.content[:200],
                    "original_tags": keep.metadata.tags,
                    "merged_tags": list(all_tags)
                },
                "archive": [
                    {
                        "hash": m.content_hash,
                        "content_preview": m.content[:100],
                        "tags": m.metadata.tags
                    }
                    for m in archive
                ],
                "total_memories_affected": len(memories),
                "NOTE": "This is a PREVIEW. No data has been modified. Call consolidate_memories with dry_run=False to execute."
            }

        except Exception as e:
            logger.error(f"Error previewing consolidation: {str(e)}")
            return {"error": str(e)}

    async def consolidate_memories(
        self,
        memory_hashes: List[str],
        keep_hash: str,
        merge_tags: bool = True,
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """
        Consolidate multiple memories into one, archiving the rest.

        SAFETY: dry_run=True by default. Archives (not deletes) memories.

        Args:
            memory_hashes: List of content hashes to consolidate
            keep_hash: Content hash of the memory to keep
            merge_tags: Whether to merge tags from archived memories
            dry_run: If True, only preview without executing

        Returns:
            Result of consolidation (or preview if dry_run)
        """
        try:
            if keep_hash not in memory_hashes:
                return {"error": "keep_hash must be in memory_hashes"}

            if dry_run:
                return await self.preview_consolidation(memory_hashes, "newest")

            # Fetch all memories
            memories = {}
            for hash_val in memory_hashes:
                mem = await self.get_by_hash(hash_val)
                if mem:
                    memories[hash_val] = mem

            if keep_hash not in memories:
                return {"error": f"Memory {keep_hash} not found"}

            keep_memory = memories[keep_hash]
            archive_hashes = [h for h in memory_hashes if h != keep_hash]

            # Collect tags to merge
            merged_tags = set(keep_memory.metadata.tags or [])
            if merge_tags:
                for h in archive_hashes:
                    if h in memories:
                        merged_tags.update(memories[h].metadata.tags or [])

            # Archive memories (soft-delete)
            archived_count = 0
            for h in archive_hashes:
                if h in memories:
                    mem = memories[h]

                    # Insert into archive
                    import uuid
                    archive_id = str(uuid.uuid4())

                    archive_sql = """
                    INSERT INTO memory_archive (id, original_content_hash, content, memory_type,
                        metadata_json, tags_json, archived_reason, consolidated_into)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """

                    archive_params = [
                        archive_id,
                        mem.content_hash,
                        mem.content,
                        mem.metadata.memory_type,
                        json.dumps(mem.metadata.to_dict()) if hasattr(mem.metadata, 'to_dict') else None,
                        json.dumps(list(mem.metadata.tags or [])),
                        "consolidated",
                        keep_hash
                    ]

                    payload = {"sql": archive_sql, "params": archive_params}
                    response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
                    result = response.json()

                    if result.get("success"):
                        # Delete from main table
                        await self.delete(h)

                        # Create "consolidated_from" edge for history
                        await self.link_memories(
                            keep_hash, h, "consolidated_from",
                            metadata={"archived_at": datetime.utcnow().isoformat()}
                        )

                        archived_count += 1

            # Update kept memory with merged tags
            if merge_tags and merged_tags:
                await self.update_memory_metadata(keep_hash, {"tags": list(merged_tags)})

            return {
                "success": True,
                "kept": keep_hash,
                "archived": archived_count,
                "merged_tags": list(merged_tags),
                "message": f"Consolidated {archived_count + 1} memories. Archived memories can be restored."
            }

        except Exception as e:
            logger.error(f"Error consolidating memories: {str(e)}")
            return {"error": str(e)}

    async def restore_archived(self, archive_id: str) -> Tuple[bool, str]:
        """
        Restore an archived memory back to active storage.

        Args:
            archive_id: ID of the archived memory

        Returns:
            Tuple of (success, restored_content_hash or error message)
        """
        try:
            # Get archived memory
            sql = "SELECT * FROM memory_archive WHERE id = ? AND restored = FALSE"
            payload = {"sql": sql, "params": [archive_id]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if not result.get("success") or not result.get("result", [{}])[0].get("results"):
                return False, "Archived memory not found or already restored"

            row = result["result"][0]["results"][0]

            # Recreate memory
            tags = json.loads(row["tags_json"]) if row.get("tags_json") else []
            memory = Memory(
                content=row["content"],
                tags=tags,
                memory_type=row.get("memory_type")
            )

            # Store it
            success, message = await self.store(memory)

            if success:
                # Mark as restored in archive
                update_sql = "UPDATE memory_archive SET restored = TRUE, restored_at = ? WHERE id = ?"
                from datetime import datetime
                payload = {"sql": update_sql, "params": [datetime.utcnow().isoformat(), archive_id]}
                await self._retry_request("POST", f"{self.d1_url}/query", json=payload)

                return True, memory.content_hash

            return False, message

        except Exception as e:
            logger.error(f"Error restoring archived memory: {str(e)}")
            return False, str(e)

    async def list_archived(
        self,
        consolidated_into: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        List archived memories for review.

        Args:
            consolidated_into: Filter by the memory they were consolidated into
            limit: Maximum results

        Returns:
            List of archived memory info
        """
        try:
            if consolidated_into:
                sql = """
                SELECT id, original_content_hash, content, memory_type, archived_at,
                    archived_reason, consolidated_into, restored
                FROM memory_archive
                WHERE consolidated_into = ? AND restored = FALSE
                ORDER BY archived_at DESC
                LIMIT ?
                """
                params = [consolidated_into, limit]
            else:
                sql = """
                SELECT id, original_content_hash, content, memory_type, archived_at,
                    archived_reason, consolidated_into, restored
                FROM memory_archive
                WHERE restored = FALSE
                ORDER BY archived_at DESC
                LIMIT ?
                """
                params = [limit]

            payload = {"sql": sql, "params": params}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if result.get("success") and result.get("result", [{}])[0].get("results"):
                return [
                    {
                        "archive_id": row["id"],
                        "original_hash": row["original_content_hash"],
                        "content_preview": row["content"][:100] if row.get("content") else "",
                        "type": row.get("memory_type"),
                        "archived_at": row.get("archived_at"),
                        "reason": row.get("archived_reason"),
                        "consolidated_into": row.get("consolidated_into")
                    }
                    for row in result["result"][0]["results"]
                ]

            return []

        except Exception as e:
            logger.error(f"Error listing archived memories: {str(e)}")
            return []

    # =========================================================================
    # AUTO-GRAPH BUILDING (v9.0) - Build relationships for existing memories
    # =========================================================================

    async def edge_exists(
        self,
        source_hash: str,
        target_hash: str,
        edge_type: Optional[str] = None
    ) -> bool:
        """
        Check if an edge exists between two memories (in either direction).

        Args:
            source_hash: Content hash of first memory
            target_hash: Content hash of second memory
            edge_type: Specific edge type to check (None = any edge)

        Returns:
            True if edge exists
        """
        try:
            if edge_type:
                sql = """
                SELECT 1 FROM memory_edges
                WHERE ((source_hash = ?1 AND target_hash = ?2)
                    OR (source_hash = ?2 AND target_hash = ?1))
                  AND edge_type = ?3
                LIMIT 1
                """
                params = [source_hash, target_hash, edge_type]
            else:
                sql = """
                SELECT 1 FROM memory_edges
                WHERE (source_hash = ?1 AND target_hash = ?2)
                   OR (source_hash = ?2 AND target_hash = ?1)
                LIMIT 1
                """
                params = [source_hash, target_hash]

            payload = {"sql": sql, "params": params}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if result.get("success") and result.get("result", [{}])[0].get("results"):
                return len(result["result"][0]["results"]) > 0
            return False

        except Exception as e:
            logger.error(f"Error checking edge existence: {str(e)}")
            return False

    async def build_auto_graph(
        self,
        threshold: float = 0.75,
        max_memories: int = 1000,
        max_edges_per_memory: int = 5,
        dry_run: bool = True,
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        Build graph relationships for existing memories based on semantic similarity.

        This method analyzes memories and creates 'related_to' edges between
        semantically similar ones. Safe operation - only adds edges, never removes.

        Args:
            threshold: Minimum similarity score (0.0-1.0) to create edge
            max_memories: Maximum memories to process
            max_edges_per_memory: Maximum edges to create per memory
            dry_run: If True, only preview without creating edges
            progress_callback: Optional callback(processed, total, edges_created)

        Returns:
            Dict with stats: processed, edges_created, edges_skipped, errors
        """
        logger.info(f"Building auto-graph: threshold={threshold}, max={max_memories}, dry_run={dry_run}")

        stats = {
            "processed": 0,
            "edges_created": 0,
            "edges_skipped": 0,
            "errors": 0,
            "dry_run": dry_run
        }

        try:
            # Get memories to process
            memories = await self.get_all_memories(limit=max_memories)
            total = len(memories)

            if not memories:
                logger.info("No memories to process for auto-graph")
                return stats

            for memory in memories:
                stats["processed"] += 1

                # Skip short content
                if not memory.content or len(memory.content) < 20:
                    continue

                try:
                    # Find similar memories
                    similar_results = await self.search(
                        query=memory.content,
                        n_results=max_edges_per_memory + 1
                    )

                    edges_for_memory = 0
                    for result in similar_results:
                        # Skip self
                        if result.memory.content_hash == memory.content_hash:
                            continue

                        # Check threshold
                        if result.relevance_score < threshold:
                            continue

                        # Limit edges per memory
                        if edges_for_memory >= max_edges_per_memory:
                            break

                        # Check if edge exists
                        if await self.edge_exists(memory.content_hash, result.memory.content_hash):
                            stats["edges_skipped"] += 1
                            continue

                        # Create edge (or just count for dry run)
                        if not dry_run:
                            success, _ = await self.link_memories(
                                source_hash=memory.content_hash,
                                target_hash=result.memory.content_hash,
                                edge_type="related_to",
                                weight=result.relevance_score,
                                metadata={
                                    "auto_generated": True,
                                    "similarity": result.relevance_score,
                                    "build_version": "v9.0"
                                }
                            )
                            if not success:
                                stats["errors"] += 1
                                continue

                        stats["edges_created"] += 1
                        edges_for_memory += 1

                except Exception as e:
                    logger.warning(f"Error processing memory {memory.content_hash[:8]}: {e}")
                    stats["errors"] += 1

                # Progress callback
                if progress_callback and stats["processed"] % 50 == 0:
                    progress_callback(stats["processed"], total, stats["edges_created"])

            logger.info(f"Auto-graph complete: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Error in build_auto_graph: {str(e)}")
            stats["errors"] += 1
            return stats

    async def close(self) -> None:
        """Close the storage backend and cleanup resources."""
        if self.client:
            await self.client.aclose()
            self.client = None

        # Clear embedding cache
        self._embedding_cache.clear()

        logger.info("Cloudflare storage backend closed")
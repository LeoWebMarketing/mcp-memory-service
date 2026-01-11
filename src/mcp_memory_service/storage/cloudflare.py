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

    # ========== PHASE 1: Vectorize Metadata Helpers ==========

    def _extract_project_from_tags(self, tags_str: str) -> Optional[str]:
        """Extract project name from tags string."""
        if not tags_str:
            return None
        for tag in tags_str.split(","):
            tag = tag.strip()
            if tag.startswith("project:"):
                return tag.split(":", 1)[1]
        return None

    def _extract_status_from_tags(self, tags_str: str) -> Optional[str]:
        """Extract status from tags string."""
        if not tags_str:
            return None
        for tag in tags_str.split(","):
            tag = tag.strip()
            if tag.startswith("status:"):
                return tag.split(":", 1)[1]
        return None

    def _extract_type_from_tags(self, tags_str: str) -> Optional[str]:
        """Extract type from tags string."""
        if not tags_str:
            return None
        for tag in tags_str.split(","):
            tag = tag.strip()
            if tag.startswith("type:"):
                return tag.split(":", 1)[1]
        return None

    def _build_vectorize_filter(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Build Cloudflare Vectorize filter syntax."""
        vectorize_filter = {}

        for key, value in filters.items():
            if value is None:
                continue
            if isinstance(value, str):
                vectorize_filter[key] = {"$eq": value}
            elif isinstance(value, list):
                vectorize_filter[key] = {"$in": value}

        return vectorize_filter

    # ========== PHASE 2: FTS5 + Hybrid Search ==========

    def _escape_fts5_query(self, query: str) -> str:
        """Escape special FTS5 characters for safe query."""
        # Remove characters that break FTS5 syntax
        special_chars = ['"', "'", "(", ")", "*", ":", "-", "+", "^", "~", "[", "]", "{", "}"]
        for char in special_chars:
            query = query.replace(char, " ")
        return " ".join(query.split())  # Normalize whitespace

    async def _fts5_search(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Full-text search using FTS5 (if available in D1).

        Returns list of matches with content_hash, content, and BM25 rank score.
        Falls back to LIKE search if FTS5 is not available.
        """
        safe_query = self._escape_fts5_query(query)
        if not safe_query.strip():
            return []

        # Build FTS5 query with wildcards: "word1 word2" -> "word1* word2*"
        terms = safe_query.split()
        fts_query = " ".join(f"{term}*" for term in terms if term)

        try:
            # Try FTS5 search first
            sql = """
            SELECT
                m.content_hash,
                m.content,
                m.memory_type,
                m.created_at_iso,
                bm25(memories_fts) as rank
            FROM memories_fts
            JOIN memories m ON memories_fts.rowid = m.rowid
            WHERE memories_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """
            payload = {"sql": sql, "params": [fts_query, limit]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if result.get("success") and result.get("result", [{}])[0].get("results"):
                results = result["result"][0]["results"]

                # Normalize BM25 scores to 0-1 range
                if results:
                    ranks = [r.get("rank", 0) for r in results]
                    min_rank = min(ranks)
                    max_rank = max(ranks)
                    rank_range = max_rank - min_rank if max_rank != min_rank else 1

                    for r in results:
                        # BM25: more negative = better match, so invert
                        r["score"] = 1.0 - ((r.get("rank", 0) - min_rank) / rank_range)

                logger.info(f"FTS5 search found {len(results)} results for query: {query}")
                return results

        except Exception as e:
            logger.warning(f"FTS5 search failed, falling back to LIKE search: {e}")

        # Fallback to LIKE search if FTS5 not available
        return await self._like_search(query, limit)

    async def _like_search(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Fallback keyword search using LIKE when FTS5 is not available."""
        safe_query = self._escape_fts5_query(query)
        if not safe_query.strip():
            return []

        terms = safe_query.split()
        # Build LIKE conditions for each term
        like_conditions = " AND ".join([f"content LIKE ?" for _ in terms])
        params = [f"%{term}%" for term in terms]
        params.append(limit)

        sql = f"""
        SELECT
            content_hash,
            content,
            memory_type,
            created_at_iso,
            LENGTH(content) as rank
        FROM memories
        WHERE {like_conditions}
        ORDER BY created_at DESC
        LIMIT ?
        """

        payload = {"sql": sql, "params": params}
        response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
        result = response.json()

        if result.get("success") and result.get("result", [{}])[0].get("results"):
            results = result["result"][0]["results"]
            # Assign equal scores for LIKE results
            for r in results:
                r["score"] = 0.5
            return results

        return []

    def _reciprocal_rank_fusion(
        self,
        result_lists: List[List[Dict[str, Any]]],
        weights: Optional[List[float]] = None,
        k: int = 60
    ) -> List[Dict[str, Any]]:
        """Merge multiple ranked result lists using Reciprocal Rank Fusion (RRF).

        RRF score = sum(weight / (k + rank)) for each list

        Args:
            result_lists: List of result lists, each sorted by relevance
            weights: Weight for each list (default: equal weights)
            k: Constant to prevent high ranks dominating (default: 60)

        Returns:
            Merged and re-ranked results
        """
        if weights is None:
            weights = [1.0] * len(result_lists)

        # Normalize weights
        total_weight = sum(weights)
        weights = [w / total_weight for w in weights]

        # Calculate RRF scores
        scores: Dict[str, Dict[str, Any]] = {}

        for list_idx, results in enumerate(result_lists):
            weight = weights[list_idx]
            method_name = ["semantic", "keyword"][list_idx] if list_idx < 2 else f"list_{list_idx}"

            for rank, result in enumerate(results, start=1):
                content_hash = result.get("content_hash")
                if not content_hash:
                    continue

                rrf_score = weight / (k + rank)

                if content_hash not in scores:
                    scores[content_hash] = {
                        "score": 0.0,
                        "data": result,
                        "sources": []
                    }

                scores[content_hash]["score"] += rrf_score
                scores[content_hash]["sources"].append({
                    "method": method_name,
                    "rank": rank,
                    "original_score": result.get("score", 0)
                })

        # Sort by RRF score
        sorted_results = sorted(
            scores.values(),
            key=lambda x: x["score"],
            reverse=True
        )

        # Format output
        return [
            {
                **item["data"],
                "rrf_score": item["score"],
                "sources": item["sources"]
            }
            for item in sorted_results
        ]

    async def hybrid_retrieve(
        self,
        query: str,
        n_results: int = 10,
        semantic_weight: float = 0.6,
        keyword_weight: float = 0.4,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Hybrid search combining semantic (Vectorize) and keyword (FTS5/LIKE) search.

        Args:
            query: Search query
            n_results: Number of results to return
            semantic_weight: Weight for semantic search (0-1)
            keyword_weight: Weight for keyword search (0-1)
            filters: Optional Vectorize metadata filters

        Returns:
            Merged results with RRF scores and source information
        """
        # Fetch more candidates for better fusion
        candidate_multiplier = 3

        # Run searches in parallel
        semantic_task = self._semantic_search(
            query,
            limit=n_results * candidate_multiplier,
            filters=filters
        )
        keyword_task = self._fts5_search(
            query,
            limit=n_results * candidate_multiplier
        )

        semantic_results, keyword_results = await asyncio.gather(
            semantic_task,
            keyword_task
        )

        logger.info(f"Hybrid search: {len(semantic_results)} semantic + {len(keyword_results)} keyword results")

        # Merge using RRF
        merged = self._reciprocal_rank_fusion(
            [semantic_results, keyword_results],
            weights=[semantic_weight, keyword_weight]
        )

        return merged[:n_results]

    async def _semantic_search(
        self,
        query: str,
        limit: int,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Semantic search via Vectorize, returning dicts for RRF fusion."""
        try:
            results = await self.retrieve(query, n_results=limit, filters=filters)

            # Convert MemoryQueryResult to dict for RRF
            return [
                {
                    "content_hash": r.memory.content_hash,
                    "content": r.memory.content,
                    "memory_type": r.memory.memory_type,
                    "created_at_iso": r.memory.created_at_iso,
                    "tags": ",".join(r.memory.tags) if r.memory.tags else "",
                    "score": r.relevance_score
                }
                for r in results
            ]
        except Exception as e:
            logger.error(f"Semantic search failed: {e}")
            return []

    # ========== PHASE 3: Smart Retrieve with Query Understanding ==========

    async def smart_retrieve(
        self,
        query: str,
        n_results: int = 10,
        use_hybrid: bool = True
    ) -> Dict[str, Any]:
        """Smart retrieval with automatic query understanding.

        Automatically detects:
        - Project names (freel, mind-space, etc.)
        - Memory types (task, error, decision)
        - Time expressions (last week, yesterday)
        - Status (completed, pending)
        - Abbreviation expansions

        Args:
            query: Natural language search query
            n_results: Number of results to return
            use_hybrid: Whether to use hybrid search (default True)

        Returns:
            Dict with 'results' and 'query_understanding' metadata
        """
        from ..utils.query_parser import query_parser

        # Parse the query
        parsed = query_parser.parse(query)
        logger.info(f"Query parsed: filters={parsed.filters}, time={parsed.time_filter}, intent={parsed.intent}")

        # Build search query with expansions
        search_query = parsed.cleaned
        if parsed.expanded_terms:
            search_query += " " + " ".join(parsed.expanded_terms)

        # Build filters from parsed data
        filters = parsed.filters if parsed.filters else None

        # Choose search method
        if use_hybrid:
            results = await self.hybrid_retrieve(
                query=search_query,
                n_results=n_results,
                filters=filters
            )
        else:
            # Use semantic-only search
            raw_results = await self.retrieve(
                query=search_query,
                n_results=n_results * 2,  # Get more, filter later
                filters=filters
            )
            # Convert to dicts
            results = [
                {
                    "content_hash": r.memory.content_hash,
                    "content": r.memory.content,
                    "memory_type": r.memory.memory_type,
                    "created_at_iso": r.memory.created_at_iso,
                    "tags": ",".join(r.memory.tags) if r.memory.tags else "",
                    "score": r.relevance_score
                }
                for r in raw_results
            ]

        # Apply time filter if present
        if parsed.time_filter and "start_date" in parsed.time_filter:
            start_date = parsed.time_filter["start_date"]
            results = [
                r for r in results
                if r.get("created_at_iso", "")[:10] >= start_date
            ]

        # Apply post-filtering for project/type tags (fallback when Vectorize metadata not available)
        if parsed.filters:
            project_filter = parsed.filters.get("project")
            type_filter = parsed.filters.get("type")

            if project_filter or type_filter:
                filtered_results = []
                for r in results:
                    tags = r.get("tags", "")
                    if isinstance(tags, list):
                        tags = ",".join(tags)
                    tags_lower = tags.lower()

                    # Check project filter
                    if project_filter:
                        if f"project:{project_filter}" not in tags_lower and project_filter not in tags_lower:
                            continue

                    # Check type filter
                    if type_filter:
                        if f"type:{type_filter}" not in tags_lower and type_filter not in tags_lower:
                            continue

                    filtered_results.append(r)

                # If we filtered too much, keep some unfiltered results
                if len(filtered_results) < n_results // 2:
                    logger.warning(f"Post-filter reduced results from {len(results)} to {len(filtered_results)}, keeping some unfiltered")
                    # Add unfiltered to reach minimum
                    for r in results:
                        if r not in filtered_results:
                            filtered_results.append(r)
                        if len(filtered_results) >= n_results:
                            break

                results = filtered_results

        return {
            "results": results[:n_results],
            "query_understanding": {
                "original": parsed.original,
                "cleaned": parsed.cleaned,
                "filters_applied": parsed.filters,
                "time_filter": parsed.time_filter,
                "expansions": parsed.expanded_terms,
                "intent": parsed.intent
            }
        }

    async def refresh_query_parser_projects(self) -> int:
        """Load project names from Memory MCP tags into QueryParser.

        Returns:
            Number of projects added
        """
        from ..utils.query_parser import query_parser

        try:
            # Get all unique project tags from D1
            sql = """
            SELECT DISTINCT tag
            FROM tags
            WHERE tag LIKE 'project:%'
            """
            payload = {"sql": sql, "params": []}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if result.get("success") and result.get("result", [{}])[0].get("results"):
                results = result["result"][0]["results"]
                tags = [r.get("tag", "") for r in results if r.get("tag")]
                added = query_parser.load_projects_from_tags(tags)
                logger.info(f"Loaded {added} projects from Memory tags into QueryParser")
                return added
            return 0
        except Exception as e:
            logger.warning(f"Failed to load projects from tags: {e}")
            return 0

    # ========== PHASE 4: Memory Quality + Deduplication ==========

    async def _find_near_duplicate(
        self,
        content: str,
        threshold: float = 0.95
    ) -> Optional[Dict[str, Any]]:
        """Check if similar content already exists in Vectorize.

        Args:
            content: Content to check for duplicates
            threshold: Similarity threshold (0.95 = 95% similar)

        Returns:
            Dict with duplicate info if found, None otherwise
        """
        try:
            # Get embedding for new content
            embedding = await self._generate_embedding(content)

            # Search for very similar vectors
            payload = {
                "vector": embedding,
                "topK": 3,
                "returnMetadata": "all",
                "returnValues": False
            }

            response = await self._retry_request(
                "POST",
                f"{self.vectorize_url}/query",
                json=payload
            )
            result = response.json()

            if result.get("success"):
                matches = result.get("result", {}).get("matches", [])

                for match in matches:
                    score = match.get("score", 0)
                    if score >= threshold:
                        return {
                            "hash": match.get("id"),
                            "score": score,
                            "metadata": match.get("metadata", {})
                        }

            return None
        except Exception as e:
            logger.warning(f"Near-duplicate check failed: {e}")
            return None

    async def find_duplicates(
        self,
        similarity_threshold: float = 0.90,
        batch_size: int = 100,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """Find semantically similar memories that may be duplicates.

        Args:
            similarity_threshold: Minimum similarity to consider duplicate (0-1)
            batch_size: Number of memories to check per batch
            max_results: Maximum duplicate pairs to return

        Returns:
            List of duplicate pairs with similarity scores
        """
        duplicates: List[Dict[str, Any]] = []
        checked_pairs: set = set()

        try:
            # Get recent memories from D1
            sql = """
            SELECT content_hash, content, created_at_iso
            FROM memories
            ORDER BY created_at DESC
            LIMIT ?
            """
            payload = {"sql": sql, "params": [batch_size * 5]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if not result.get("success") or not result.get("result", [{}])[0].get("results"):
                return []

            memories = result["result"][0]["results"]

            for mem in memories:
                if len(duplicates) >= max_results:
                    break

                content_hash = mem.get("content_hash", "")
                content = mem.get("content", "")

                if not content:
                    continue

                # Get embedding and search for similar
                try:
                    embedding = await self._generate_embedding(content)

                    search_payload = {
                        "vector": embedding,
                        "topK": 5,
                        "returnMetadata": "all",
                        "returnValues": False
                    }

                    search_response = await self._retry_request(
                        "POST",
                        f"{self.vectorize_url}/query",
                        json=search_payload
                    )
                    search_result = search_response.json()

                    if search_result.get("success"):
                        matches = search_result.get("result", {}).get("matches", [])

                        for match in matches:
                            match_id = match.get("id", "")
                            score = match.get("score", 0)

                            # Skip self-match
                            if match_id == content_hash:
                                continue

                            # Skip if pair already checked
                            pair_key = tuple(sorted([content_hash, match_id]))
                            if pair_key in checked_pairs:
                                continue

                            checked_pairs.add(pair_key)

                            # Check threshold
                            if score >= similarity_threshold:
                                # Get details of duplicate
                                dup_mem = await self._get_memory_by_hash(match_id)

                                duplicates.append({
                                    "memory_1": {
                                        "hash": content_hash,
                                        "content_preview": content[:200],
                                        "created_at": mem.get("created_at_iso", "")
                                    },
                                    "memory_2": {
                                        "hash": match_id,
                                        "content_preview": dup_mem.get("content", "")[:200] if dup_mem else "",
                                        "created_at": dup_mem.get("created_at_iso", "") if dup_mem else ""
                                    },
                                    "similarity": score,
                                    "recommendation": self._recommend_dedup_action(
                                        mem.get("created_at_iso", ""),
                                        dup_mem.get("created_at_iso", "") if dup_mem else "",
                                        score
                                    )
                                })

                except Exception as e:
                    logger.warning(f"Error checking memory {content_hash}: {e}")
                    continue

            return duplicates

        except Exception as e:
            logger.error(f"find_duplicates failed: {e}")
            return []

    async def _get_memory_by_hash(self, content_hash: str) -> Optional[Dict[str, Any]]:
        """Get memory details by content hash."""
        try:
            sql = "SELECT content, memory_type, created_at_iso FROM memories WHERE content_hash = ?"
            payload = {"sql": sql, "params": [content_hash]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if result.get("success") and result.get("result", [{}])[0].get("results"):
                results = result["result"][0]["results"]
                if results:
                    return results[0]
            return None
        except Exception as e:
            logger.warning(f"Failed to get memory by hash {content_hash}: {e}")
            return None

    def _recommend_dedup_action(
        self,
        date1: str,
        date2: str,
        similarity: float
    ) -> str:
        """Recommend action for duplicate pair."""
        # Very similar = keep newer, delete older
        if similarity >= 0.98:
            if date1 > date2:
                return "delete_second"
            else:
                return "delete_first"

        # Moderately similar = suggest review
        if similarity >= 0.90:
            return "review_and_merge"

        return "keep_both"

    async def consolidate_memories(
        self,
        hashes: List[str],
        keep_hash: str,
        merge_tags: bool = True
    ) -> Tuple[bool, str]:
        """Consolidate multiple memories into one.

        Args:
            hashes: List of memory hashes to consolidate
            keep_hash: Hash of memory to keep
            merge_tags: Whether to merge tags from deleted memories

        Returns:
            (success, message)
        """
        if keep_hash not in hashes:
            return False, "keep_hash must be in hashes list"

        if len(hashes) < 2:
            return False, "Need at least 2 hashes to consolidate"

        # Verify all hashes exist
        for h in hashes:
            exists = await self._memory_exists(h)
            if not exists:
                return False, f"Memory {h} not found"

        # Collect tags from all memories if merging
        all_tags: set = set()
        if merge_tags:
            for h in hashes:
                tags = await self._get_memory_tags(h)
                all_tags.update(tags)

        # Delete duplicates (keep keep_hash)
        deleted = 0
        for h in hashes:
            if h != keep_hash:
                success = await self.delete(h)
                if success:
                    deleted += 1

        # Update tags on kept memory if merged
        if merge_tags and all_tags:
            await self._update_memory_tags(keep_hash, list(all_tags))

        return True, f"Consolidated {len(hashes)} memories. Deleted {deleted}, kept {keep_hash}"

    async def _memory_exists(self, content_hash: str) -> bool:
        """Check if memory exists."""
        try:
            sql = "SELECT 1 FROM memories WHERE content_hash = ? LIMIT 1"
            payload = {"sql": sql, "params": [content_hash]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if result.get("success") and result.get("result", [{}])[0].get("results"):
                return len(result["result"][0]["results"]) > 0
            return False
        except Exception:
            return False

    async def _get_memory_tags(self, content_hash: str) -> List[str]:
        """Get tags for a memory."""
        try:
            sql = """
            SELECT t.tag
            FROM tags t
            JOIN memory_tags mt ON t.id = mt.tag_id
            JOIN memories m ON mt.memory_id = m.id
            WHERE m.content_hash = ?
            """
            payload = {"sql": sql, "params": [content_hash]}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            if result.get("success") and result.get("result", [{}])[0].get("results"):
                return [r.get("tag", "") for r in result["result"][0]["results"]]
            return []
        except Exception:
            return []

    async def cleanup_old_duplicates(
        self,
        max_age_days: int = 90,
        similarity_threshold: float = 0.98,
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """Automatically clean up old near-duplicates.

        Args:
            max_age_days: Only process memories older than this
            similarity_threshold: Only auto-delete if similarity >= this
            dry_run: If True, only report what would be deleted

        Returns:
            Summary of cleanup actions
        """
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).strftime("%Y-%m-%d")

        # Find duplicates with high threshold
        duplicates = await self.find_duplicates(
            similarity_threshold=similarity_threshold,
            max_results=1000
        )

        # Filter to old ones
        old_duplicates = [
            d for d in duplicates
            if d["memory_2"]["created_at"][:10] < cutoff_date
        ]

        results: Dict[str, Any] = {
            "total_found": len(duplicates),
            "old_duplicates": len(old_duplicates),
            "would_delete": [],
            "deleted": []
        }

        for dup in old_duplicates:
            # Prefer keeping newer
            if dup["memory_1"]["created_at"] > dup["memory_2"]["created_at"]:
                to_delete = dup["memory_2"]["hash"]
                to_keep = dup["memory_1"]["hash"]
            else:
                to_delete = dup["memory_1"]["hash"]
                to_keep = dup["memory_2"]["hash"]

            if dry_run:
                results["would_delete"].append({
                    "delete": to_delete,
                    "keep": to_keep,
                    "similarity": dup["similarity"]
                })
            else:
                success = await self.delete(to_delete)
                if success:
                    results["deleted"].append(to_delete)

        return results

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
        # Check cache first
        text_hash = hashlib.sha256(text.encode()).hexdigest()
        if text_hash in self._embedding_cache:
            return self._embedding_cache[text_hash]
        
        try:
            # Use Workers AI to generate embedding
            payload = {"text": [text]}
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
            r2_key TEXT
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
        """Store a memory in Cloudflare storage.

        Includes auto-dedup warning: if similar content exists (>90% similarity),
        the memory is still stored but returns a warning in the message.
        """
        try:
            # Generate embedding for the content
            embedding = await self._generate_embedding(memory.content)

            # IMPROVEMENT #4: Check for near-duplicates (warning only, no blocking)
            duplicate_warning = ""
            try:
                similar = await self._find_near_duplicate(memory.content, threshold=0.90)
                if similar:
                    duplicate_warning = f" ⚠️ WARNING: Similar content exists (similarity: {similar['score']:.1%}). Consider reviewing: {similar['hash'][:16]}..."
            except Exception as e:
                logger.debug(f"Duplicate check skipped: {e}")
            
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
                "content_hash": memory.content_hash,
                "memory_type": memory.memory_type or "standard",
                "tags": tags_str,
                "created_at": created_at_iso,
                # PHASE 1: Enhanced metadata for Vectorize filtering
                "project": self._extract_project_from_tags(tags_str),
                "type": memory.memory_type or self._extract_type_from_tags(tags_str),
                "status": self._extract_status_from_tags(tags_str),
                "date": created_at_iso[:10] if created_at_iso else None  # YYYY-MM-DD
            }

            await self._store_vectorize_vector(vector_id, embedding, vector_metadata)
            
            # Store metadata in D1
            await self._store_d1_memory(memory, vector_id, content_size, r2_key, stored_content)
            
            logger.info(f"Successfully stored memory: {memory.content_hash}")
            return True, f"Memory stored successfully (vector_id: {vector_id}){duplicate_warning}"
            
        except Exception as e:
            logger.error(f"Failed to store memory {memory.content_hash}: {e}")
            return False, f"Storage failed: {str(e)}"
    
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
    
    async def retrieve(
        self,
        query: str,
        n_results: int = 5,
        filters: Optional[Dict[str, Any]] = None,
        smart: bool = True
    ) -> List[MemoryQueryResult]:
        """Retrieve memories by semantic search with optional metadata filtering.

        Args:
            query: Search query text
            n_results: Number of results to return
            filters: Optional metadata filters (e.g., {"project": "freel", "type": "task"})
            smart: Use smart query understanding + hybrid search (default True)

        Returns:
            List of MemoryQueryResult objects
        """
        # IMPROVEMENT #1: Smart retrieve as default
        if smart:
            try:
                smart_results = await self.smart_retrieve(query, n_results, use_hybrid=True)
                # Convert smart_retrieve dict results to MemoryQueryResult objects
                results = []
                for r in smart_results.get("results", []):
                    memory = Memory(
                        content=r.get("content", ""),
                        content_hash=r.get("content_hash", ""),
                        memory_type=r.get("memory_type"),
                        created_at_iso=r.get("created_at_iso"),
                        tags=r.get("tags", "").split(",") if r.get("tags") else []
                    )
                    results.append(MemoryQueryResult(
                        memory=memory,
                        relevance_score=r.get("rrf_score", r.get("score", 0.0))
                    ))
                return results[:n_results]
            except Exception as e:
                logger.warning(f"Smart retrieve failed, falling back to semantic: {e}")
                # Fall through to semantic-only search

        try:
            # Generate query embedding
            query_embedding = await self._generate_embedding(query)

            # Search Vectorize with optional filtering
            search_payload = {
                "vector": query_embedding,
                "topK": n_results,
                "returnMetadata": "all",
                "returnValues": False
            }

            # PHASE 1: Add metadata filter if provided
            if filters:
                vectorize_filter = self._build_vectorize_filter(filters)
                if vectorize_filter:
                    search_payload["filter"] = vectorize_filter
                    logger.info(f"Applying Vectorize filter: {vectorize_filter}")

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
    
    async def _get_vectorize_index_info(self) -> Dict[str, Any]:
        """Get Vectorize index info including actual vector count.

        PHASE 1: Fix stats bug - get real vector count from Vectorize API.
        """
        try:
            response = await self._retry_request("GET", f"{self.vectorize_url}")
            result = response.json()

            if result.get("success") and result.get("result"):
                return result["result"]
            return {}
        except Exception as e:
            logger.warning(f"Failed to get Vectorize index info: {e}")
            return {}

    async def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics with accurate vector count from Vectorize API."""
        try:
            # Calculate timestamp for memories from last 7 days
            week_ago = time.time() - (7 * 24 * 60 * 60)

            # Get memory count and size from D1
            sql = f"""
            SELECT
                COUNT(*) as total_memories,
                SUM(content_size) as total_content_size,
                COUNT(r2_key) as r2_stored_count,
                (SELECT COUNT(*) FROM tags) as unique_tags,
                (SELECT COUNT(*) FROM memories WHERE created_at >= {week_ago}) as memories_this_week
            FROM memories
            """

            payload = {"sql": sql}
            response = await self._retry_request("POST", f"{self.d1_url}/query", json=payload)
            result = response.json()

            # PHASE 1: Get actual vector count from Vectorize API (not from D1)
            vectorize_info = await self._get_vectorize_index_info()
            actual_vector_count = vectorize_info.get("vectorCount", 0)
            vectorize_dimensions = vectorize_info.get("dimensions", 768)

            if result.get("success") and result.get("result", [{}])[0].get("results"):
                stats = result["result"][0]["results"][0]

                return {
                    "total_memories": stats.get("total_memories", 0),
                    "unique_tags": stats.get("unique_tags", 0),
                    "memories_this_week": stats.get("memories_this_week", 0),
                    "total_content_size_bytes": stats.get("total_content_size", 0),
                    "total_vectors": actual_vector_count,  # FIXED: From Vectorize API
                    "vectorize_dimensions": vectorize_dimensions,
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
                "total_vectors": actual_vector_count,
                "storage_backend": "cloudflare",
                "status": "operational"
            }

        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {
                "total_memories": 0,
                "unique_tags": 0,
                "memories_this_week": 0,
                "total_vectors": 0,
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

    # ========== IMPROVEMENT #2: Find Stale Tasks ==========

    async def find_stale_tasks(
        self,
        days: int = 7,
        project: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Find tasks that are in-progress for too long.

        Args:
            days: Tasks older than this many days are considered stale
            project: Optional project filter

        Returns:
            List of stale task memories
        """
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

            sql = """
            SELECT m.content_hash, m.content, m.memory_type, m.created_at_iso,
                   GROUP_CONCAT(t.name, ',') as tags
            FROM memories m
            LEFT JOIN memory_tags mt ON m.id = mt.memory_id
            LEFT JOIN tags t ON mt.tag_id = t.id
            WHERE m.created_at_iso < ?
            GROUP BY m.content_hash
            HAVING tags LIKE '%status:in-progress%'
            """

            params = [cutoff]

            if project:
                sql = sql.replace(
                    "HAVING tags LIKE '%status:in-progress%'",
                    f"HAVING tags LIKE '%status:in-progress%' AND tags LIKE '%project:{project}%'"
                )

            sql += " ORDER BY m.created_at_iso ASC LIMIT 100"

            response = await self._retry_request("POST", f"{self.d1_url}/query", json={"sql": sql, "params": params})
            result = response.json()

            if not result.get("success"):
                raise ValueError(f"D1 query failed: {result}")

            results = result.get("result", [{}])[0].get("results", [])
            logger.info(f"Found {len(results)} stale tasks older than {days} days")
            return results

        except Exception as e:
            logger.error(f"Error finding stale tasks: {e}")
            return []

    # ========== IMPROVEMENT #3: Load Project Context (PRIORITY) ==========

    async def load_project_context(
        self,
        project: str,
        include_tasks: bool = True,
        include_recent: bool = True,
        include_config: bool = True,
        days: int = 14
    ) -> Dict[str, Any]:
        """Load full context for a project - perfect for starting a new session.

        Args:
            project: Project name (e.g., "freel", "mind-space")
            include_tasks: Include pending and in-progress tasks
            include_recent: Include recent sessions and decisions
            include_config: Include project configuration/setup info
            days: How many days back to look for recent items

        Returns:
            Dict with project context sections
        """
        context = {
            "project": project,
            "loaded_at": datetime.now(timezone.utc).isoformat(),
            "pending_tasks": [],
            "in_progress_tasks": [],
            "recent_sessions": [],
            "recent_decisions": [],
            "recent_errors": [],
            "config_and_setup": [],
            "client_chats": [],
            "summary": ""
        }

        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

            # Get tasks (pending and in-progress)
            if include_tasks:
                for status in ["pending", "in-progress"]:
                    sql = """
                    SELECT m.content_hash, m.content, m.memory_type, m.created_at_iso,
                           GROUP_CONCAT(t.name, ',') as tags
                    FROM memories m
                    LEFT JOIN memory_tags mt ON m.id = mt.memory_id
                    LEFT JOIN tags t ON mt.tag_id = t.id
                    GROUP BY m.content_hash
                    HAVING tags LIKE ? AND tags LIKE ?
                    ORDER BY m.created_at_iso DESC
                    LIMIT 20
                    """
                    response = await self._retry_request(
                        "POST", f"{self.d1_url}/query",
                        json={"sql": sql, "params": [f"%project:{project}%", f"%status:{status}%"]}
                    )
                    result = response.json()
                    if result.get("success"):
                        tasks = result.get("result", [{}])[0].get("results", [])
                        if status == "pending":
                            context["pending_tasks"] = tasks
                        else:
                            context["in_progress_tasks"] = tasks

            # Get recent items by type
            if include_recent:
                for item_type, context_key in [
                    ("session", "recent_sessions"),
                    ("decision", "recent_decisions"),
                    ("error", "recent_errors"),
                    ("client-chat", "client_chats"),
                ]:
                    sql = """
                    SELECT m.content_hash, m.content, m.memory_type, m.created_at_iso,
                           GROUP_CONCAT(t.name, ',') as tags
                    FROM memories m
                    LEFT JOIN memory_tags mt ON m.id = mt.memory_id
                    LEFT JOIN tags t ON mt.tag_id = t.id
                    WHERE m.created_at_iso >= ?
                    GROUP BY m.content_hash
                    HAVING tags LIKE ? AND tags LIKE ?
                    ORDER BY m.created_at_iso DESC
                    LIMIT 10
                    """
                    response = await self._retry_request(
                        "POST", f"{self.d1_url}/query",
                        json={"sql": sql, "params": [cutoff, f"%project:{project}%", f"%type:{item_type}%"]}
                    )
                    result = response.json()
                    if result.get("success"):
                        context[context_key] = result.get("result", [{}])[0].get("results", [])

            # Get config/setup info (no time limit)
            if include_config:
                sql = """
                SELECT m.content_hash, m.content, m.memory_type, m.created_at_iso,
                       GROUP_CONCAT(t.name, ',') as tags
                FROM memories m
                LEFT JOIN memory_tags mt ON m.id = mt.memory_id
                LEFT JOIN tags t ON mt.tag_id = t.id
                GROUP BY m.content_hash
                HAVING tags LIKE ? AND (tags LIKE '%type:config%' OR tags LIKE '%type:setup%' OR tags LIKE '%type:architecture%')
                ORDER BY m.created_at_iso DESC
                LIMIT 10
                """
                response = await self._retry_request(
                    "POST", f"{self.d1_url}/query",
                    json={"sql": sql, "params": [f"%project:{project}%"]}
                )
                result = response.json()
                if result.get("success"):
                    context["config_and_setup"] = result.get("result", [{}])[0].get("results", [])

            # Generate summary
            total_pending = len(context["pending_tasks"])
            total_in_progress = len(context["in_progress_tasks"])
            total_errors = len(context["recent_errors"])

            context["summary"] = (
                f"Project: {project}\n"
                f"Pending tasks: {total_pending}\n"
                f"In-progress tasks: {total_in_progress}\n"
                f"Recent errors: {total_errors}\n"
                f"Recent sessions: {len(context['recent_sessions'])}\n"
                f"Recent decisions: {len(context['recent_decisions'])}"
            )

            logger.info(f"Loaded context for project {project}: {total_pending} pending, {total_in_progress} in-progress")
            return context

        except Exception as e:
            logger.error(f"Error loading project context: {e}")
            context["error"] = str(e)
            return context

    # ========== IMPROVEMENT #5: Session State Helpers ==========

    async def save_session_state(
        self,
        project: str,
        current_task: str,
        progress: str,
        next_steps: List[str],
        blockers: Optional[List[str]] = None
    ) -> Tuple[bool, str]:
        """Save current session state for continuity.

        Call this before context overflow or session end.

        Args:
            project: Project name
            current_task: What you're currently working on
            progress: What's been done
            next_steps: What needs to be done next
            blockers: Any blockers or issues

        Returns:
            Tuple of (success, message/hash)
        """
        content = f"""SESSION STATE: {project}
Saved: {datetime.now(timezone.utc).isoformat()}

CURRENT TASK:
{current_task}

PROGRESS:
{progress}

NEXT STEPS:
{chr(10).join(f'- {step}' for step in next_steps)}
"""
        if blockers:
            content += f"\nBLOCKERS:\n{chr(10).join(f'- {b}' for b in blockers)}"

        # Generate content hash
        content_hash = hashlib.sha256(content.encode()).hexdigest()

        memory = Memory(
            content=content,
            content_hash=content_hash,
            memory_type="session",
            tags=[f"project:{project}", "type:session", "session-state", "auto-save"]
        )

        return await self.store(memory)

    async def get_interrupted_sessions(
        self,
        project: Optional[str] = None,
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """Find sessions that were saved but not marked as completed.

        Use this at session start to resume interrupted work.

        Args:
            project: Optional project filter
            days: Look back this many days

        Returns:
            List of interrupted session states
        """
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

            sql = """
            SELECT m.content_hash, m.content, m.memory_type, m.created_at_iso,
                   GROUP_CONCAT(t.name, ',') as tags
            FROM memories m
            LEFT JOIN memory_tags mt ON m.id = mt.memory_id
            LEFT JOIN tags t ON mt.tag_id = t.id
            WHERE m.created_at_iso >= ?
            GROUP BY m.content_hash
            HAVING tags LIKE '%session-state%'
            """

            params = [cutoff]

            if project:
                sql = sql.replace(
                    "HAVING tags LIKE '%session-state%'",
                    f"HAVING tags LIKE '%session-state%' AND tags LIKE '%project:{project}%'"
                )

            sql += " ORDER BY m.created_at_iso DESC LIMIT 20"

            response = await self._retry_request("POST", f"{self.d1_url}/query", json={"sql": sql, "params": params})
            result = response.json()

            if not result.get("success"):
                raise ValueError(f"D1 query failed: {result}")

            return result.get("result", [{}])[0].get("results", [])

        except Exception as e:
            logger.error(f"Error getting interrupted sessions: {e}")
            return []

    # ========== IMPROVEMENT #6: Memory Analytics ==========

    async def get_memory_analytics(
        self,
        project: Optional[str] = None,
        days: int = 30
    ) -> Dict[str, Any]:
        """Get analytics about memory usage.

        Args:
            project: Optional project filter
            days: Analyze this many days

        Returns:
            Dict with analytics data
        """
        analytics = {
            "period_days": days,
            "project": project or "all",
            "by_type": {},
            "by_status": {},
            "by_project": {},
            "daily_counts": [],
            "total_memories": 0,
            "duplicate_estimate": 0
        }

        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

            # Total count
            sql_total = "SELECT COUNT(*) as count FROM memories WHERE created_at_iso >= ?"
            response = await self._retry_request("POST", f"{self.d1_url}/query", json={"sql": sql_total, "params": [cutoff]})
            result = response.json()
            if result.get("success"):
                analytics["total_memories"] = result.get("result", [{}])[0].get("results", [{}])[0].get("count", 0)

            # By type
            sql_by_type = """
            SELECT memory_type, COUNT(*) as count
            FROM memories
            WHERE created_at_iso >= ?
            GROUP BY memory_type
            ORDER BY count DESC
            """
            response = await self._retry_request("POST", f"{self.d1_url}/query", json={"sql": sql_by_type, "params": [cutoff]})
            result = response.json()
            if result.get("success"):
                for row in result.get("result", [{}])[0].get("results", []):
                    analytics["by_type"][row.get("memory_type") or "unknown"] = row.get("count", 0)

            # By project (from tags)
            sql_by_project = """
            SELECT t.name, COUNT(DISTINCT m.id) as count
            FROM memories m
            JOIN memory_tags mt ON m.id = mt.memory_id
            JOIN tags t ON mt.tag_id = t.id
            WHERE t.name LIKE 'project:%' AND m.created_at_iso >= ?
            GROUP BY t.name
            ORDER BY count DESC
            LIMIT 20
            """
            response = await self._retry_request("POST", f"{self.d1_url}/query", json={"sql": sql_by_project, "params": [cutoff]})
            result = response.json()
            if result.get("success"):
                for row in result.get("result", [{}])[0].get("results", []):
                    project_name = row.get("name", "").replace("project:", "")
                    analytics["by_project"][project_name] = row.get("count", 0)

            # By status (from tags)
            sql_by_status = """
            SELECT t.name, COUNT(DISTINCT m.id) as count
            FROM memories m
            JOIN memory_tags mt ON m.id = mt.memory_id
            JOIN tags t ON mt.tag_id = t.id
            WHERE t.name LIKE 'status:%' AND m.created_at_iso >= ?
            GROUP BY t.name
            ORDER BY count DESC
            """
            response = await self._retry_request("POST", f"{self.d1_url}/query", json={"sql": sql_by_status, "params": [cutoff]})
            result = response.json()
            if result.get("success"):
                for row in result.get("result", [{}])[0].get("results", []):
                    status_name = row.get("name", "").replace("status:", "")
                    analytics["by_status"][status_name] = row.get("count", 0)

            # Daily counts (last 7 days)
            sql_daily = """
            SELECT DATE(created_at_iso) as day, COUNT(*) as count
            FROM memories
            WHERE created_at_iso >= ?
            GROUP BY DATE(created_at_iso)
            ORDER BY day DESC
            LIMIT 7
            """
            response = await self._retry_request("POST", f"{self.d1_url}/query", json={"sql": sql_daily, "params": [cutoff]})
            result = response.json()
            if result.get("success"):
                analytics["daily_counts"] = result.get("result", [{}])[0].get("results", [])

            logger.info(f"Generated analytics: {analytics['total_memories']} memories in {days} days")
            return analytics

        except Exception as e:
            logger.error(f"Error generating analytics: {e}")
            analytics["error"] = str(e)
            return analytics

    # ========== IMPROVEMENT #7: Safe Cleanup (dry-run by default) ==========

    async def cleanup_old_memories(
        self,
        older_than_days: int = 90,
        memory_types: Optional[List[str]] = None,
        exclude_tags: Optional[List[str]] = None,
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """Clean up old memories safely.

        IMPORTANT: dry_run=True by default to prevent accidental deletion.

        Args:
            older_than_days: Delete memories older than this
            memory_types: Only these types (e.g., ["session", "test"])
            exclude_tags: Never delete if has these tags (e.g., ["important", "keep"])
            dry_run: If True, only report what would be deleted (default: True)

        Returns:
            Dict with cleanup results
        """
        result = {
            "dry_run": dry_run,
            "older_than_days": older_than_days,
            "would_delete": 0,
            "deleted": 0,
            "preserved": 0,
            "samples": []
        }

        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=older_than_days)).strftime("%Y-%m-%d")

            # Build query
            sql = """
            SELECT m.content_hash, m.content, m.memory_type, m.created_at_iso,
                   GROUP_CONCAT(t.name, ',') as tags
            FROM memories m
            LEFT JOIN memory_tags mt ON m.id = mt.memory_id
            LEFT JOIN tags t ON mt.tag_id = t.id
            WHERE m.created_at_iso < ?
            GROUP BY m.content_hash
            """

            params = [cutoff]

            # Filter by memory types if specified
            if memory_types:
                placeholders = ",".join("?" * len(memory_types))
                sql += f" HAVING m.memory_type IN ({placeholders})"
                params.extend(memory_types)

            sql += " ORDER BY m.created_at_iso ASC LIMIT 500"

            response = await self._retry_request("POST", f"{self.d1_url}/query", json={"sql": sql, "params": params})
            query_result = response.json()

            if not query_result.get("success"):
                raise ValueError(f"D1 query failed: {query_result}")

            candidates = query_result.get("result", [{}])[0].get("results", [])

            # Filter out excluded tags
            exclude_tags = exclude_tags or ["important", "keep", "permanent", "config", "architecture"]
            to_delete = []

            for mem in candidates:
                tags = mem.get("tags", "") or ""
                should_preserve = any(excl in tags for excl in exclude_tags)

                if should_preserve:
                    result["preserved"] += 1
                else:
                    to_delete.append(mem)

            result["would_delete"] = len(to_delete)
            result["samples"] = [
                {"hash": m["content_hash"][:16], "type": m.get("memory_type"), "date": m.get("created_at_iso", "")[:10]}
                for m in to_delete[:10]
            ]

            # Actually delete if not dry run
            if not dry_run and to_delete:
                for mem in to_delete:
                    try:
                        await self.delete(mem["content_hash"])
                        result["deleted"] += 1
                    except Exception as e:
                        logger.error(f"Failed to delete {mem['content_hash']}: {e}")

            logger.info(f"Cleanup {'(dry-run)' if dry_run else ''}: {result['would_delete']} candidates, {result['deleted']} deleted, {result['preserved']} preserved")
            return result

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            result["error"] = str(e)
            return result

    async def close(self) -> None:
        """Close the storage backend and cleanup resources."""
        if self.client:
            await self.client.aclose()
            self.client = None

        # Clear embedding cache
        self._embedding_cache.clear()

        logger.info("Cloudflare storage backend closed")
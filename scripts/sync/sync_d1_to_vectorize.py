#!/usr/bin/env python3
"""
Sync D1 memories to Vectorize index.

This script ensures all memories in D1 have corresponding vectors in Vectorize.
Run this when:
- Vectorize index is out of sync with D1
- After bulk D1 imports
- When vectors are missing for existing memories

Usage:
    python scripts/sync/sync_d1_to_vectorize.py                    # Sync all missing
    python scripts/sync/sync_d1_to_vectorize.py --dry-run          # Preview only
    python scripts/sync/sync_d1_to_vectorize.py --batch-size 50    # Custom batch size
    python scripts/sync/sync_d1_to_vectorize.py --limit 100        # Sync only first 100 missing
"""
import sys
import os
import asyncio
import logging
import argparse
import json
from pathlib import Path
from typing import List, Dict, Any, Set, Optional
from datetime import datetime

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from mcp_memory_service.config import (
    CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_VECTORIZE_INDEX,
    CLOUDFLARE_D1_DATABASE_ID
)
from mcp_memory_service.storage.cloudflare import CloudflareStorage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger("d1_vectorize_sync")


class D1VectorizeSync:
    """Syncs D1 memories to Vectorize index."""

    def __init__(self):
        """Initialize with Cloudflare storage."""
        self.storage = CloudflareStorage(
            api_token=CLOUDFLARE_API_TOKEN,
            account_id=CLOUDFLARE_ACCOUNT_ID,
            vectorize_index=CLOUDFLARE_VECTORIZE_INDEX,
            d1_database_id=CLOUDFLARE_D1_DATABASE_ID
        )

    async def initialize(self):
        """Initialize storage connection."""
        await self.storage.initialize()

    async def get_d1_memories(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all memories from D1 with their tags."""
        # Get memories with tags joined
        sql = """
        SELECT
            m.content_hash,
            m.content,
            m.memory_type,
            m.created_at_iso,
            GROUP_CONCAT(t.name, ',') as tags
        FROM memories m
        LEFT JOIN memory_tags mt ON m.id = mt.memory_id
        LEFT JOIN tags t ON mt.tag_id = t.id
        GROUP BY m.content_hash
        ORDER BY m.created_at DESC
        """

        if limit:
            sql += f" LIMIT {limit}"

        client = await self.storage._get_client()
        response = await client.post(
            f"{self.storage.d1_url}/query",
            json={"sql": sql},
            headers={"Authorization": f"Bearer {self.storage.api_token}"}
        )

        result = response.json()
        if not result.get("success"):
            raise ValueError(f"D1 query failed: {result}")

        results = result.get("result", [{}])[0].get("results", [])
        logger.info(f"Found {len(results)} memories in D1")
        return results

    async def get_vectorize_ids(self) -> Set[str]:
        """Get all vector IDs from Vectorize."""
        # Vectorize doesn't have a list-all endpoint, so we'll use a workaround:
        # Query with an empty/random vector to get some results, but this won't give us all
        # Instead, we'll query D1 for vector_id column which should match

        sql = "SELECT DISTINCT vector_id FROM memories WHERE vector_id IS NOT NULL"
        client = await self.storage._get_client()
        response = await client.post(
            f"{self.storage.d1_url}/query",
            json={"sql": sql},
            headers={"Authorization": f"Bearer {self.storage.api_token}"}
        )

        result = response.json()
        if not result.get("success"):
            logger.warning(f"Could not get vector_ids from D1: {result}")
            return set()

        results = result.get("result", [{}])[0].get("results", [])
        vector_ids = {r["vector_id"] for r in results if r.get("vector_id")}
        logger.info(f"Found {len(vector_ids)} vector_ids in D1")
        return vector_ids

    async def check_vector_exists(self, content_hash: str) -> bool:
        """Check if a vector exists in Vectorize by querying it."""
        try:
            client = await self.storage._get_client()
            response = await client.post(
                f"{self.storage.vectorize_url}/get-by-ids",
                json={"ids": [content_hash]},
                headers={"Authorization": f"Bearer {self.storage.api_token}"}
            )

            result = response.json()
            if result.get("success") and result.get("result"):
                vectors = result.get("result", {}).get("vectors", [])
                return len(vectors) > 0
            return False
        except Exception as e:
            logger.debug(f"Vector check failed for {content_hash}: {e}")
            return False

    async def sync_memory_to_vectorize(self, memory: Dict[str, Any]) -> bool:
        """Create vector for a single memory."""
        try:
            content = memory.get("content", "")
            content_hash = memory.get("content_hash")
            memory_type = memory.get("memory_type", "standard")
            created_at_iso = memory.get("created_at_iso", "")
            tags_str = memory.get("tags") or ""

            # Generate embedding
            embedding = await self.storage._generate_embedding(content)

            # Build enhanced metadata
            metadata = {
                "content_hash": content_hash,
                "memory_type": memory_type,
                "tags": tags_str,
                "created_at": created_at_iso,
                # Phase 1 enhanced metadata
                "project": self.storage._extract_project_from_tags(tags_str),
                "type": memory_type or self.storage._extract_type_from_tags(tags_str),
                "status": self.storage._extract_status_from_tags(tags_str),
                "date": created_at_iso[:10] if created_at_iso else None
            }

            # Store in Vectorize
            await self.storage._store_vectorize_vector(content_hash, embedding, metadata)

            # Update D1 to mark vector_id
            update_sql = "UPDATE memories SET vector_id = ? WHERE content_hash = ?"
            client = await self.storage._get_client()
            await client.post(
                f"{self.storage.d1_url}/query",
                json={"sql": update_sql, "params": [content_hash, content_hash]},
                headers={"Authorization": f"Bearer {self.storage.api_token}"}
            )

            return True

        except Exception as e:
            logger.error(f"Failed to sync memory {memory.get('content_hash')}: {e}")
            return False

    async def find_missing_vectors(self, memories: List[Dict[str, Any]], batch_check: int = 100) -> List[Dict[str, Any]]:
        """Find memories that don't have vectors in Vectorize."""
        missing = []

        for i, memory in enumerate(memories):
            content_hash = memory.get("content_hash")

            # Check every Nth memory or use batch check
            if i % 50 == 0:
                logger.info(f"Checking {i}/{len(memories)} memories...")

            exists = await self.check_vector_exists(content_hash)
            if not exists:
                missing.append(memory)

        logger.info(f"Found {len(missing)} memories without vectors")
        return missing

    async def sync_all(
        self,
        dry_run: bool = False,
        batch_size: int = 20,
        limit: Optional[int] = None,
        skip_check: bool = False
    ) -> Dict[str, int]:
        """Sync all missing memories to Vectorize."""
        stats = {
            "total_d1": 0,
            "missing_vectors": 0,
            "synced": 0,
            "failed": 0
        }

        # Get all D1 memories
        memories = await self.get_d1_memories(limit=limit if not skip_check else None)
        stats["total_d1"] = len(memories)

        if skip_check:
            # Sync all without checking (faster for full reindex)
            missing = memories[:limit] if limit else memories
        else:
            # Find missing vectors
            logger.info("Checking which memories are missing vectors...")
            missing = await self.find_missing_vectors(memories)
            if limit:
                missing = missing[:limit]

        stats["missing_vectors"] = len(missing)

        if dry_run:
            logger.info(f"DRY RUN: Would sync {len(missing)} memories")
            for m in missing[:10]:
                logger.info(f"  - {m.get('content_hash')[:16]}... | tags: {m.get('tags', 'none')[:50]}")
            if len(missing) > 10:
                logger.info(f"  ... and {len(missing) - 10} more")
            return stats

        # Sync in batches
        logger.info(f"Syncing {len(missing)} memories in batches of {batch_size}...")

        for i in range(0, len(missing), batch_size):
            batch = missing[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(missing) + batch_size - 1) // batch_size

            logger.info(f"Processing batch {batch_num}/{total_batches}...")

            for memory in batch:
                success = await self.sync_memory_to_vectorize(memory)
                if success:
                    stats["synced"] += 1
                else:
                    stats["failed"] += 1

            # Small delay between batches to avoid rate limits
            if i + batch_size < len(missing):
                await asyncio.sleep(0.5)

        logger.info(f"Sync complete: {stats['synced']} synced, {stats['failed']} failed")
        return stats


async def main():
    parser = argparse.ArgumentParser(description="Sync D1 memories to Vectorize")
    parser.add_argument("--dry-run", action="store_true", help="Preview without making changes")
    parser.add_argument("--batch-size", type=int, default=20, help="Batch size for sync (default: 20)")
    parser.add_argument("--limit", type=int, help="Limit number of memories to sync")
    parser.add_argument("--skip-check", action="store_true", help="Skip vector existence check (full reindex)")

    args = parser.parse_args()

    sync = D1VectorizeSync()
    await sync.initialize()

    print("\n" + "="*60)
    print("D1 → Vectorize Sync")
    print("="*60 + "\n")

    stats = await sync.sync_all(
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        limit=args.limit,
        skip_check=args.skip_check
    )

    print("\n" + "="*60)
    print("RESULTS:")
    print(f"  Total D1 memories:    {stats['total_d1']}")
    print(f"  Missing vectors:      {stats['missing_vectors']}")
    print(f"  Successfully synced:  {stats['synced']}")
    print(f"  Failed:               {stats['failed']}")
    print("="*60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())

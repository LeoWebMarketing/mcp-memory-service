#!/usr/bin/env python3
"""
Build initial graph relationships for existing memories.

This script analyzes all existing memories and creates 'related_to' edges
between semantically similar ones (similarity > threshold).

Usage:
    python scripts/maintenance/build_initial_graph.py --threshold 0.75 --dry-run
    python scripts/maintenance/build_initial_graph.py --threshold 0.75  # Execute
"""

import asyncio
import argparse
import os
import sys
import httpx

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.mcp_memory_service.storage.cloudflare import CloudflareStorage


async def check_edge_exists(client, url, headers, source_hash, target_hash):
    """Check if an edge already exists between two memories."""
    sql = """
        SELECT id FROM memory_edges
        WHERE (source_hash = ?1 AND target_hash = ?2)
           OR (source_hash = ?2 AND target_hash = ?1)
    """
    resp = await client.post(url, headers=headers, json={
        'sql': sql,
        'params': [source_hash, target_hash]
    })
    data = resp.json()
    if data.get('success') and data.get('result'):
        results = data['result'][0].get('results', [])
        return len(results) > 0
    return False


async def build_graph(threshold: float = 0.75, dry_run: bool = True, max_memories: int = 500, max_edges_per_memory: int = 5):
    """Build initial graph from existing memories."""

    print(f"🔗 Building Initial Memory Graph")
    print(f"   Threshold: {threshold}")
    print(f"   Dry run: {dry_run}")
    print(f"   Max memories to process: {max_memories}")
    print(f"   Max edges per memory: {max_edges_per_memory}")
    print()

    # Get credentials
    api_token = os.environ.get("CLOUDFLARE_API_TOKEN")
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    d1_database_id = os.environ.get("CLOUDFLARE_D1_DATABASE_ID")

    if not all([api_token, account_id, d1_database_id]):
        print("❌ Missing Cloudflare credentials in environment")
        return

    # Initialize storage
    storage = CloudflareStorage(
        api_token=api_token,
        account_id=account_id,
        vectorize_index=os.environ.get("CLOUDFLARE_VECTORIZE_INDEX", "mcp-memory-index"),
        d1_database_id=d1_database_id,
        embedding_model=os.environ.get("MCP_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    )

    await storage.initialize()
    print("✅ Storage initialized")

    # D1 API endpoint
    d1_url = f'https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{d1_database_id}/query'
    headers = {'Authorization': f'Bearer {api_token}', 'Content-Type': 'application/json'}

    # Get all memories
    print("\n📊 Fetching memories...")
    memories = await storage.get_all_memories(limit=max_memories)
    print(f"   Found {len(memories)} memories to process")

    if not memories:
        print("   No memories to process")
        return

    # Track statistics
    edges_created = 0
    edges_skipped = 0
    processed = 0

    # Process memories
    print(f"\n🔍 Finding similar memories (threshold >= {threshold})...")

    async with httpx.AsyncClient(timeout=30.0) as client:
        for memory in memories:
            processed += 1

            # Skip if memory has no content
            if not memory.content or len(memory.content) < 20:
                continue

            # Find similar memories using Vectorize
            try:
                similar = await storage.search(
                    query=memory.content,
                    n_results=max_edges_per_memory + 1  # +1 to account for self
                )

                edges_for_this_memory = 0
                for result in similar:
                    # Skip self-reference
                    if result.memory.content_hash == memory.content_hash:
                        continue

                    # Skip if similarity below threshold
                    if result.relevance_score < threshold:
                        continue

                    # Limit edges per memory
                    if edges_for_this_memory >= max_edges_per_memory:
                        break

                    # Check if edge already exists
                    exists = await check_edge_exists(
                        client, d1_url, headers,
                        memory.content_hash, result.memory.content_hash
                    )

                    if exists:
                        edges_skipped += 1
                        continue

                    # Create edge
                    if dry_run:
                        content_preview = memory.content[:40].replace('\n', ' ')
                        target_preview = result.memory.content[:40].replace('\n', ' ')
                        print(f"   [DRY] {memory.content_hash[:8]}... ({content_preview}...)")
                        print(f"      <-> {result.memory.content_hash[:8]}... ({target_preview}...) sim={result.relevance_score:.3f}")
                    else:
                        await storage.link_memories(
                            source_hash=memory.content_hash,
                            target_hash=result.memory.content_hash,
                            edge_type="related_to",
                            weight=result.relevance_score,
                            metadata={"auto_generated": True, "similarity": result.relevance_score}
                        )

                    edges_created += 1
                    edges_for_this_memory += 1

            except Exception as e:
                print(f"   ⚠️ Error processing {memory.content_hash[:8]}...: {e}")

            # Progress update
            if processed % 100 == 0:
                print(f"   Progress: {processed}/{len(memories)} memories, {edges_created} edges")

    # Summary
    print(f"\n📈 Summary:")
    print(f"   Memories processed: {processed}")
    print(f"   Edges {'would be ' if dry_run else ''}created: {edges_created}")
    print(f"   Edges skipped (duplicates): {edges_skipped}")

    if dry_run:
        print(f"\n⚠️  This was a DRY RUN. Run without --dry-run to create edges.")
    else:
        print(f"\n✅ Graph building complete!")


async def main():
    parser = argparse.ArgumentParser(description="Build initial graph for existing memories")
    parser.add_argument("--threshold", type=float, default=0.75,
                       help="Similarity threshold (0.0-1.0, default: 0.75)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Preview changes without executing")
    parser.add_argument("--max-memories", type=int, default=500,
                       help="Max memories to process (default: 500)")
    parser.add_argument("--max-edges", type=int, default=5,
                       help="Max edges per memory (default: 5)")

    args = parser.parse_args()

    await build_graph(
        threshold=args.threshold,
        dry_run=args.dry_run,
        max_memories=args.max_memories,
        max_edges_per_memory=args.max_edges
    )


if __name__ == "__main__":
    asyncio.run(main())

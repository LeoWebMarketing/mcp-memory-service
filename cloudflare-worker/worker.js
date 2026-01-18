/**
 * MCP Memory Service - Cloudflare Worker API
 * Provides REST API for ChatGPT integration
 * Uses Vectorize for semantic search + D1 for metadata
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS headers for ChatGPT
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    try {
      // Route requests
      if (url.pathname === '/api/health') {
        return handleHealth(env, corsHeaders);
      }

      if (url.pathname === '/api/search' && request.method === 'POST') {
        return handleSearch(request, env, corsHeaders);
      }

      if (url.pathname === '/api/store' && request.method === 'POST') {
        return handleStore(request, env, corsHeaders);
      }

      return new Response('Not Found', { status: 404, headers: corsHeaders });

    } catch (error) {
      return new Response(JSON.stringify({ error: error.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
  }
};

// Health check endpoint
async function handleHealth(env, corsHeaders) {
  return new Response(JSON.stringify({
    status: 'healthy',
    version: '1.0.0',
    timestamp: new Date().toISOString(),
    services: {
      vectorize: !!env.VECTORIZE,
      d1: !!env.DB
    }
  }), {
    headers: { ...corsHeaders, 'Content-Type': 'application/json' }
  });
}

// Search memories - semantic search in Vectorize
async function handleSearch(request, env, corsHeaders) {
  const { query, n_results = 5 } = await request.json();

  if (!query) {
    return new Response(JSON.stringify({ error: 'query is required' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }

  // Generate embedding for query using Workers AI
  const embeddings = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
    text: query
  });

  // Search in Vectorize
  const results = await env.VECTORIZE.query(embeddings.data[0], {
    topK: n_results,
    returnMetadata: true
  });

  // Format results for ChatGPT
  const formattedResults = results.matches.map(match => ({
    content: match.metadata?.content || '',
    similarity_score: match.score,
    tags: match.metadata?.tags || [],
    created_at: match.metadata?.created_at
  }));

  return new Response(JSON.stringify({
    results: formattedResults,
    query: query,
    count: formattedResults.length
  }), {
    headers: { ...corsHeaders, 'Content-Type': 'application/json' }
  });
}

// Store new memory
async function handleStore(request, env, corsHeaders) {
  const { content, tags = [] } = await request.json();

  if (!content) {
    return new Response(JSON.stringify({ error: 'content is required' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }

  // Generate embedding
  const embeddings = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
    text: content
  });

  // Generate unique ID
  const id = crypto.randomUUID();
  const timestamp = Date.now();

  // Store in Vectorize
  await env.VECTORIZE.upsert([{
    id: id,
    values: embeddings.data[0],
    metadata: {
      content: content,
      tags: tags,
      created_at: timestamp
    }
  }]);

  // Store metadata in D1
  await env.DB.prepare(
    'INSERT INTO memories (id, content, tags, created_at) VALUES (?, ?, ?, ?)'
  ).bind(id, content, JSON.stringify(tags), timestamp).run();

  return new Response(JSON.stringify({
    success: true,
    id: id,
    content: content,
    tags: tags
  }), {
    headers: { ...corsHeaders, 'Content-Type': 'application/json' }
  });
}

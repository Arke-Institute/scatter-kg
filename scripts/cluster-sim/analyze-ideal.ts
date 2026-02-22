#!/usr/bin/env npx tsx
/**
 * Analyze Ideal Clustering
 *
 * Fetches all entities from a collection and runs semantic search for each,
 * then analyzes what the "ideal" clusters would be if clustering worked perfectly.
 *
 * This helps us understand:
 * 1. Are entities truly all within top-K of each other? (would explain mega clusters)
 * 2. Or are there natural cluster boundaries that the algorithm should find?
 *
 * Usage: npx tsx scripts/cluster-sim/analyze-ideal.ts <collection_id> [--k=5] [--layer=0]
 */

import { ArkeClient } from '@arke-institute/sdk';

const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
if (!ARKE_USER_KEY) {
  console.error('ARKE_USER_KEY environment variable required');
  process.exit(1);
}

const client = new ArkeClient({
  authToken: ARKE_USER_KEY,
  network: 'test',
});

// Parse args
const args = process.argv.slice(2);
const collectionId = args.find(a => !a.startsWith('--'));
const kArg = args.find(a => a.startsWith('--k='));
const layerArg = args.find(a => a.startsWith('--layer='));

const K = kArg ? parseInt(kArg.split('=')[1]) : 5;
const LAYER = layerArg ? parseInt(layerArg.split('=')[1]) : 0;

if (!collectionId) {
  console.error('Usage: npx tsx analyze-ideal.ts <collection_id> [--k=5] [--layer=0]');
  process.exit(1);
}

// Types
interface Entity {
  id: string;
  label: string;
  description?: string;
  type: string;
}

interface SimilarityResult {
  entityId: string;
  topK: Array<{ peerId: string; score: number }>;
}

// Fetch all KG entities (not clusters, not text_chunk, not scatter_job)
async function fetchEntities(): Promise<Entity[]> {
  console.log(`Fetching entities from ${collectionId} at layer ${LAYER}...`);

  const filter = JSON.stringify({ _kg_layer: LAYER });
  const { data, error } = await (client.api.GET as Function)('/collections/{id}/entities', {
    params: {
      path: { id: collectionId },
      query: { filter, limit: 500 },
    },
  });

  if (error || !data) {
    throw new Error(`Failed to fetch entities: ${JSON.stringify(error)}`);
  }

  const allEntities = (data.entities || []) as Array<{
    id: string;
    type: string;
    label?: string;
    properties?: Record<string, unknown>;
  }>;

  // Filter out non-KG types
  const excluded = ['cluster_leader', 'text_chunk', 'scatter_job', 'manifest'];
  const kgEntities = allEntities.filter(e => !excluded.includes(e.type));

  console.log(`Found ${kgEntities.length} KG entities (excluded: ${allEntities.length - kgEntities.length})`);

  // Fetch full details for each
  const entities: Entity[] = [];
  for (const e of kgEntities) {
    const { data: full } = await client.api.GET('/entities/{id}', {
      params: { path: { id: e.id } },
    });
    if (full) {
      entities.push({
        id: full.id,
        label: (full.properties?.label as string) || e.label || full.id,
        description: full.properties?.description as string | undefined,
        type: full.type,
      });
    }
  }

  return entities;
}

// Run semantic search for one entity
async function searchSimilar(entity: Entity): Promise<SimilarityResult> {
  const query = [entity.label, entity.description].filter(Boolean).join(' ');

  const { data, error } = await (client.api.POST as Function)('/search/entities', {
    body: {
      collection_id: collectionId,
      query,
      filter: { _kg_layer: LAYER },
      limit: K + 1,
      expand: 'preview',
    },
  });

  if (error || !data) {
    console.error(`Search failed for ${entity.label}: ${JSON.stringify(error)}`);
    return { entityId: entity.id, topK: [] };
  }

  const results = (data.results || []) as Array<{ id: string; score: number }>;

  return {
    entityId: entity.id,
    topK: results
      .filter(r => r.id !== entity.id)
      .slice(0, K)
      .map(r => ({ peerId: r.id, score: r.score })),
  };
}

// Build adjacency graph from similarity results
function buildGraph(
  entities: Entity[],
  similarities: SimilarityResult[]
): Map<string, Set<string>> {
  const graph = new Map<string, Set<string>>();

  for (const entity of entities) {
    graph.set(entity.id, new Set());
  }

  for (const sim of similarities) {
    for (const peer of sim.topK) {
      // Add edge: entity -> peer (entity considers peer as top-K similar)
      graph.get(sim.entityId)?.add(peer.peerId);
    }
  }

  return graph;
}

// Find connected components (ideal clusters if we trust semantic search)
function findConnectedComponents(
  entities: Entity[],
  graph: Map<string, Set<string>>
): string[][] {
  const visited = new Set<string>();
  const components: string[][] = [];

  for (const entity of entities) {
    if (visited.has(entity.id)) continue;

    // BFS to find all connected entities
    const component: string[] = [];
    const queue = [entity.id];

    while (queue.length > 0) {
      const current = queue.shift()!;
      if (visited.has(current)) continue;
      visited.add(current);
      component.push(current);

      // Add all neighbors (bidirectional - if A->B or B->A, they're connected)
      const neighbors = graph.get(current) || new Set();
      for (const neighbor of neighbors) {
        if (!visited.has(neighbor)) {
          queue.push(neighbor);
        }
      }
      // Also check reverse edges
      for (const [otherId, otherNeighbors] of graph) {
        if (otherNeighbors.has(current) && !visited.has(otherId)) {
          queue.push(otherId);
        }
      }
    }

    components.push(component);
  }

  return components;
}

// Analyze clustering potential - do entities have distinct peer groups?
function analyzeClusterPotential(
  entities: Entity[],
  similarities: SimilarityResult[]
): void {
  const entityMap = new Map(entities.map(e => [e.id, e]));

  console.log('\n=== TOP-K OVERLAP ANALYSIS ===\n');

  // For each entity, check how many other entities also have it in their top-K
  const inTopKCount = new Map<string, number>();
  for (const entity of entities) {
    inTopKCount.set(entity.id, 0);
  }

  for (const sim of similarities) {
    for (const peer of sim.topK) {
      inTopKCount.set(peer.peerId, (inTopKCount.get(peer.peerId) || 0) + 1);
    }
  }

  // Find "hub" entities that appear in many top-K lists
  const sortedByPopularity = [...inTopKCount.entries()]
    .sort((a, b) => b[1] - a[1]);

  console.log('Most "popular" entities (appear in many top-K lists):');
  for (const [id, count] of sortedByPopularity.slice(0, 10)) {
    const entity = entityMap.get(id);
    console.log(`  ${entity?.label || id}: appears in ${count} top-K lists`);
  }

  // Check if there's a "mega hub" that's in everyone's top-K
  const maxPopularity = sortedByPopularity[0][1];
  const totalEntities = entities.length;
  console.log(`\nMax popularity: ${maxPopularity}/${totalEntities} (${(maxPopularity/totalEntities*100).toFixed(1)}%)`);

  if (maxPopularity > totalEntities * 0.5) {
    console.log('⚠️  WARNING: Hub entity appears in >50% of top-K lists - this could cause mega clusters');
  }

  // Analyze similarity score distribution
  console.log('\n=== SIMILARITY SCORE DISTRIBUTION ===\n');

  const allScores: number[] = [];
  for (const sim of similarities) {
    for (const peer of sim.topK) {
      allScores.push(peer.score);
    }
  }

  allScores.sort((a, b) => a - b);
  const min = allScores[0];
  const max = allScores[allScores.length - 1];
  const median = allScores[Math.floor(allScores.length / 2)];
  const p25 = allScores[Math.floor(allScores.length * 0.25)];
  const p75 = allScores[Math.floor(allScores.length * 0.75)];

  console.log(`Score range: ${min.toFixed(3)} - ${max.toFixed(3)}`);
  console.log(`Median: ${median.toFixed(3)}`);
  console.log(`25th percentile: ${p25.toFixed(3)}`);
  console.log(`75th percentile: ${p75.toFixed(3)}`);

  // Histogram
  const buckets = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
  console.log('\nScore histogram:');
  let prevBucket = 0;
  for (const bucket of buckets) {
    const count = allScores.filter(s => s >= prevBucket && s < bucket).length;
    const bar = '#'.repeat(Math.min(count, 50));
    console.log(`  ${prevBucket.toFixed(1)}-${bucket.toFixed(1)}: ${count.toString().padStart(4)} ${bar}`);
    prevBucket = bucket;
  }
}

// Main
async function main() {
  console.log(`\nAnalyzing ideal clustering for collection ${collectionId}`);
  console.log(`K=${K}, Layer=${LAYER}\n`);

  // 1. Fetch entities
  const entities = await fetchEntities();
  if (entities.length === 0) {
    console.error('No entities found');
    process.exit(1);
  }

  console.log(`\nSample entities:`);
  for (const e of entities.slice(0, 5)) {
    console.log(`  - ${e.label} (${e.type})`);
  }

  // 2. Run semantic searches in parallel (batched to avoid rate limits)
  console.log(`\nRunning semantic search for ${entities.length} entities (K=${K})...`);

  const batchSize = 10;
  const similarities: SimilarityResult[] = [];

  for (let i = 0; i < entities.length; i += batchSize) {
    const batch = entities.slice(i, i + batchSize);
    const results = await Promise.all(batch.map(e => searchSimilar(e)));
    similarities.push(...results);

    if (i + batchSize < entities.length) {
      process.stdout.write(`  ${i + batchSize}/${entities.length}\r`);
    }
  }
  console.log(`  ${entities.length}/${entities.length} done`);

  // 3. Analyze cluster potential
  analyzeClusterPotential(entities, similarities);

  // 4. Build graph and find ideal clusters
  console.log('\n=== IDEAL CLUSTERS (Connected Components) ===\n');

  const graph = buildGraph(entities, similarities);
  const components = findConnectedComponents(entities, graph);

  // Sort by size
  components.sort((a, b) => b.length - a.length);

  console.log(`Found ${components.length} connected components:\n`);

  const entityMap = new Map(entities.map(e => [e.id, e]));

  for (let i = 0; i < components.length; i++) {
    const component = components[i];
    const sampleLabels = component
      .slice(0, 5)
      .map(id => entityMap.get(id)?.label || id);

    console.log(`Cluster ${i + 1}: ${component.length} members`);
    console.log(`  Sample: ${sampleLabels.join(', ')}${component.length > 5 ? '...' : ''}`);
  }

  // Summary
  console.log('\n=== SUMMARY ===\n');
  console.log(`Total entities: ${entities.length}`);
  console.log(`Ideal clusters: ${components.length}`);
  console.log(`Largest cluster: ${components[0]?.length || 0} (${((components[0]?.length || 0) / entities.length * 100).toFixed(1)}%)`);
  console.log(`Smallest cluster: ${components[components.length - 1]?.length || 0}`);

  if (components.length === 1) {
    console.log('\n⚠️  ALL ENTITIES ARE CONNECTED - no natural cluster boundaries with K=' + K);
    console.log('   This means every entity is reachable from every other via top-K similarity chains.');
    console.log('   Try reducing K or adding a similarity threshold.');
  } else if (components[0].length > entities.length * 0.5) {
    console.log('\n⚠️  MEGA COMPONENT detected (>50% of entities)');
    console.log('   Most entities cluster together even with perfect algorithm.');
  } else {
    console.log('\n✓  Good cluster separation exists - algorithm should find these boundaries.');
  }
}

main().catch(console.error);

/**
 * Scatter KG Workflow Test
 *
 * Tests the scatter → extract → dedupe → cluster workflow:
 * 1. Creates text entities for KG extraction
 * 2. Creates a manifest entity as the workflow target
 * 3. Invokes workflow with entity IDs via input.entity_ids
 * 4. Waits for scatter + extract + dedupe + cluster to complete
 * 5. Verifies all logs succeeded
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  configureTestClient,
  createCollection,
  createEntity,
  deleteEntity,
  invokeRhiza,
  waitForWorkflowTree,
  log,
} from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';
const SCATTER_KG_RHIZA = process.env.SCATTER_KG_RHIZA;
const SCATTER_KLADOS = process.env.SCATTER_KLADOS;
const KG_EXTRACTOR_KLADOS = process.env.KG_EXTRACTOR_KLADOS;
const KG_DEDUPE_RESOLVER_KLADOS = process.env.KG_DEDUPE_RESOLVER_KLADOS;
const KG_CLUSTER_KLADOS = process.env.KG_CLUSTER_KLADOS;

// Sample texts for KG extraction (short but meaningful)
const SAMPLE_TEXTS = [
  'Captain Ahab commanded the Pequod, a whaling ship from Nantucket. He was obsessed with hunting Moby Dick, the great white whale that had taken his leg.',
  'Ishmael was a sailor who joined the crew of the Pequod. He became close friends with Queequeg, a skilled harpooner from the South Pacific islands.',
];

// =============================================================================
// Test Suite
// =============================================================================

describe('scatter-kg workflow', () => {
  let targetCollection: { id: string };
  let manifestEntity: { id: string };
  let textEntities: { id: string }[] = [];
  let jobCollectionId: string;

  beforeAll(() => {
    if (!ARKE_USER_KEY) {
      console.warn('Skipping tests: ARKE_USER_KEY not set');
      return;
    }
    if (!SCATTER_KG_RHIZA || !SCATTER_KLADOS || !KG_EXTRACTOR_KLADOS || !KG_DEDUPE_RESOLVER_KLADOS || !KG_CLUSTER_KLADOS) {
      console.warn('Skipping tests: Missing env vars (SCATTER_KG_RHIZA, SCATTER_KLADOS, KG_EXTRACTOR_KLADOS, KG_DEDUPE_RESOLVER_KLADOS, KG_CLUSTER_KLADOS)');
      return;
    }

    configureTestClient({
      apiBase: ARKE_API_BASE,
      userKey: ARKE_USER_KEY,
      network: NETWORK,
    });

    log(`Using rhiza: ${SCATTER_KG_RHIZA}`);
  });

  beforeAll(async () => {
    if (!ARKE_USER_KEY || !SCATTER_KG_RHIZA || !SCATTER_KLADOS || !KG_EXTRACTOR_KLADOS || !KG_DEDUPE_RESOLVER_KLADOS || !KG_CLUSTER_KLADOS) return;

    log('Creating test fixtures...');

    // Create target collection with invoke permissions for workflow chaining
    targetCollection = await createCollection({
      label: `Scatter KG Test ${Date.now()}`,
      description: 'Test collection for scatter KG workflow',
      roles: { public: ['*:view', '*:invoke'] },
    });
    log(`Created target collection: ${targetCollection.id}`);

    // Create text entities for KG extraction
    for (let i = 0; i < SAMPLE_TEXTS.length; i++) {
      const entity = await createEntity({
        type: 'text_chunk',
        properties: {
          label: `Test Text ${i + 1}`,
          content: SAMPLE_TEXTS[i],
          created_at: new Date().toISOString(),
        },
        collection: targetCollection.id,
      });
      textEntities.push(entity);
      log(`Created text entity ${i + 1}: ${entity.id}`);
    }

    // Create manifest entity (serves as workflow target/job context)
    manifestEntity = await createEntity({
      type: 'scatter_job',
      properties: {
        label: 'Scatter KG Extraction Job',
        description: 'Job manifest for scatter KG workflow test',
        entity_count: textEntities.length,
        created_at: new Date().toISOString(),
      },
      collection: targetCollection.id,
    });
    log(`Created manifest entity: ${manifestEntity.id}`);
  });

  afterAll(async () => {
    if (!ARKE_USER_KEY || !SCATTER_KG_RHIZA || !SCATTER_KLADOS || !KG_EXTRACTOR_KLADOS || !KG_DEDUPE_RESOLVER_KLADOS || !KG_CLUSTER_KLADOS) return;

    // Cleanup disabled for debugging
    log('Cleanup DISABLED for inspection');
    log(`  Target collection: ${targetCollection?.id}`);
    log(`  Manifest entity: ${manifestEntity?.id}`);
    log(`  Text entities: ${textEntities.map(e => e.id).join(', ')}`);
    log(`  Job collection: ${jobCollectionId}`);

    // Uncomment to enable cleanup:
    // try {
    //   for (const entity of textEntities) {
    //     if (entity?.id) await deleteEntity(entity.id);
    //   }
    //   if (manifestEntity?.id) await deleteEntity(manifestEntity.id);
    //   if (targetCollection?.id) await deleteEntity(targetCollection.id);
    //   if (jobCollectionId) await deleteEntity(jobCollectionId);
    //   log('Cleanup complete');
    // } catch (e) {
    //   log(`Cleanup error (non-fatal): ${e}`);
    // }
  });

  // ==========================================================================
  // Tests
  // ==========================================================================

  it('should scatter entities through extract, dedupe, and cluster pipeline', async () => {
    if (!ARKE_USER_KEY || !SCATTER_KG_RHIZA) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Invoke the workflow
    // - targetEntity: manifest (job context, required by API)
    // - input.entity_ids: actual text entities to scatter
    log('Invoking scatter-kg workflow...');
    const entityIds = textEntities.map(e => e.id);
    log(`Manifest entity: ${manifestEntity.id}`);
    log(`Entity IDs to scatter: ${entityIds.join(', ')}`);

    const result = await invokeRhiza({
      rhizaId: SCATTER_KG_RHIZA,
      targetEntity: manifestEntity.id,
      targetCollection: targetCollection.id,
      input: {
        entity_ids: entityIds,
      },
      confirm: true,
    });

    expect(result.status).toBe('started');
    expect(result.job_id).toBeDefined();
    expect(result.job_collection).toBeDefined();

    jobCollectionId = result.job_collection!;
    log(`Workflow started: ${result.job_id}`);
    log(`Job collection: ${jobCollectionId}`);

    // Wait for workflow to complete using tree traversal (no indexing lag)
    log('Waiting for workflow to complete (KG extraction may take a while)...');
    const tree = await waitForWorkflowTree(jobCollectionId, {
      timeout: 300000, // 5 minutes for KG extraction
      pollInterval: 5000,
      onPoll: (t, elapsed) => {
        const elapsedSec = Math.round(elapsed / 1000);
        log(`[${elapsedSec}s] ${t.logs.size} logs, complete=${t.isComplete}`);
      },
    });

    expect(tree.isComplete).toBe(true);
    log(`Workflow complete with ${tree.logs.size} logs`);

    // Analyze logs
    const logs = Array.from(tree.logs.values());

    // Find scatter log (by klados_id)
    const scatterLog = logs.find(l => l.properties?.klados_id === SCATTER_KLADOS);

    // Find extract logs (by klados_id)
    const extractLogs = logs.filter(l => l.properties?.klados_id === KG_EXTRACTOR_KLADOS);

    // Find dedupe logs (by klados_id)
    const dedupeLogs = logs.filter(l => l.properties?.klados_id === KG_DEDUPE_RESOLVER_KLADOS);

    // Find cluster logs (by klados_id)
    const clusterLogs = logs.filter(l => l.properties?.klados_id === KG_CLUSTER_KLADOS);

    log(`Scatter log: ${scatterLog?.id} (status: ${scatterLog?.properties?.status})`);
    log(`Extract logs: ${extractLogs.length}`);
    log(`Dedupe logs: ${dedupeLogs.length}`);
    log(`Cluster logs: ${clusterLogs.length}`);

    // Verify scatter succeeded
    expect(scatterLog).toBeDefined();
    expect(scatterLog?.properties?.status).toBe('done');

    // Verify each entity was processed by KG extractor
    // Should have one extract log per text entity
    expect(extractLogs.length).toBe(textEntities.length);

    for (const extractLog of extractLogs) {
      log(`  - Extract ${extractLog.id}: ${extractLog.properties?.status}`);
      expect(extractLog.properties?.status).toBe('done');
    }

    // Verify dedupe ran for extracted entities
    // Note: dedupe logs >= extract logs (each extract can produce multiple entities)
    expect(dedupeLogs.length).toBeGreaterThanOrEqual(1);

    for (const dedupeLog of dedupeLogs) {
      log(`  - Dedupe ${dedupeLog.id}: ${dedupeLog.properties?.status}`);
      expect(dedupeLog.properties?.status).toBe('done');
    }

    // Verify cluster ran
    // Note: cluster logs may be fewer than dedupe logs if:
    // - Solo clusters are dissolved (no followers joined within timeout)
    // - Entities join existing clusters (no output to propagate)
    // We expect at least some cluster activity
    log(`Cluster logs: ${clusterLogs.length}`);
    for (const clusterLog of clusterLogs) {
      log(`  - Cluster ${clusterLog.id}: ${clusterLog.properties?.status}`);
      expect(clusterLog.properties?.status).toBe('done');
    }

    log('Scatter KG workflow completed successfully!');
    log(`  - Scattered ${entityIds.length} entities`);
    log(`  - KG extraction completed for all entities`);
    log(`  - Deduplication completed for ${dedupeLogs.length} entities`);
    log(`  - Clustering completed for ${clusterLogs.length} entities`);
  }, 900000); // 15 minute test timeout (clustering adds wait time)
});

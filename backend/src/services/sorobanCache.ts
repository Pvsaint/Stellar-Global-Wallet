/**
 * Soroban read-result cache (#86).
 *
 * Each simulate() call is a full RPC round-trip (~100–500 ms). This thin
 * Redis layer caches the JSON-serialised return value keyed by
 * `contractId:method:base64(args)` with a 15-second TTL — short enough
 * that most on-chain state changes are visible within one ledger close
 * (~5 s) plus a comfortable margin, yet long enough to absorb repeated
 * identical reads inside a single user interaction.
 *
 * Invalidation: call `bustContractCache(contractId)` from any indexer or
 * event handler that detects a state-mutating event for the contract.  The
 * bust uses Redis SCAN + DEL so it never blocks the event loop.
 *
 * Cache-hit/miss counts are written to stdout so log-aggregation tools
 * (e.g. Datadog, CloudWatch) can track hit rate without any extra
 * instrumentation library.
 */

import { createClient, RedisClientType } from "redis";
import { config } from "../config";

const CACHE_TTL_SECONDS = 15;
const KEY_PREFIX = "orbit-wal:soroban-cache:";

type CacheClient = ReturnType<typeof createClient>;

let _client: CacheClient | undefined;

function getClient(): CacheClient {
  if (!_client) {
    _client = createClient({ url: config.REDIS_URL });
    (_client as unknown as RedisClientType).on("error", (err: Error) => {
      console.error("[soroban-cache] redis error", err.message);
    });
  }
  return _client;
}

async function ensureConnected(): Promise<CacheClient> {
  const client = getClient();
  if (!(client as unknown as RedisClientType).isOpen) {
    await client.connect();
  }
  return client;
}

function buildKey(contractId: string, method: string, args: string): string {
  return `${KEY_PREFIX}${contractId}:${method}:${args}`;
}

/**
 * Returns the cached result for a simulate() call, or null on a miss.
 * `argsKey` should be a stable, compact representation of the call's
 * XDR args (e.g. base64-encoded).
 */
export async function getCachedSimulate<T>(
  contractId: string,
  method: string,
  argsKey: string
): Promise<T | null> {
  try {
    const client = await ensureConnected();
    const raw = await client.get(buildKey(contractId, method, argsKey));
    if (raw === null) {
      console.log(`[soroban-cache] MISS ${contractId}.${method}`);
      return null;
    }
    console.log(`[soroban-cache] HIT  ${contractId}.${method}`);
    return JSON.parse(raw) as T;
  } catch (err) {
    // Cache failures must never break the hot path — log and fall through.
    console.error("[soroban-cache] getCachedSimulate error", (err as Error).message);
    return null;
  }
}

/**
 * Stores a simulate() result.  Failures are swallowed so a Redis outage
 * degrades gracefully to un-cached behaviour.
 */
export async function setCachedSimulate(
  contractId: string,
  method: string,
  argsKey: string,
  value: unknown
): Promise<void> {
  try {
    const client = await ensureConnected();
    await client.set(buildKey(contractId, method, argsKey), JSON.stringify(value), {
      EX: CACHE_TTL_SECONDS,
    });
  } catch (err) {
    console.error("[soroban-cache] setCachedSimulate error", (err as Error).message);
  }
}

/**
 * Invalidates all cached entries for a given contract.  Called by the
 * indexer/event handler when a state-mutating event is observed.
 *
 * Uses SCAN instead of KEYS to avoid blocking the Redis event loop on
 * large keyspaces.
 */
export async function bustContractCache(contractId: string): Promise<void> {
  try {
    const client = await ensureConnected();
    const pattern = `${KEY_PREFIX}${contractId}:*`;
    let cursor = 0;
    let busted = 0;
    do {
      // redis v4 SCAN returns { cursor, keys }
      const reply = await (client as unknown as RedisClientType).scan(cursor, {
        MATCH: pattern,
        COUNT: 100,
      });
      cursor = reply.cursor;
      if (reply.keys.length > 0) {
        await (client as unknown as RedisClientType).del(reply.keys);
        busted += reply.keys.length;
      }
    } while (cursor !== 0);
    console.log(`[soroban-cache] busted ${busted} key(s) for contract ${contractId}`);
  } catch (err) {
    console.error("[soroban-cache] bustContractCache error", (err as Error).message);
  }
}

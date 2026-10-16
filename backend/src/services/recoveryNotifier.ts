/**
 * Recovery & guardian event notification worker (#89).
 *
 * Security-critical: the owner MUST be alerted the moment a recovery is
 * initiated, regardless of any notification preferences.  This closes the
 * attack window described below.
 *
 * Threat model
 * ────────────
 * Attacker: a compromised or malicious guardian (or someone who has obtained
 *           guardian keys out-of-band).
 * What they can do today (without this fix): silently call `initiate_recovery`
 *   on the contract and collect guardian approvals without the owner knowing
 *   until funds have moved.
 * What this fix does: as soon as the indexer observes a `recovery.initiated`
 *   or guardian-approval event, it pushes an alert to every device registered
 *   for the owner account.  Owner alerts bypass the quiet-hours/preferences
 *   check intentionally — the issue explicitly states "Cannot be disabled".
 *
 * Events watched (via Soroban RPC getEvents, poll loop):
 *   topic[0] = "recovery", topic[1] = "initiated"  → owner alert + guardian alert
 *   topic[0] = "recovery", topic[1] = "approved"   → owner alert
 *   topic[0] = "guardian", topic[1] = "added"      → owner alert
 *   topic[0] = "guardian", topic[1] = "removed"    → owner alert
 *
 * Deduplication: event IDs from Soroban RPC are stored in Redis (SET NX,
 * 48-hour TTL) so retries never re-deliver.
 */

import { createClient } from "redis";
import { config } from "../config";
import { getDevicesForAccounts } from "./deviceRegistry";
import { sendPushToAll } from "./pushSender";

const DEDUP_KEY_PREFIX = "orbit-wal:notif-dedup:recovery:";
const DEDUP_TTL_SECONDS = 48 * 60 * 60;
const POLL_INTERVAL_MS = 10_000; // poll every 10 s
const RECONNECT_DELAY_MS = 15_000;

let dedupClient: ReturnType<typeof createClient> | undefined;

function getDedupClient() {
  if (!dedupClient) {
    dedupClient = createClient({ url: config.REDIS_URL });
    dedupClient.on("error", (err: Error) =>
      console.error("[recovery-notifier] redis error", err.message)
    );
  }
  return dedupClient;
}

async function ensureDedupConnected() {
  const c = getDedupClient();
  if (!c.isOpen) await c.connect();
  return c;
}

async function markSeen(eventId: string): Promise<boolean> {
  const client = await ensureDedupConnected();
  const key = `${DEDUP_KEY_PREFIX}${eventId}`;
  const result = await client.set(key, "1", { NX: true, EX: DEDUP_TTL_SECONDS });
  return result === "OK";
}

// ---------------------------------------------------------------------------
// Soroban event types (subset we care about)
// ---------------------------------------------------------------------------

interface SorobanEventValue {
  type: string;
  value: unknown;
}

interface SorobanEvent {
  id: string;
  contractId: string;
  topic: SorobanEventValue[];
  value: SorobanEventValue;
  ledger: number;
}

interface SorobanGetEventsResponse {
  events: SorobanEvent[];
  latestLedger: number;
}

/**
 * Starts the recovery-event notification worker.  Never returns — runs for
 * the lifetime of the process.  Call once from index.ts after startup.
 */
export async function startRecoveryNotifier(): Promise<void> {
  if (!config.GLOBE_WALLET_CONTRACT_ID) {
    console.warn(
      "[recovery-notifier] GLOBE_WALLET_CONTRACT_ID not set, worker disabled"
    );
    return;
  }

  console.log("[recovery-notifier] starting for contract", config.GLOBE_WALLET_CONTRACT_ID);

  let startLedger = 0; // 0 = latest; will be updated after each poll

  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      const result = await pollEvents(startLedger);
      if (result !== null) {
        startLedger = result.latestLedger;
        for (const event of result.events) {
          await processEvent(event);
        }
      }
    } catch (err) {
      console.error(
        "[recovery-notifier] poll error, retrying in",
        RECONNECT_DELAY_MS,
        "ms",
        (err as Error).message
      );
      await sleep(RECONNECT_DELAY_MS);
      continue;
    }
    await sleep(POLL_INTERVAL_MS);
  }
}

async function pollEvents(
  startLedger: number
): Promise<SorobanGetEventsResponse | null> {
  if (!config.GLOBE_WALLET_CONTRACT_ID) return null;

  const body = JSON.stringify({
    jsonrpc: "2.0",
    id: 1,
    method: "getEvents",
    params: {
      startLedger: startLedger || undefined,
      filters: [
        {
          type: "contract",
          contractIds: [config.GLOBE_WALLET_CONTRACT_ID],
          topics: [
            // recovery.initiated, recovery.approved
            ["recovery", "*"],
            // guardian.added, guardian.removed
            ["guardian", "*"],
          ],
        },
      ],
      pagination: { limit: 100 },
    },
  });

  const { default: https } = await import("https");
  const rpcUrl = new URL(config.SOROBAN_RPC_URL);

  const raw: string = await new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    const req = https.request(
      {
        hostname: rpcUrl.hostname,
        port: rpcUrl.port || 443,
        path: rpcUrl.pathname,
        method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) },
      },
      (res) => {
        res.on("data", (d: Buffer) => chunks.push(d));
        res.on("end", () => resolve(Buffer.concat(chunks).toString()));
        res.on("error", reject);
      }
    );
    req.on("error", reject);
    req.write(body);
    req.end();
  });

  const parsed = JSON.parse(raw) as {
    result?: { events: SorobanEvent[]; latestLedger: number };
    error?: { message: string };
  };

  if (parsed.error) {
    throw new Error(`Soroban RPC error: ${parsed.error.message}`);
  }

  return parsed.result ?? null;
}

async function processEvent(event: SorobanEvent): Promise<void> {
  const isNew = await markSeen(event.id);
  if (!isNew) return;

  const topic0 = extractTopicString(event.topic[0]);
  const topic1 = extractTopicString(event.topic[1]);

  if (!topic0 || !topic1) return;

  // Extract the owner address from the event value (contract emits the
  // owner/user address as the event body).
  const ownerAddress = extractAddress(event.value);
  if (!ownerAddress) return;

  const isRecoveryInitiated = topic0 === "recovery" && topic1 === "initiated";
  const isRecoveryApproved = topic0 === "recovery" && topic1 === "approved";
  const isGuardianEvent =
    topic0 === "guardian" && (topic1 === "added" || topic1 === "removed");

  if (!isRecoveryInitiated && !isRecoveryApproved && !isGuardianEvent) return;

  // Owner notification — ALWAYS sent, cannot be disabled (#89 requirement).
  const ownerDevices = await getDevicesForAccounts([ownerAddress]);
  if (ownerDevices.length > 0) {
    const msg = buildOwnerMessage(topic0, topic1);
    await sendPushToAll(ownerDevices, {
      title: msg.title,
      body: msg.body,
      data: { eventId: event.id, contractId: event.contractId, topic0, topic1 },
    });
    console.log(
      `[recovery-notifier] owner alert (${topic0}.${topic1}) sent to ${ownerDevices.length} device(s) for ${ownerAddress}`
    );
  }

  // Guardian notification — only on initiation (they need to approve).
  if (isRecoveryInitiated) {
    await notifyGuardians(event, ownerAddress);
  }
}

async function notifyGuardians(event: SorobanEvent, ownerAddress: string): Promise<void> {
  // The guardian addresses are embedded in the contract event.  In the
  // globe-wallet contract, `recovery.initiated` emits a Vec<Address> of
  // the guardian list as part of the event value.  We extract whatever
  // addresses are present in the value; if the structure differs from
  // expectations we log and skip rather than crash.
  const guardianAddresses = extractGuardianAddresses(event.value);

  if (guardianAddresses.length === 0) {
    console.warn(
      "[recovery-notifier] could not extract guardian addresses from event",
      event.id
    );
    return;
  }

  // Exclude the owner from the guardian list (should not happen but be safe).
  const otherGuardians = guardianAddresses.filter((g) => g !== ownerAddress);
  if (otherGuardians.length === 0) return;

  const devices = await getDevicesForAccounts(otherGuardians);
  if (devices.length === 0) return;

  await sendPushToAll(devices, {
    title: "Recovery approval needed",
    body: "A wallet recovery has been initiated and requires your approval.",
    data: { eventId: event.id, contractId: event.contractId, ownerAddress },
  });
  console.log(
    `[recovery-notifier] guardian approval request sent to ${devices.length} device(s)`
  );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildOwnerMessage(
  topic0: string,
  topic1: string
): { title: string; body: string } {
  if (topic0 === "recovery" && topic1 === "initiated") {
    return {
      title: "⚠️ Recovery initiated on your wallet",
      body: "Someone has started a recovery process on your account. If this was not you, contact support immediately.",
    };
  }
  if (topic0 === "recovery" && topic1 === "approved") {
    return {
      title: "Recovery approval received",
      body: "A guardian has approved your wallet recovery.",
    };
  }
  if (topic0 === "guardian" && topic1 === "added") {
    return {
      title: "New guardian added",
      body: "A new guardian has been added to your wallet.",
    };
  }
  if (topic0 === "guardian" && topic1 === "removed") {
    return {
      title: "Guardian removed",
      body: "A guardian has been removed from your wallet.",
    };
  }
  return { title: "Wallet security event", body: `${topic0}.${topic1}` };
}

function extractTopicString(val: SorobanEventValue | undefined): string | null {
  if (!val) return null;
  // The stellar-sdk decodes symbol ScVals as plain strings.
  if (typeof val === "string") return val;
  if (val.type === "symbol" || val.type === "string") return String(val.value);
  return null;
}

function extractAddress(val: SorobanEventValue): string | null {
  if (!val) return null;
  if (typeof val.value === "string" && val.value.startsWith("G")) return val.value;
  // Address may be nested under a .value key.
  if (
    typeof val.value === "object" &&
    val.value !== null &&
    "address" in (val.value as object)
  ) {
    const addr = (val.value as { address: unknown }).address;
    if (typeof addr === "string" && addr.startsWith("G")) return addr;
  }
  return null;
}

function extractGuardianAddresses(val: SorobanEventValue): string[] {
  if (!val || !val.value) return [];
  // If the value is an array of addresses.
  if (Array.isArray(val.value)) {
    return (val.value as unknown[])
      .map((v) => (typeof v === "string" ? v : null))
      .filter((v): v is string => v !== null && v.startsWith("G"));
  }
  return [];
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

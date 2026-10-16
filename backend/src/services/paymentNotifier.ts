/**
 * Incoming-payment notification worker (#88).
 *
 * Subscribes to the Stellar Horizon payment stream for every account that
 * has at least one registered device.  When a payment lands above the
 * configured dust threshold it dispatches a push notification to all of
 * that account's devices.
 *
 * Deduplication: Horizon operation IDs are stored in Redis with a 48-hour
 * TTL.  A second delivery attempt for the same operation ID is a no-op,
 * which prevents double-notifications on stream reconnects.
 *
 * Dust/spam filtering: Payments whose `amount` (converted to float) is
 * below `config.SPAM_DUST_THRESHOLD` are silently dropped (see #105 /
 * paymentFlags.ts for the same threshold).
 *
 * This worker is started once at server boot from index.ts and runs for
 * the lifetime of the process.
 */

import { createClient } from "redis";
import { config } from "../config";
import { pool } from "../db";
import { getDevicesForAccounts } from "./deviceRegistry";
import { sendPushToAll } from "./pushSender";

const DEDUP_KEY_PREFIX = "orbit-wal:notif-dedup:payment:";
const DEDUP_TTL_SECONDS = 48 * 60 * 60; // 48 h
// Horizon streams take cursor='now' to receive only future events.
const STREAM_CURSOR = "now";
// Reconnect delay on stream error (ms).
const RECONNECT_DELAY_MS = 5_000;

let dedupClient: ReturnType<typeof createClient> | undefined;

function getDedupClient() {
  if (!dedupClient) {
    dedupClient = createClient({ url: config.REDIS_URL });
    dedupClient.on("error", (err: Error) =>
      console.error("[payment-notifier] redis error", err.message)
    );
  }
  return dedupClient;
}

async function ensureDedupConnected() {
  const client = getDedupClient();
  if (!client.isOpen) await client.connect();
  return client;
}

/** Returns true the first time `id` is seen, false on duplicates. */
async function markSeen(operationId: string): Promise<boolean> {
  const client = await ensureDedupConnected();
  const key = `${DEDUP_KEY_PREFIX}${operationId}`;
  // SET NX returns "OK" when the key was inserted, null when it already existed.
  const result = await client.set(key, "1", { NX: true, EX: DEDUP_TTL_SECONDS });
  return result === "OK";
}

/**
 * Starts the payment-notification worker.  This function never returns
 * (it loops forever, reconnecting on errors).  Call it once at startup
 * from index.ts.
 */
export async function startPaymentNotifier(): Promise<void> {
  console.log("[payment-notifier] starting");
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      await runStream();
    } catch (err) {
      console.error(
        "[payment-notifier] stream error, reconnecting in",
        RECONNECT_DELAY_MS,
        "ms",
        (err as Error).message
      );
      await sleep(RECONNECT_DELAY_MS);
    }
  }
}

async function runStream(): Promise<void> {
  // Fetch all accounts that currently have at least one registered device.
  const { rows } = await pool.query<{ account: string }>(
    `SELECT DISTINCT account FROM devices`
  );
  const accounts = rows.map((r) => r.account);
  if (accounts.length === 0) {
    // No registered devices; wait and retry.
    await sleep(30_000);
    return;
  }

  console.log(`[payment-notifier] watching ${accounts.length} account(s)`);

  // Subscribe to each account's payment stream.  Horizon payment streams
  // send SSE; we use the stellar-sdk's built-in EventSource-based streaming
  // which returns a close() handle.
  const { default: StellarSdk } = await import("@stellar/stellar-sdk");
  const server = new StellarSdk.Horizon.Server(config.HORIZON_URL);

  const closers: Array<() => void> = [];

  await Promise.all(
    accounts.map(
      (account) =>
        new Promise<void>((resolve, reject) => {
          const close = server
            .payments()
            .forAccount(account)
            .cursor(STREAM_CURSOR)
            .stream({
              onmessage: (record) => {
                void handlePayment(account, record).catch((err: Error) => {
                  console.error("[payment-notifier] handlePayment error", err.message);
                });
              },
              onerror: (err: unknown) => {
                reject(err);
              },
            }) as unknown as () => void;
          closers.push(close);
          // resolve() immediately; the stream runs asynchronously.
          resolve();
        })
    )
  );

  // Keep the worker alive indefinitely.  Re-check for new accounts every 5 min.
  await sleep(5 * 60 * 1000);
  closers.forEach((c) => c());
}

interface HorizonPaymentRecord {
  id: string;
  type: string;
  to?: string;
  amount?: string;
  asset_type?: string;
  asset_code?: string;
}

async function handlePayment(account: string, record: unknown): Promise<void> {
  const payment = record as HorizonPaymentRecord;

  // Only notify on received payments (to == account).
  if (payment.to !== account) return;
  // Dust filter.
  const amount = parseFloat(payment.amount ?? "0");
  if (amount < config.SPAM_DUST_THRESHOLD) return;

  // Deduplication.
  const isNew = await markSeen(payment.id);
  if (!isNew) {
    console.log(`[payment-notifier] duplicate operation ${payment.id}, skipping`);
    return;
  }

  const devices = await getDevicesForAccounts([account]);
  if (devices.length === 0) return;

  const assetLabel =
    payment.asset_type === "native"
      ? "XLM"
      : (payment.asset_code ?? "asset");

  await sendPushToAll(devices, {
    title: "Payment received",
    body: `You received ${amount} ${assetLabel}`,
    data: { operationId: payment.id, account, amount, asset: assetLabel },
  });

  console.log(
    `[payment-notifier] notified ${devices.length} device(s) for account ${account} op ${payment.id}`
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

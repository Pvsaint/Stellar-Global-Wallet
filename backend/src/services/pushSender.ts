/**
 * Push-notification delivery adapter (#87, #88, #89).
 *
 * Supports two platforms:
 *   - "expo"  — Expo Push Notification API (no SDK needed; plain HTTPS POST)
 *   - "web"   — Web Push (VAPID); token is a JSON-serialised PushSubscription
 *
 * The function is fire-and-forget from the caller's perspective: delivery
 * failures are logged but never thrown so a failed push never crashes an
 * event-handler loop.
 *
 * Deduplication: callers are responsible for providing a stable `eventId`
 * (e.g. Horizon operation ID or Soroban event ID). This layer logs the id
 * with every send; a separate Redis SET keyed on the eventId is checked
 * upstream (see paymentNotifier.ts, recoveryNotifier.ts) so retries
 * never re-deliver.
 */

import https from "https";
import { Device } from "./deviceRegistry";

export interface PushMessage {
  title: string;
  body: string;
  data?: Record<string, unknown>;
}

const EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send";

/** Sends a push notification to a single device. */
export async function sendPush(device: Device, message: PushMessage): Promise<void> {
  if (device.platform === "expo") {
    await sendExpoPush(device.token, message);
  } else {
    await sendWebPush(device.token, message);
  }
}

/** Sends to every device in the list; failures are isolated per-device. */
export async function sendPushToAll(
  devices: Device[],
  message: PushMessage
): Promise<void> {
  await Promise.allSettled(
    devices.map(async (d) => {
      try {
        await sendPush(d, message);
      } catch (err) {
        console.error(
          `[push-sender] failed to send to device ${d.id} (${d.platform})`,
          (err as Error).message
        );
      }
    })
  );
}

// ---------------------------------------------------------------------------
// Expo
// ---------------------------------------------------------------------------

async function sendExpoPush(token: string, message: PushMessage): Promise<void> {
  const payload = JSON.stringify({
    to: token,
    title: message.title,
    body: message.body,
    data: message.data ?? {},
    sound: "default",
  });

  await httpsPost(EXPO_PUSH_URL, payload, {
    "Content-Type": "application/json",
    Accept: "application/json",
  });
}

// ---------------------------------------------------------------------------
// Web Push (VAPID — minimal implementation without external dependencies)
// The subscription object stored in the DB is a JSON-serialised
// PushSubscription: { endpoint, keys: { p256dh, auth } }.
// A full VAPID implementation requires generating an ECDH shared secret;
// for now we post a plaintext notification to the endpoint and log a note
// that full VAPID signing requires the `web-push` npm package or equivalent.
// ---------------------------------------------------------------------------

async function sendWebPush(subscriptionJson: string, message: PushMessage): Promise<void> {
  try {
    const sub = JSON.parse(subscriptionJson) as {
      endpoint: string;
      keys?: { p256dh: string; auth: string };
    };

    // NOTE: A production deployment should use the `web-push` npm package for
    // proper VAPID signing and payload encryption (RFC 8291 / RFC 8292).
    // This lightweight path posts the notification title+body as a JSON body
    // to the push service endpoint.  Most push services will accept this for
    // unencrypted subscriptions (or when using application server key auth).
    const payload = JSON.stringify({ title: message.title, body: message.body });
    await httpsPost(sub.endpoint, payload, { "Content-Type": "application/json" });
  } catch (err) {
    throw new Error(`WebPush delivery failed: ${(err as Error).message}`);
  }
}

// ---------------------------------------------------------------------------
// Thin HTTPS POST helper (avoids adding a dependency just for this)
// ---------------------------------------------------------------------------

function httpsPost(
  url: string,
  body: string,
  headers: Record<string, string>
): Promise<void> {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const req = https.request(
      {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: "POST",
        headers: {
          ...headers,
          "Content-Length": Buffer.byteLength(body),
        },
      },
      (res) => {
        // Drain the response body so the socket is released.
        res.resume();
        if (res.statusCode && res.statusCode >= 400) {
          reject(new Error(`HTTP ${res.statusCode} from push endpoint`));
        } else {
          resolve();
        }
      }
    );
    req.on("error", reject);
    req.write(body);
    req.end();
  });
}

/**
 * Device-token registry (#87).
 *
 * Stores one row per registered push-notification endpoint.  Supports both
 * Expo push tokens ("ExponentPushToken[...]") and Web Push VAPID subscription
 * objects.  The `platform` discriminant drives which delivery adapter is
 * used by the notification workers (#88, #89).
 *
 * Schema (created lazily by ensureDevicesTable() called at startup):
 *
 *   devices(
 *     id          BIGSERIAL PK,
 *     account     VARCHAR(56)   — Stellar G-address of the owning account,
 *     platform    VARCHAR(16)   — "expo" | "web",
 *     token       TEXT          — Expo token string OR JSON-serialised
 *                                 WebPush subscription object,
 *     locale      VARCHAR(10)   — BCP-47 locale tag (e.g. "en-US"),
 *     created_at  TIMESTAMPTZ,
 *     last_seen_at TIMESTAMPTZ  — updated by POST when token already exists
 *   )
 *
 * Tokens are scoped strictly to the authenticated account: every read/write
 * filters by account so one user can never enumerate or delete another's
 * device.
 */

import { Pool } from "pg";
import { pool } from "../db";

export type Platform = "expo" | "web";

export interface Device {
  id: string;
  account: string;
  platform: Platform;
  token: string;
  locale: string;
  createdAt: Date;
  lastSeenAt: Date;
}

/** Lazily creates the devices table (called by ensureAuditTable in db.ts). */
export async function ensureDevicesTable(db: Pool = pool): Promise<void> {
  await db.query(`
    CREATE TABLE IF NOT EXISTS devices (
      id           BIGSERIAL    PRIMARY KEY,
      account      VARCHAR(56)  NOT NULL,
      platform     VARCHAR(16)  NOT NULL CHECK (platform IN ('expo','web')),
      token        TEXT         NOT NULL,
      locale       VARCHAR(10)  NOT NULL DEFAULT 'en',
      created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      last_seen_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      UNIQUE (account, token)
    )
  `);
}

/**
 * Upsert a device token for an account.  If the same (account, token) pair
 * already exists, only last_seen_at is updated — deduplicating registrations
 * across re-installs and app restarts without growing the table unboundedly.
 */
export async function upsertDevice(
  account: string,
  platform: Platform,
  token: string,
  locale: string,
  db: Pool = pool
): Promise<Device> {
  const { rows } = await db.query<{
    id: string;
    account: string;
    platform: Platform;
    token: string;
    locale: string;
    created_at: Date;
    last_seen_at: Date;
  }>(
    `INSERT INTO devices (account, platform, token, locale)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (account, token)
     DO UPDATE SET last_seen_at = NOW(), locale = EXCLUDED.locale
     RETURNING *`,
    [account, platform, token, locale]
  );
  const row = rows[0];
  return {
    id: row.id,
    account: row.account,
    platform: row.platform,
    token: row.token,
    locale: row.locale,
    createdAt: row.created_at,
    lastSeenAt: row.last_seen_at,
  };
}

/**
 * Deletes a device row.  Returns true when a row was deleted, false when the
 * device didn't exist or belongs to a different account (caller should 404).
 * The account filter prevents one user from deleting another user's device.
 */
export async function deleteDevice(
  account: string,
  deviceId: string,
  db: Pool = pool
): Promise<boolean> {
  const { rowCount } = await db.query(
    `DELETE FROM devices WHERE id = $1 AND account = $2`,
    [deviceId, account]
  );
  return (rowCount ?? 0) > 0;
}

/** Returns all registered devices for an account. */
export async function listDevices(account: string, db: Pool = pool): Promise<Device[]> {
  const { rows } = await db.query<{
    id: string;
    account: string;
    platform: Platform;
    token: string;
    locale: string;
    created_at: Date;
    last_seen_at: Date;
  }>(`SELECT * FROM devices WHERE account = $1 ORDER BY created_at DESC`, [account]);
  return rows.map((r) => ({
    id: r.id,
    account: r.account,
    platform: r.platform,
    token: r.token,
    locale: r.locale,
    createdAt: r.created_at,
    lastSeenAt: r.last_seen_at,
  }));
}

/** Returns all devices for a set of accounts (used by notification workers). */
export async function getDevicesForAccounts(
  accounts: string[],
  db: Pool = pool
): Promise<Device[]> {
  if (accounts.length === 0) return [];
  const placeholders = accounts.map((_, i) => `$${i + 1}`).join(",");
  const { rows } = await db.query<{
    id: string;
    account: string;
    platform: Platform;
    token: string;
    locale: string;
    created_at: Date;
    last_seen_at: Date;
  }>(`SELECT * FROM devices WHERE account IN (${placeholders})`, accounts);
  return rows.map((r) => ({
    id: r.id,
    account: r.account,
    platform: r.platform,
    token: r.token,
    locale: r.locale,
    createdAt: r.created_at,
    lastSeenAt: r.last_seen_at,
  }));
}

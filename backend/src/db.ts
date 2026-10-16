import { Pool } from "pg";
import { config } from "./config";
import { ensureDevicesTable } from "./services/deviceRegistry";

export const pool = new Pool({ connectionString: config.DATABASE_URL });

export async function ensureAuditTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS keypair_audit_log (
      id BIGSERIAL PRIMARY KEY,
      issued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      api_key_hash VARCHAR(64) NOT NULL,
      public_key VARCHAR(56) NOT NULL
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS idempotency_keys (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      source_public_key VARCHAR(56) NOT NULL,
      idempotency_key VARCHAR(255) NOT NULL,
      result_hash VARCHAR(64) NOT NULL,
      result_successful BOOLEAN NOT NULL,
      UNIQUE (source_public_key, idempotency_key)
    )
  `);

  // #87 — device token table for push notifications.
  await ensureDevicesTable(pool);
}

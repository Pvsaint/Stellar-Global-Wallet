import "dotenv/config";
import { config } from "./config";
import { createApp } from "./app";
import { ensureAuditTable } from "./db";
import { StellarService } from "./services/stellar";
import { createAccountLock } from "./services/locks/createAccountLock";
import { startPaymentNotifier } from "./services/paymentNotifier";
import { startRecoveryNotifier } from "./services/recoveryNotifier";

async function main() {
  const { lock, shutdown } = await createAccountLock();
  const stellar = new StellarService(lock);
  const app = createApp(stellar);

  await ensureAuditTable();

  const server = app.listen(config.PORT, () => {
    console.log(
      `GlobeWallet API running on port ${config.PORT} (lock backend: ${config.LOCK_BACKEND})`
    );
  });

  // #88 — incoming-payment push notifications (fire-and-forget worker).
  void startPaymentNotifier().catch((err: Error) => {
    console.error("[payment-notifier] fatal error", err.message);
  });

  // #89 — recovery/guardian event push notifications (fire-and-forget worker).
  void startRecoveryNotifier().catch((err: Error) => {
    console.error("[recovery-notifier] fatal error", err.message);
  });

  const shutdownGracefully = async (signal: string) => {
    console.log(`${signal} received, shutting down`);
    server.close(async () => {
      await shutdown();
      process.exit(0);
    });
  };

  process.on("SIGTERM", () => void shutdownGracefully("SIGTERM"));
  process.on("SIGINT", () => void shutdownGracefully("SIGINT"));
}

main().catch((err) => {
  console.error("Failed to start GlobeWallet API", err);
  process.exit(1);
});

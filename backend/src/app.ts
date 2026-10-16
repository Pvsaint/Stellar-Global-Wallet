import express from "express";
import helmet from "helmet";
import cors from "cors";
import morgan from "morgan";
import { rateLimit } from "express-rate-limit";
import { createWalletRouter } from "./routes/wallet";
import { createAccountRouter } from "./routes/account";
import { createContractRouter } from "./routes/contract";
import { priceRouter } from "./routes/price";
import { authRouter } from "./routes/auth";
import { cspReportRouter } from "./routes/cspReport";
import { flagsRouter } from "./routes/flags";
import { devicesRouter } from "./routes/devices";
import { errorHandler } from "./middleware/errorHandler";
import { config } from "./config";
import { StellarService } from "./services/stellar";
import { SorobanService } from "./services/soroban";
import { GlobeWalletContract } from "./services/contracts/globeWallet";
import { NotFoundError } from "./errors/appError";

export function createApp(
  stellar: StellarService = new StellarService(),
  globeWallet: GlobeWalletContract = new GlobeWalletContract(new SorobanService())
) {
  const app = express();

  // Must be set before any middleware that reads req.ip (rate limiter
  // below, and anything else added later) — see issue #90. The hop count
  // is configured via TRUST_PROXY_HOPS (config.ts) rather than hardcoded
  // `true`, since trusting the entire X-Forwarded-For chain unconditionally
  // lets a client spoof their apparent IP if there's more than one real
  // proxy hop between them and this process.
  app.set("trust proxy", config.TRUST_PROXY_HOPS);

  // helmet()'s defaults are tuned for a server rendering HTML (CSP that
  // still allows inline styles/scripts via nonces, X-Frame-Options, a
  // same-origin resource policy meant to protect pages with embeddable
  // assets). This API never serves HTML or embeddable assets — every
  // response is JSON — so the HTML-oriented defaults are either useless or
  // actively wrong here. Configure it explicitly for an API-only surface
  // instead of relying on defaults that were never written with this case
  // in mind (issue #119):
  //   - `default-src 'none'` / `frame-ancestors 'none'`: nothing on this
  //     origin should ever execute script or be framed — there's no HTML
  //     to inject into, and no page here should be embeddable.
  //   - `cross-origin-resource-policy: same-site`: this API is read by
  //     browser-based clients (web/mobile apps on other origins, once
  //     built — see root README), so the default `same-origin` would block
  //     them from even reading responses; `same-site` keeps third-party
  //     sites that aren't part of this project from loading API responses
  //     via <script>/<img> while still allowing the project's own apps.
  app.use(
    helmet({
      contentSecurityPolicy: {
        directives: {
          defaultSrc: ["'none'"],
          frameAncestors: ["'none'"],
        },
      },
      crossOriginResourcePolicy: { policy: "same-site" },
    })
  );
  app.use(cors({ origin: config.CORS_ORIGIN.split(",") }));
  app.use(morgan("combined"));

  // A multi-signed transaction envelope (several signatures on one XDR
  // blob) or a fee-bump wrapping one can comfortably exceed a few KB, and
  // the default 10kb limit below would reject a legitimate request with a
  // generic 413 before it ever reaches validation. These two routes are
  // the ones that actually accept an XDR envelope as input (`fee-bump`'s
  // `transactionXdr`, `submit-multisig`'s `xdr` + its signer key array),
  // so they get a larger, still-bounded limit; everything else keeps the
  // tighter default. Registering a path-specific `express.json()` before
  // the general one works because body-parser marks the body parsed once
  // one middleware has consumed the stream — the later, broader
  // `express.json` for every other route is then a no-op for these paths.
  app.use("/api/v1/wallet/fee-bump", express.json({ limit: "64kb" }));
  app.use("/api/v1/wallet/submit-multisig", express.json({ limit: "64kb" }));
  app.use(express.json({ limit: "10kb" }));

  app.use(
    rateLimit({
      windowMs: 15 * 60 * 1000, // 15 min
      max: 100,
      standardHeaders: true,
      legacyHeaders: false,
    })
  );

  // Health
  app.get("/health", (_req, res) => res.json({ status: "ok" }));

  // Unversioned and unauthenticated: this is where a browser enforcing
  // the Content-Security-Policy documented in docs/security/csp.md sends
  // violation reports, not something our own API clients call, so it
  // doesn't live under /api/v1 alongside the versioned wallet/account/etc.
  // routes (see issue: "Add Content-Security-Policy guidance for the
  // future web app").
  app.use("/csp-report", cspReportRouter);

  // Routes
  app.use("/api/v1/wallet", createWalletRouter(stellar));
  app.use("/api/v1/account", createAccountRouter(stellar));
  app.use("/api/v1/contract", createContractRouter(globeWallet));
  app.use("/api/v1/price", priceRouter);
  app.use("/api/v1/auth", authRouter);
  app.use("/api/v1/flags", flagsRouter);
  // #87 — push notification device registration
  app.use("/api/v1/me/devices", devicesRouter);

  app.use((req, _res, next) => {
    next(
      new NotFoundError(
        `Route ${req.method} ${req.originalUrl} was not found`,
        "ROUTE_NOT_FOUND"
      )
    );
  });

  app.use(errorHandler);

  return app;
}

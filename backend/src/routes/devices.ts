/**
 * Device-token registration routes (#87).
 *
 * POST  /me/devices           — register an Expo or Web Push token
 * DELETE /me/devices/:id      — unregister a specific device
 *
 * Both endpoints require a valid JWT access token (jwtAuth middleware).
 * The authenticated user's `sub` (Stellar G-address) is used to scope
 * every DB operation, so users can only manage their own devices.
 *
 * Expo push tokens look like:  ExponentPushToken[xxxxxx]
 * Web Push subscriptions are JSON-serialised PushSubscription objects
 * (endpoint + keys.p256dh + keys.auth).
 */

import { Router } from "express";
import { body, param } from "express-validator";
import { jwtAuth } from "../middleware/jwtAuth";
import { assertValidRequest } from "../middleware/requestValidation";
import { NotFoundError, ValidationError } from "../errors/appError";
import { upsertDevice, deleteDevice, Platform } from "../services/deviceRegistry";

export const devicesRouter = Router();

// All device routes require authentication.
devicesRouter.use(jwtAuth);

const EXPO_TOKEN_RE = /^ExponentPushToken\[.+\]$/;
const LOCALE_RE = /^[a-z]{2,3}(-[A-Z]{2})?$/;

devicesRouter.post(
  "/",
  body("platform").isIn(["expo", "web"]).withMessage("platform must be 'expo' or 'web'"),
  body("token")
    .isString()
    .notEmpty()
    .isLength({ max: 2048 })
    .withMessage("token is required and must be ≤2048 chars"),
  body("locale")
    .optional()
    .isString()
    .matches(LOCALE_RE)
    .withMessage("locale must be a BCP-47 tag, e.g. 'en' or 'en-US'"),
  async (req, res, next) => {
    try {
      assertValidRequest(req);

      const { platform, token, locale = "en" } = req.body as {
        platform: Platform;
        token: string;
        locale?: string;
      };

      // Validate Expo token format when platform is "expo".
      if (platform === "expo" && !EXPO_TOKEN_RE.test(token)) {
        throw new ValidationError([
          {
            path: "token",
            message: "Expo push tokens must match ExponentPushToken[...]",
          },
        ]);
      }

      // For "web", token must be valid JSON containing an endpoint.
      if (platform === "web") {
        try {
          const parsed = JSON.parse(token) as { endpoint?: unknown };
          if (typeof parsed.endpoint !== "string" || !parsed.endpoint.startsWith("https://")) {
            throw new Error("missing endpoint");
          }
        } catch {
          throw new ValidationError([
            {
              path: "token",
              message:
                "Web Push token must be a JSON-serialised PushSubscription with an https endpoint",
            },
          ]);
        }
      }

      const account = req.user!.sub;
      const device = await upsertDevice(account, platform, token, locale);

      res.status(201).json({ device });
    } catch (err) {
      next(err);
    }
  }
);

devicesRouter.delete(
  "/:id",
  param("id").isInt({ min: 1 }).withMessage("id must be a positive integer"),
  async (req, res, next) => {
    try {
      assertValidRequest(req);

      const account = req.user!.sub;
      const deleted = await deleteDevice(account, req.params.id);

      if (!deleted) {
        throw new NotFoundError("Device not found", "DEVICE_NOT_FOUND");
      }

      res.json({ message: "Device removed" });
    } catch (err) {
      next(err);
    }
  }
);

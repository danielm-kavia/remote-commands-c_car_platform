"use strict";

const { extractBearerToken, createJwtValidator } = require("@connected-car/shared");

const jwtValidator = createJwtValidator();

/**
 * @typedef {Object} Actor
 * @property {string} sub
 * @property {string=} orgId
 * @property {string[]=} roles
 * @property {any} rawClaims
 */

/**
 * PUBLIC_INTERFACE
 * Express middleware: authenticate via Bearer token (JWT stub from @connected-car/shared).
 *
 * For this MVP we keep it intentionally permissive. If AUTH_REQUIRED=false we attach a dev actor.
 *
 * @param {{ required: boolean, issuer?: string, audience?: string|string[], clockToleranceSeconds: number, logger: any }} options
 */
function authMiddleware(options) {
  return async function auth(req, res, next) {
    try {
      if (!options.required) {
        req.actor = /** @type {Actor} */ ({ sub: "dev-user", orgId: "dev-org", roles: ["admin"], rawClaims: {} });
        return next();
      }

      const token = extractBearerToken(req.header("authorization"));
      if (!token) return res.status(401).json({ ok: false, error: "missing_bearer_token" });

      const validation = await jwtValidator.validate(token, {
        issuer: options.issuer,
        audience: options.audience,
        clockToleranceSeconds: options.clockToleranceSeconds,
      });

      if (!validation.ok) return res.status(401).json({ ok: false, error: validation.errorCode || "invalid_token" });

      const claims = validation.payload || {};
      const sub = claims.sub;

      if (!sub || typeof sub !== "string") return res.status(401).json({ ok: false, error: "missing_sub" });

      req.actor = /** @type {Actor} */ ({
        sub,
        orgId: typeof claims.orgId === "string" ? claims.orgId : undefined,
        roles: Array.isArray(claims.roles) ? claims.roles : (typeof claims.role === "string" ? [claims.role] : undefined),
        rawClaims: claims,
      });

      return next();
    } catch (e) {
      options?.logger?.warn?.("Auth middleware error", { error: String(e?.message || e) });
      return res.status(500).json({ ok: false, error: "auth_error" });
    }
  };
}

module.exports = { authMiddleware };

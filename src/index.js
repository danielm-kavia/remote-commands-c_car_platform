"use strict";

const crypto = require("crypto");
const express = require("express");
const {
  createLogger,
  withCorrelationId,
  getCorrelationId,
  validateAgainstSchema,
  schemas,
  createSecurityHeadersMiddleware,
  createRateLimitMiddleware,
} = require("@connected-car/shared");

const { loadConfig } = require("./config");
const { authMiddleware } = require("./auth");
const { createCommandStore } = require("./store");
const { createTransport } = require("./transport");

const cfg = loadConfig();
const logger = createLogger({ serviceName: cfg.serviceName, level: cfg.logLevel });

const app = express();

// Hardening (Phase 9): security headers + optional rate limiting (disabled by default).
app.use(
  createSecurityHeadersMiddleware({
    serviceName: cfg.serviceName,
    enabled: true,
    enableCsp: String(process.env.SECURITY_ENABLE_CSP || "false").toLowerCase() === "true",
    csp: process.env.SECURITY_CSP || undefined,
    enableHsts: String(process.env.SECURITY_ENABLE_HSTS || "false").toLowerCase() === "true",
  })
);
app.use(
  createRateLimitMiddleware({
    enabled: String(process.env.RATE_LIMIT_ENABLED || "false").toLowerCase() === "true",
    windowSeconds: Number(process.env.RATE_LIMIT_WINDOW_S || 60),
    maxRequests: Number(process.env.RATE_LIMIT_MAX || 100),
    logger,
  })
);

app.use(express.json({ limit: "256kb" }));

/**
 * Generate a stable-ish correlationId for a command (separate from request correlationId).
 * @returns {string}
 */
function newCorrelationId() {
  return crypto.randomUUID();
}

/**
 * Generate a command id (UUID).
 * @returns {string}
 */
function newCommandId() {
  return crypto.randomUUID();
}

const store = createCommandStore(cfg.store, logger);

/** @type {Map<string, NodeJS.Timeout>} */
const ackTimers = new Map();

/**
 * Clear any existing ack timeout timer.
 * @param {string} commandId
 */
function clearAckTimer(commandId) {
  const t = ackTimers.get(commandId);
  if (t) clearTimeout(t);
  ackTimers.delete(commandId);
}

/**
 * Start/restart an ack timeout timer.
 * If the command remains PENDING/SENT beyond timeout, we mark it FAILED (timeout).
 *
 * @param {string} commandId
 */
function armAckTimeout(commandId) {
  clearAckTimer(commandId);

  const t = setTimeout(async () => {
    try {
      const rec = await store.getById(commandId);
      if (!rec) return;

      // If already terminal, ignore.
      if (rec.state === "ACKED" || rec.state === "FAILED") return;

      await store.updateState(commandId, {
        state: "FAILED",
        failedAt: new Date().toISOString(),
        lastAckStatus: "failed",
        lastAckReason: "ack_timeout",
      });

      logger.warn("Command failed due to ack timeout", { commandId });
    } catch (e) {
      logger.warn("Ack timeout handler error", { error: String(e?.message || e), commandId });
    }
  }, cfg.ackTimeoutMs);

  ackTimers.set(commandId, t);
}

/**
 * Apply an ack payload to command state.
 * @param {any} ack
 */
async function reconcileAck(ack) {
  const cmd = await store.getById(String(ack.commandId));
  if (!cmd) {
    logger.info("Ack for unknown command ignored", { commandId: ack.commandId });
    return;
  }

  const status = String(ack.status);
  const now = new Date().toISOString();

  if (status === "accepted") {
    await store.updateState(cmd.id, {
      state: "SENT",
      sentAt: cmd.sentAt || now,
      lastAckStatus: status,
      lastAckReason: ack.reason ? String(ack.reason) : undefined,
    });
    armAckTimeout(cmd.id);
    return;
  }

  if (status === "completed") {
    clearAckTimer(cmd.id);
    await store.updateState(cmd.id, {
      state: "ACKED",
      ackedAt: now,
      lastAckStatus: status,
      lastAckReason: ack.reason ? String(ack.reason) : undefined,
    });
    return;
  }

  if (status === "failed" || status === "rejected") {
    clearAckTimer(cmd.id);
    await store.updateState(cmd.id, {
      state: "FAILED",
      failedAt: now,
      lastAckStatus: status,
      lastAckReason: ack.reason ? String(ack.reason) : undefined,
    });
    return;
  }

  logger.warn("Unknown ack status ignored", { status, commandId: cmd.id });
}

const transport = createTransport(logger, cfg.dispatch, {
  onAck: reconcileAck,
});

app.use(
  authMiddleware({
    required: cfg.auth.required,
    issuer: cfg.auth.issuer,
    audience: cfg.auth.audience,
    clockToleranceSeconds: cfg.auth.clockToleranceSeconds,
    logger,
  })
);

/**
 * PUBLIC_INTERFACE
 * Health endpoint for remote-commands.
 */
app.get(
  "/health",
  withCorrelationId(logger, async (req, res) => {
    res.json({
      ok: true,
      service: cfg.serviceName,
      correlationId: getCorrelationId(),
      transport: {
        kafkaEnabled: transport.isKafkaEnabled(),
        kafkaConfigured: cfg.dispatch.useKafka,
      },
      store: { mode: cfg.store.mode },
    });
  })
);

/**
 * PUBLIC_INTERFACE
 * Issue a remote door unlock command (MVP).
 *
 * POST /commands/unlock
 * Body:
 * {
 *   "vehicleId": "VIN123"
 * }
 */
app.post(
  "/commands/unlock",
  withCorrelationId(logger, async (req, res) => {
    const vehicleId = req?.body?.vehicleId;

    if (!vehicleId || typeof vehicleId !== "string") {
      return res.status(400).json({ ok: false, error: "missing_vehicleId" });
    }

    const commandId = newCommandId();
    const correlationId = newCorrelationId();
    const requestedAt = new Date().toISOString();

    const commandPayload = {
      schemaVersion: "v1",
      commandId,
      vehicleId,
      commandType: "unlock",
      requestedAt,
      parameters: {},
    };

    // Validate with shared schema (remote-command v1)
    const v = validateAgainstSchema(schemas.remoteCommand.v1, commandPayload);
    if (!v.ok) return res.status(400).json({ ok: false, error: "schema_validation_failed", details: v.errors });

    const record = {
      id: commandId,
      correlationId,
      vehicleId,
      commandType: "unlock",
      state: "PENDING",
      requestedAt,
      updatedAt: requestedAt,
    };

    await store.put(record);

    // Start timeout immediately for "end-to-end" MVP (accepted/completed acks will reconcile).
    armAckTimeout(commandId);

    // Dispatch is non-blocking-friendly: it should not crash if gateway/kafka are down.
    const dispatchResult = await transport.dispatchCommand(commandPayload).catch((e) => ({
      ok: false,
      error: "dispatch_error",
      details: String(e?.message || e),
    }));

    if (!dispatchResult.ok) {
      await store.updateState(commandId, {
        state: "FAILED",
        failedAt: new Date().toISOString(),
        lastAckStatus: "failed",
        lastAckReason: `dispatch_failed:${dispatchResult.error}`,
      });
      clearAckTimer(commandId);

      return res.status(502).json({
        ok: false,
        error: "dispatch_failed",
        details: dispatchResult,
        command: await store.getById(commandId),
      });
    }

    return res.status(202).json({
      ok: true,
      command: await store.getById(commandId),
    });
  })
);

/**
 * PUBLIC_INTERFACE
 * Get a command by id.
 * GET /commands/:id
 */
app.get(
  "/commands/:id",
  withCorrelationId(logger, async (req, res) => {
    const id = String(req.params.id || "");
    const rec = await store.getById(id);
    if (!rec) return res.status(404).json({ ok: false, error: "not_found" });
    return res.json({ ok: true, command: rec });
  })
);

/**
 * PUBLIC_INTERFACE
 * List recent commands, optionally by vehicleId.
 * GET /commands?vehicleId=VIN123
 */
app.get(
  "/commands",
  withCorrelationId(logger, async (req, res) => {
    const vehicleId = req.query.vehicleId ? String(req.query.vehicleId) : undefined;
    const limit = req.query.limit ? Math.max(1, Math.min(200, Number(req.query.limit))) : 50;

    const list = vehicleId ? await store.listByVehicleId(vehicleId, limit) : await store.listRecent(limit);
    return res.json({ ok: true, commands: list });
  })
);

/**
 * PUBLIC_INTERFACE
 * Dev/ingress endpoint for command-ack messages (HTTP fallback).
 *
 * POST /acks
 * Body: CommandAckV1
 */
app.post(
  "/acks",
  withCorrelationId(logger, async (req, res) => {
    const payload = req.body;

    const v = validateAgainstSchema(schemas.commandAck.v1, payload);
    if (!v.ok) return res.status(400).json({ ok: false, error: "schema_validation_failed", details: v.errors });

    await reconcileAck(payload);
    return res.status(200).json({ ok: true });
  })
);

async function main() {
  app.listen(cfg.port, cfg.host, () => {
    logger.info("Remote commands service listening", { port: cfg.port });
  });

  // Start optional Kafka in background.
  transport.start().catch((e) => {
    logger.warn("Transport failed to start (non-fatal)", { error: String(e?.message || e) });
  });

  process.on("SIGINT", async () => {
    try {
      await transport.stop();
    } catch (_) {}
    process.exit(0);
  });
  process.on("SIGTERM", async () => {
    try {
      await transport.stop();
    } catch (_) {}
    process.exit(0);
  });
}

main().catch((e) => {
  logger.error("Fatal startup error", { error: String(e?.message || e) });
});

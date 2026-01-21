"use strict";

const fs = require("fs");
const path = require("path");

/**
 * @typedef {"PENDING"|"SENT"|"ACKED"|"FAILED"} CommandState
 */

/**
 * @typedef {Object} CommandRecord
 * @property {string} id
 * @property {string} correlationId
 * @property {string} vehicleId
 * @property {"unlock"} commandType
 * @property {CommandState} state
 * @property {string} requestedAt
 * @property {string=} sentAt
 * @property {string=} ackedAt
 * @property {string=} failedAt
 * @property {string=} lastAckStatus
 * @property {string=} lastAckReason
 * @property {string=} updatedAt
 */

/**
 * Ensure a directory exists (mkdir -p behavior).
 * @param {string} dirPath
 */
function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

/**
 * Atomically write JSON file by writing to a tmp file then renaming.
 * @param {string} filePath
 * @param {any} data
 */
function writeJsonAtomic(filePath, data) {
  ensureDir(path.dirname(filePath));
  const tmp = `${filePath}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), "utf8");
  fs.renameSync(tmp, filePath);
}

/**
 * Read JSON file if present; otherwise returns fallback.
 * @param {string} filePath
 * @param {any} fallback
 */
function readJsonOr(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    const raw = fs.readFileSync(filePath, "utf8");
    return JSON.parse(raw);
  } catch (_) {
    return fallback;
  }
}

/**
 * PUBLIC_INTERFACE
 * Create a Remote Commands store.
 *
 * @param {{ mode: "memory"|"file", filePath: string, maxRecentPerVehicle: number }} cfg
 * @param {any} logger
 * @returns {{
 *  put: (rec: CommandRecord) => Promise<void>,
 *  getById: (id: string) => Promise<CommandRecord|undefined>,
 *  listByVehicleId: (vehicleId: string, limit?: number) => Promise<CommandRecord[]>,
 *  listRecent: (limit?: number) => Promise<CommandRecord[]>,
 *  updateState: (id: string, patch: Partial<CommandRecord>) => Promise<CommandRecord|undefined>
 * }}
 */
function createCommandStore(cfg, logger) {
  /** @type {Map<string, CommandRecord>} */
  const byId = new Map();
  /** @type {Map<string, string[]>} */
  const recentByVehicle = new Map();
  /** @type {string[]} */
  let globalRecent = [];

  function hydrateFromDisk() {
    if (cfg.mode !== "file") return;
    const data = readJsonOr(cfg.filePath, { byId: {}, recentByVehicle: {}, globalRecent: [] });
    try {
      byId.clear();
      for (const [id, rec] of Object.entries(data.byId || {})) byId.set(id, rec);
      recentByVehicle.clear();
      for (const [vehicleId, arr] of Object.entries(data.recentByVehicle || {})) {
        recentByVehicle.set(vehicleId, Array.isArray(arr) ? arr : []);
      }
      globalRecent = Array.isArray(data.globalRecent) ? data.globalRecent : [];
      logger.info("Remote command store hydrated (file)", { path: cfg.filePath, count: byId.size });
    } catch (e) {
      logger.warn("Remote command store hydration failed; starting empty", { error: String(e?.message || e) });
    }
  }

  function persistToDisk() {
    if (cfg.mode !== "file") return;
    try {
      /** @type {Record<string, CommandRecord>} */
      const byIdObj = {};
      for (const [id, rec] of byId.entries()) byIdObj[id] = rec;
      const recentObj = {};
      for (const [vehicleId, list] of recentByVehicle.entries()) recentObj[vehicleId] = list;
      writeJsonAtomic(cfg.filePath, { byId: byIdObj, recentByVehicle: recentObj, globalRecent });
    } catch (e) {
      logger.warn("Remote command store persist failed", { error: String(e?.message || e) });
    }
  }

  function pushRecent(vehicleId, id) {
    const perVehicle = recentByVehicle.get(vehicleId) || [];
    const nextVehicle = [id, ...perVehicle.filter((x) => x !== id)].slice(0, cfg.maxRecentPerVehicle);
    recentByVehicle.set(vehicleId, nextVehicle);

    globalRecent = [id, ...globalRecent.filter((x) => x !== id)].slice(0, 500);
  }

  hydrateFromDisk();

  async function put(rec) {
    byId.set(rec.id, rec);
    pushRecent(rec.vehicleId, rec.id);
    persistToDisk();
  }

  async function getById(id) {
    return byId.get(id);
  }

  async function listByVehicleId(vehicleId, limit = 50) {
    const ids = recentByVehicle.get(vehicleId) || [];
    const records = ids.map((id) => byId.get(id)).filter(Boolean);
    return records.slice(0, limit);
  }

  async function listRecent(limit = 50) {
    const records = globalRecent.map((id) => byId.get(id)).filter(Boolean);
    return records.slice(0, limit);
  }

  async function updateState(id, patch) {
    const existing = byId.get(id);
    if (!existing) return undefined;

    const updated = {
      ...existing,
      ...patch,
      updatedAt: new Date().toISOString(),
    };
    byId.set(id, updated);
    pushRecent(updated.vehicleId, id);
    persistToDisk();
    return updated;
  }

  return { put, getById, listByVehicleId, listRecent, updateState };
}

module.exports = { createCommandStore };

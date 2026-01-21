"use strict";

/**
 * PUBLIC_INTERFACE
 * Load Remote Commands service configuration from environment variables.
 * No .env file is read directly here; process.env is assumed to be populated by runtime tooling.
 *
 * @returns {{
 *   serviceName: string,
 *   host: string,
 *   port: number,
 *   logLevel: "debug"|"info"|"warn"|"error",
 *   auth: { required: boolean, issuer?: string, audience?: string|string[], clockToleranceSeconds: number },
 *   store: { mode: "memory"|"file", filePath: string, maxRecentPerVehicle: number },
 *   dispatch: {
 *     useKafka: boolean,
 *     kafka: {
 *       brokers: string[],
 *       clientId: string,
 *       consumerGroupId: string,
 *       topicRemoteCommand: string,
 *       topicCommandAck: string
 *     },
 *     http: {
 *       gatewayUrl: string,
 *       publicServiceUrl: string
 *     }
 *   },
 *   ackTimeoutMs: number
 * }}
 */
function loadConfig() {
  function env(name, fallback) {
    return process.env[name] !== undefined ? process.env[name] : fallback;
  }

  function parseBool(value, fallback) {
    if (value === undefined) return fallback;
    const v = String(value).toLowerCase().trim();
    if (["1", "true", "yes", "y", "on"].includes(v)) return true;
    if (["0", "false", "no", "n", "off"].includes(v)) return false;
    return fallback;
  }

  const authRequired = parseBool(env("AUTH_REQUIRED", "true"), true);

  // JWT audience can be a comma-separated list
  const audRaw = String(env("JWT_AUDIENCE", "") || "").trim();
  const audience =
    audRaw && audRaw.includes(",")
      ? audRaw
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : audRaw || undefined;

  const brokers = String(env("KAFKA_BROKERS", "localhost:9092"))
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  return {
    serviceName: "remote-commands",
    host: env("HOST", "0.0.0.0"),
    port: Number(env("PORT", "3020")),
    logLevel: /** @type {any} */ (env("LOG_LEVEL", "info")),
    auth: {
      required: authRequired,
      issuer: String(env("JWT_ISSUER", "") || "").trim() || undefined,
      audience,
      clockToleranceSeconds: Number(env("JWT_CLOCK_TOLERANCE_SECONDS", "0")),
    },
    store: {
      mode: env("RC_STORE", "file") === "memory" ? "memory" : "file",
      filePath: env("RC_STORE_FILE_PATH", "./data/remote-commands.json"),
      maxRecentPerVehicle: Number(env("RC_MAX_RECENT_PER_VEHICLE", "50")),
    },
    dispatch: {
      useKafka: parseBool(env("RC_USE_KAFKA", "false"), false),
      kafka: {
        brokers,
        clientId: env("KAFKA_CLIENT_ID", "remote-commands"),
        consumerGroupId: env("KAFKA_CONSUMER_GROUP_ID", "remote-commands-acks-v1"),
        topicRemoteCommand: env("KAFKA_TOPIC_REMOTE_COMMAND", "remote-command.v1"),
        topicCommandAck: env("KAFKA_TOPIC_COMMAND_ACK", "command-ack.v1"),
      },
      http: {
        gatewayUrl: env("RC_GATEWAY_HTTP_URL", "http://localhost:3004"),
        publicServiceUrl: env("RC_PUBLIC_HTTP_URL", "http://localhost:3020"),
      },
    },
    ackTimeoutMs: Math.max(1000, Number(env("RC_ACK_TIMEOUT_MS", "15000"))),
  };
}

module.exports = { loadConfig };

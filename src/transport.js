"use strict";

const { Kafka } = require("kafkajs");
const { validateAgainstSchema, schemas } = require("@connected-car/shared");

/**
 * @typedef {Object} Transport
 * @property {(cmd: any) => Promise<{ ok: true } | { ok: false, error: string, details?: any }>} dispatchCommand
 * @property {() => Promise<void>} start
 * @property {() => Promise<void>} stop
 * @property {() => boolean} isKafkaEnabled
 */

/**
 * Build an HTTP JSON response helper.
 * @param {Response} resp
 */
async function readRespText(resp) {
  try {
    return await resp.text();
  } catch (_) {
    return "";
  }
}

/**
 * PUBLIC_INTERFACE
 * Create a transport for dispatching remote commands and ingesting command acks.
 *
 * Kafka is optional. If Kafka is configured but unavailable, start() logs and continues.
 *
 * @param {any} logger
 * @param {{
 *   useKafka: boolean,
 *   kafka: {
 *     brokers: string[],
 *     clientId: string,
 *     consumerGroupId: string,
 *     topicRemoteCommand: string,
 *     topicCommandAck: string
 *   },
 *   http: { gatewayUrl: string }
 * }} cfg
 * @param {{
 *   onAck: (ackPayload: any) => Promise<void>
 * }} handlers
 * @returns {Transport}
 */
function createTransport(logger, cfg, handlers) {
  /** @type {import("kafkajs").Kafka | undefined} */
  let kafka;
  /** @type {import("kafkajs").Producer | undefined} */
  let producer;
  /** @type {import("kafkajs").Consumer | undefined} */
  let consumer;
  let kafkaHealthy = false;
  let started = false;

  async function startKafkaProducerAndConsumer() {
    kafka = new Kafka({ brokers: cfg.kafka.brokers, clientId: cfg.kafka.clientId });
    producer = kafka.producer();
    consumer = kafka.consumer({ groupId: cfg.kafka.consumerGroupId });

    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({ topic: cfg.kafka.topicCommandAck, fromBeginning: false });

    await consumer.run({
      autoCommit: true,
      eachMessage: async ({ topic, partition, message }) => {
        const rawValue = message.value ? message.value.toString("utf8") : "";
        let payload;
        try {
          payload = JSON.parse(rawValue);
        } catch (_) {
          logger.warn("Command-ack Kafka message parse failed; skipping", { topic, partition, offset: message.offset });
          return;
        }

        const v = validateAgainstSchema(schemas.commandAck.v1, payload);
        if (!v.ok) {
          logger.warn("Command-ack Kafka message schema invalid; skipping", { errors: v.errors });
          return;
        }

        await handlers.onAck(payload);
      },
    });

    kafkaHealthy = true;
    logger.info("Remote-commands transport Kafka connected", {
      topicRemoteCommand: cfg.kafka.topicRemoteCommand,
      topicCommandAck: cfg.kafka.topicCommandAck,
    });
  }

  async function start() {
    if (started) return;
    started = true;

    if (!cfg.useKafka) {
      logger.info("Remote-commands transport Kafka disabled (RC_USE_KAFKA=false); using HTTP gateway fallback");
      return;
    }

    // Non-blocking optional Kafka: failures should not crash the service.
    startKafkaProducerAndConsumer().catch((e) => {
      kafkaHealthy = false;
      logger.warn("Kafka unavailable; remote-commands will fall back to HTTP gateway dispatch", {
        error: String(e?.message || e),
      });
    });
  }

  async function stop() {
    started = false;
    kafkaHealthy = false;

    if (consumer) {
      try {
        await consumer.disconnect();
      } catch (e) {
        logger.warn("Kafka consumer disconnect failed", { error: String(e?.message || e) });
      }
    }
    if (producer) {
      try {
        await producer.disconnect();
      } catch (e) {
        logger.warn("Kafka producer disconnect failed", { error: String(e?.message || e) });
      }
    }

    consumer = undefined;
    producer = undefined;
    kafka = undefined;
  }

  async function dispatchCommandKafka(commandPayload) {
    if (!producer || !kafkaHealthy) return { ok: false, error: "kafka_not_ready" };
    await producer.send({
      topic: cfg.kafka.topicRemoteCommand,
      messages: [{ value: JSON.stringify(commandPayload) }],
    });
    return { ok: true };
  }

  async function dispatchCommandHttp(commandPayload) {
    const url = `${cfg.http.gatewayUrl.replace(/\/+$/, "")}/v1/dev/commands`;
    const resp = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(commandPayload),
    });

    if (!resp.ok) {
      const text = await readRespText(resp);
      return { ok: false, error: `gateway_http_failed:${resp.status}`, details: text };
    }

    // Gateway may respond with an immediate ack (accepted). If so, ingest it.
    let maybeAck;
    try {
      maybeAck = await resp.json();
    } catch (_) {
      maybeAck = undefined;
    }

    if (maybeAck && maybeAck.schemaVersion === "v1" && maybeAck.commandId) {
      const v = validateAgainstSchema(schemas.commandAck.v1, maybeAck);
      if (v.ok) await handlers.onAck(maybeAck);
    }

    return { ok: true };
  }

  async function dispatchCommand(commandPayload) {
    // Validate outbound command against shared schema before dispatch.
    const v = validateAgainstSchema(schemas.remoteCommand.v1, commandPayload);
    if (!v.ok) return { ok: false, error: "schema_validation_failed", details: v.errors };

    // Prefer Kafka if enabled AND healthy, else HTTP fallback.
    if (cfg.useKafka && kafkaHealthy) {
      try {
        return await dispatchCommandKafka(commandPayload);
      } catch (e) {
        logger.warn("Kafka dispatch failed; falling back to HTTP gateway", { error: String(e?.message || e) });
        return await dispatchCommandHttp(commandPayload);
      }
    }
    return await dispatchCommandHttp(commandPayload);
  }

  function isKafkaEnabled() {
    return Boolean(cfg.useKafka && kafkaHealthy);
  }

  return { dispatchCommand, start, stop, isKafkaEnabled };
}

module.exports = { createTransport };

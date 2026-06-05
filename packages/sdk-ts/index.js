/* eslint-disable */
/**
 * tap-cdc — TypeScript SDK for Tap CDC engine.
 *
 * Entry point for `import { Tap, normalizeConfig, changeEventToJson } from "tap-cdc"`.
 *
 * The native binding is loaded lazily when the `Tap` class is instantiated,
 * so pure functions (`normalizeConfig`, `changeEventToJson`) are always
 * accessible without the native binary.
 */

"use strict";

const { existsSync, readFileSync } = require("fs");
const { join } = require("path");

const { platform, arch } = process;

// ---------------------------------------------------------------------------
// Lazy native binding resolver
// ---------------------------------------------------------------------------

let nativeBinding = null;
let loadError = null;

/**
 * Detect whether the system uses musl libc (Alpine Linux et al.).
 */
function isMusl() {
  if (process.report && typeof process.report.getReport === "function") {
    try {
      const report = process.report.getReport();
      const sharedObjects =
        report && typeof report === "object" ? report.sharedObjects : null;
      if (sharedObjects && Array.isArray(sharedObjects)) {
        if (sharedObjects.some((s) => s.includes("musl") || s.includes("ld-musl"))) {
          return true;
        }
      }
    } catch {
      // fall through to ldd check
    }
  }
  try {
    return readFileSync("/usr/bin/ldd", "utf8").includes("musl");
  } catch {
    return false;
  }
}

const musl = platform === "linux" ? isMusl() : false;

function resolveBinding() {
  if (nativeBinding) return nativeBinding;
  if (loadError) throw loadError;

  switch (platform) {
    case "darwin":
      switch (arch) {
        case "arm64":
          try {
            nativeBinding = require("tap-cdc-darwin-arm64");
          } catch (e) {
            loadError = e;
          }
          break;
        case "x64":
          try {
            nativeBinding = require("tap-cdc-darwin-x64");
          } catch (e) {
            loadError = e;
          }
          break;
        default:
          throw new Error(
            `Unsupported architecture on macOS: ${arch}. ` +
              `tap-cdc supports arm64 and x64. ` +
              `Please file an issue at https://github.com/levu304/tap/issues`
          );
      }
      break;

    case "linux":
      switch (arch) {
        case "arm64": {
          if (musl) {
            throw new Error(
              "The platform linux-arm64-musl is not supported by tap-cdc. " +
                "Please file an issue at https://github.com/levu304/tap/issues"
            );
          }
          try {
            nativeBinding = require("tap-cdc-linux-arm64-gnu");
          } catch (e) {
            loadError = e;
          }
          break;
        }
        case "x64": {
          if (musl) {
            try {
              nativeBinding = require("tap-cdc-linux-x64-musl");
            } catch (e) {
              loadError = e;
            }
          } else {
            try {
              nativeBinding = require("tap-cdc-linux-x64-gnu");
            } catch (e) {
              loadError = e;
            }
          }
          break;
        }
        default:
          throw new Error(
            `Unsupported architecture on Linux: ${arch}. ` +
              `tap-cdc supports arm64 and x64. ` +
              `Please file an issue at https://github.com/levu304/tap/issues`
          );
      }
      break;

    default:
      throw new Error(
        `Unsupported OS: ${platform}, architecture: ${arch}. ` +
          `tap-cdc currently supports macOS (arm64, x64) and Linux (arm64, x64). ` +
          `Please file an issue at https://github.com/levu304/tap/issues`
      );
  }

  // Fallback: local development build
  if (!nativeBinding) {
    let localTriple;
    if (platform === "darwin") {
      localTriple = `${platform}-${arch}`;
    } else if (platform === "linux") {
      localTriple = `${platform}-${arch}${musl ? "-musl" : "-gnu"}`;
    }
    if (localTriple) {
      const localFile = join(__dirname, `tap-sdk.${localTriple}.node`);
      if (existsSync(localFile)) {
        try {
          nativeBinding = require(localFile);
        } catch (e) {
          loadError = e;
        }
      }
    }
  }

  // Generic fallback name
  if (!nativeBinding) {
    const fallbackFile = join(__dirname, "tap-sdk.node");
    if (existsSync(fallbackFile)) {
      try {
        nativeBinding = require(fallbackFile);
      } catch (e) {
        loadError = e;
      }
    }
  }

  if (!nativeBinding) {
    if (loadError) throw loadError;
    throw new Error(
      `Failed to load native binding for tap-cdc on ${platform}-${arch}.\n\n` +
        `This usually means one of the following:\n` +
        `  1. \`npm install\` did not download the correct platform package.\n` +
        `     Try reinstalling: rm -rf node_modules && npm install\n` +
        `  2. You are running on an unsupported platform/architecture.\n` +
        `     Supported targets: darwin-arm64, darwin-x64, linux-arm64-gnu, linux-x64-gnu, linux-x64-musl\n` +
        `  3. The native addon has not been built yet (local development).\n` +
        `     Run: napi build --platform --release\n` +
        `  4. The package manager hoisted the platform package incorrectly.\n` +
        `     Try: npm dedupe\n\n` +
        `If none of these apply, please file an issue at:\n` +
        `  https://github.com/levu304/tap/issues`
    );
  }

  return nativeBinding;
}

// ---------------------------------------------------------------------------
// Tap class (lazy binding)
// ---------------------------------------------------------------------------

/**
 * Tap CDC session manager.
 *
 * Manages the full lifecycle of a Postgres logical-replication capture:
 * connecting, slot/publication setup, WAL streaming, SSE delivery,
 * and in-process JS callbacks.
 */
class Tap {
  constructor(config) {
    const binding = resolveBinding();
    this.inner = new binding.Tap(normalizeConfig(config));
  }

  /** Start capturing — returns the SSE endpoint URL. */
  async start() {
    return this.inner.start();
  }

  /** Stop capturing and release all resources. */
  async stop() {
    return this.inner.stop();
  }

  /** Pause WAL reading while keeping Postgres connections open. */
  async pause() {
    return this.inner.pause();
  }

  /** Resume WAL reading after a pause. */
  async resume() {
    return this.inner.resume();
  }

  /** Return the current capture status. */
  async status() {
    return this.inner.status();
  }

  /** Register a callback invoked on every row-level change event. */
  onChange(handler) {
    this.inner.onChange(handler);
  }

  /** Register a callback invoked on capture errors. */
  onError(handler) {
    this.inner.onError(handler);
  }
}

// ---------------------------------------------------------------------------
// Pure functions (no native binding required)
// ---------------------------------------------------------------------------

/**
 * Convert the public TapConfig (camelCase) to the native form
 * (snake_case) expected by the napi-rs binding.
 */
function normalizeConfig(config) {
  return {
    connection: config.connection,
    slot_name: config.slotName,
    publication: config.publication,
    tables: config.tables,
    plugin: config.plugin,
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
    password: config.password,
    state_path: config.statePath,
    max_batch_size: config.maxBatchSize,
    flush_interval_ms: config.flushIntervalMs,
    ssl_mode: config.sslMode,
    sink: config.sink
      ? {
          host: config.sink.host,
          port: config.sink.port,
          max_buffer_size: config.sink.maxBufferSize,
          heartbeat_interval_ms: config.sink.heartbeatIntervalMs,
        }
      : undefined,
  };
}

/**
 * Serialize a ChangeEvent to a JSON string.
 *
 * This is a standalone function because napi-rs delivers #[napi(object)]
 * structs as plain JS objects — instance methods are not available at
 * runtime on callback-delivered events.
 */
function changeEventToJson(event) {
  return JSON.stringify(event, null, 2);
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = { Tap, normalizeConfig, changeEventToJson };
module.exports.default = Tap;

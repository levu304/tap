/**
 * @tap/sdk — PostgreSQL Change Data Capture SDK
 *
 * Native Node.js binding to the Tap CDC engine via napi-rs.
 * Provides a `Tap` class for managing CDC sessions with
 * in-process event callbacks and SSE delivery.
 *
 * @example
 * ```ts
 * import { Tap } from "@tap/sdk";
 *
 * const tap = new Tap({
 *   connection: "postgresql://user:pass@localhost/db",
 *   tables: ["public.users"],
 * });
 *
 * tap.onChange((event) => {
 *   console.log(`[${event.op}] ${event.source.table}`, event.after);
 * });
 *
 * const sseUrl = await tap.start();
 * console.log(`SSE endpoint: ${sseUrl}`);
 * ```
 */

// The auto-generated bindings are loaded from `index.js` after `napi build`.
// During development / type-checking, `./binding` resolves after a build.

// eslint-disable-next-line @typescript-eslint/no-require-imports
const binding = require("./binding");

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Source database metadata for a change event. */
export interface SourceMetadata {
  /** Source database name. */
  db: string;
  /** Source schema name. */
  schema: string;
  /** Source table name. */
  table: string;
  /** Postgres WAL Log Sequence Number (e.g., `"0/16B37428"`). */
  lsn: string;
  /** Transaction identifier. */
  txId: string;
  /** Timestamp (ms since UNIX epoch) of the change in Postgres. */
  tsMs: number;
  /** `true` when this event originated from a snapshot. */
  snapshot?: boolean;
}

/** A single row-level change event in Debezium-like envelope format. */
export interface ChangeEvent {
  /** Operation type: `"c"` (create), `"u"` (update), `"d"` (delete), `"r"` (snapshot read). */
  op: "c" | "u" | "d" | "r";
  /** Row state before the change (`null` for inserts). */
  before: Record<string, unknown> | null;
  /** Row state after the change (`null` for deletes). */
  after: Record<string, unknown> | null;
  /** Source metadata describing the origin of this event. */
  source: SourceMetadata;
  /** Timestamp (ms since UNIX epoch) of the Tap event. */
  tsMs: number;
  /** Unique event identifier (`{lsn}:{txId}` for streaming, `snap:...` for snapshot). */
  id: string;

}

/** Current capture-engine status. */
export interface CaptureStatus {
  /** Current state: `"idle" | "snapshot" | "streaming" | "paused" | "stopped"`. */
  state: "idle" | "snapshot" | "streaming" | "paused" | "stopped";
  /** Total number of events captured since the session started. */
  eventsCaptured: number;
  /** Current WAL Log Sequence Number. */
  currentLsn: string;
  /** Approximate capture lag in milliseconds. */
  lagMs: number;
}

/** Optional SSE sink configuration. */
export interface SinkConfig {
  /** Host address for the SSE server to bind to (default: `"127.0.0.1"`). */
  host?: string;
  /** Port for the SSE server (default: `0` = ephemeral). */
  port?: number;
  /** Maximum number of buffered events (default: `10000`). */
  maxBufferSize?: number;
  /** SSE heartbeat interval in milliseconds (default: `30000`). */
  heartbeatIntervalMs?: number;
}

/** Configuration for creating a {@link Tap} instance. */
export interface TapConfig {
  /**
   * Postgres connection string.
   * Overrides `host`, `port`, `database`, `user`, and `password` when set.
   */
  connection: string;
  /** Logical replication slot name (default: `"tap_slot"`). */
  slotName?: string;
  /** Publication name (default: `"tap_publication"`). */
  publication?: string;
  /** Tables to capture (e.g., `["public.users", "public.orders"]`). Empty means all. */
  tables?: string[];
  /** Output plugin: `"pgoutput"` or `"wal2json"` (default: `"pgoutput"`). */
  plugin?: "pgoutput" | "wal2json";
  /** Postgres server hostname (default: `"localhost"`). */
  host?: string;
  /** Postgres server port (default: `5432`). */
  port?: number;
  /** Database name. */
  database?: string;
  /** Replication user name. */
  user?: string;
  /** Replication user password. */
  password?: string;
  /** Path to the SQLite state store (default: `".tap/state.db"`). */
  statePath?: string;
  /** Maximum number of events per batch (default: `100`). */
  maxBatchSize?: number;
  /** Flush interval in milliseconds (default: `1000`). */
  flushIntervalMs?: number;
  /** TLS encryption mode for the Postgres connection (`"disable"`, `"require"`, `"verify-ca"`, or `"verify-full"`; default: `"disable"`). */
  sslMode?: string;
  /** Optional SSE sink configuration. */
  sink?: SinkConfig;
}

// ---------------------------------------------------------------------------
// Tap class
// ---------------------------------------------------------------------------

/**
 * Tap CDC session manager.
 *
 * Manages the full lifecycle of a Postgres logical-replication capture:
 * connecting, slot/publication setup, WAL streaming, SSE delivery,
 * and in-process JS callbacks.
 */
export class Tap {
  private inner: InstanceType<typeof binding.Tap>;

  /**
   * Create a new Tap instance.
   *
   * Opens the SQLite state store and validates the config, but does **not**
   * connect to Postgres.  Call {@link start} to begin capturing.
   *
   * @param config - Capture session configuration.
   */
  constructor(config: TapConfig) {
    this.inner = new binding.Tap(normalizeConfig(config));
  }

  /**
   * Start capturing changes.
   *
   * Connects to Postgres, ensures the replication slot and publication,
   * starts the SSE event server, and begins streaming WAL changes.
   *
   * @returns The SSE endpoint URL (e.g., `"http://127.0.0.1:{port}/events"`).
   */
  async start(): Promise<string> {
    return this.inner.start();
  }

  /**
   * Stop capturing and release all resources.
   *
   * Flushes the final checkpoint to the state store, closes the Postgres
   * connection, and shuts down the SSE server.
   */
  async stop(): Promise<void> {
    return this.inner.stop();
  }

  /**
   * Pause WAL reading while keeping Postgres connections open.
   *
   * @throws If the capture is not in the `"streaming"` state.
   */
  async pause(): Promise<void> {
    return this.inner.pause();
  }

  /**
   * Resume WAL reading after a pause.
   *
   * @throws If the capture is not in the `"paused"` state.
   */
  async resume(): Promise<void> {
    return this.inner.resume();
  }

  /**
   * Return the current capture status.
   *
   * Includes the state machine value, total events captured, current LSN,
   * and approximate lag.
   */
  async status(): Promise<CaptureStatus> {
    return this.inner.status();
  }

  /**
   * Register a callback invoked on every row-level change event.
   *
   * Only one callback can be registered at a time; calling this method
   * again replaces the previous handler.
   *
   * @param handler - Function receiving the {@link ChangeEvent}.
   */
  onChange(handler: (event: ChangeEvent) => void): void {
    this.inner.onChange(handler);
  }

  /**
   * Register a callback invoked on capture errors.
   *
   * Only one callback can be registered at a time.
   *
   * @param handler - Function receiving the error.
   */
  onError(handler: (error: Error) => void): void {
    this.inner.onError((message: string) => {
      handler(new Error(message));
    });
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Convert the public `TapConfig` (camelCase) to the native form
 * (snake_case) expected by the napi-rs binding.
 */
function normalizeConfig(config: TapConfig): Record<string, unknown> {
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
 * Serialize a {@link ChangeEvent} to a JSON string.
 *
 * This is a standalone function (not a method) because napi-rs delivers
 * `#[napi(object)]` structs as plain JS objects, so instance methods
 * are not available at runtime on callback-delivered events.
 *
 * @param event - The change event to serialize.
 * @returns A JSON representation of the event.
 */
export function changeEventToJson(event: ChangeEvent): string {
  return JSON.stringify(event, null, 2);
}

// ---------------------------------------------------------------------------
// Re-exports
// ---------------------------------------------------------------------------

export default Tap;

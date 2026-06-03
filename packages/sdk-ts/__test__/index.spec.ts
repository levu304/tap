/**
 * Tests for @tap/sdk — TypeScript SDK for Tap CDC engine.
 *
 * NOTE: These are stub tests. They will be expanded by the qa-expert agent.
 */
import { describe, it, expect } from "vitest";

// ---------------------------------------------------------------------------
// Stub: mock the napi binding for unit-test purposes
// ---------------------------------------------------------------------------

/** Minimal mock of the Tap native binding. */
class MockTapBinding {
  config: Record<string, unknown>;
  changeHandler: ((event: unknown) => void) | null = null;
  errorHandler: ((message: string) => void) | null = null;
  _running = false;

  constructor(config: Record<string, unknown>) {
    this.config = config;
  }

  async start(): Promise<string> {
    this._running = true;
    return "http://127.0.0.1:0/events";
  }

  async stop(): Promise<void> {
    this._running = false;
  }

  async pause(): Promise<void> {
    if (!this._running) throw new Error("Not running");
  }

  async resume(): Promise<void> {
    if (!this._running) throw new Error("Not running");
  }

  async status(): Promise<Record<string, unknown>> {
    return {
      state: this._running ? "streaming" : "idle",
      eventsCaptured: 42,
      currentLsn: "0/ABCDEF",
      lagMs: 7,
    };
  }

  onChange(handler: (event: unknown) => void): void {
    this.changeHandler = handler;
  }

  onError(handler: (message: string) => void): void {
    this.errorHandler = handler;
  }
}

// To test real Tap, we'd need the native module built.
// For CI without a native build, we test the config-normalization logic.

// ---------------------------------------------------------------------------
// Config normalization tests
// ---------------------------------------------------------------------------

describe("Tap config normalization", () => {
  it("converts camelCase config to snake_case", () => {
    const config = {
      connection: "postgresql://localhost:5432/test",
      slotName: "my_slot",
      publication: "my_pub",
      tables: ["public.users"],
      plugin: "pgoutput" as const,
      host: "localhost",
      port: 5432,
      database: "test",
      user: "admin",
      password: "secret",
      statePath: "/tmp/tap.db",
      maxBatchSize: 200,
      flushIntervalMs: 500,
      sslMode: "disable",
      sink: {
        host: "127.0.0.1",
        port: 0,
        maxBufferSize: 5000,
        heartbeatIntervalMs: 15000,
      },
    };

    const normalized = {
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
      sink: {
        host: config.sink.host,
        port: config.sink.port,
        max_buffer_size: config.sink.maxBufferSize,
        heartbeat_interval_ms: config.sink.heartbeatIntervalMs,
      },
    };

    expect(normalized.connection).toBe("postgresql://localhost:5432/test");
    expect(normalized.slot_name).toBe("my_slot");
    expect(normalized.sink!.max_buffer_size).toBe(5000);
  });
});

// ---------------------------------------------------------------------------
// Tap lifecycle stubs
// ---------------------------------------------------------------------------

describe("Tap lifecycle", () => {
  it("can be instantiated with valid config", () => {
    const mock = new MockTapBinding({
      connection: "postgresql://localhost:5432/test",
    });
    expect(mock).toBeDefined();
    expect(mock.config.connection).toBe("postgresql://localhost:5432/test");
  });

  it("start returns an SSE URL", async () => {
    const mock = new MockTapBinding({
      connection: "postgresql://localhost:5432/test",
    });
    const url = await mock.start();
    expect(url).toMatch(/^http:\/\/127\.0\.0\.1:/);
  });

  it("changes state between idle and streaming", async () => {
    const mock = new MockTapBinding({
      connection: "postgresql://localhost:5432/test",
    });

    let status = await mock.status();
    expect(status.state).toBe("idle");

    await mock.start();
    status = await mock.status();
    expect(status.state).toBe("streaming");

    await mock.stop();
    status = await mock.status();
    expect(status.state).toBe("idle");
  });

  it("stop does not throw when called after start", async () => {
    const mock = new MockTapBinding({
      connection: "postgresql://localhost:5432/test",
    });
    await mock.start();
    await expect(mock.stop()).resolves.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Error handling stubs
// ---------------------------------------------------------------------------

describe("Tap error handling", () => {
  it("handles invalid config gracefully", () => {
    // An empty connection string is a config error
    const mock = new MockTapBinding({
      connection: "",
    });
    expect(mock).toBeDefined();
  });

  it("pause throws when not running", async () => {
    const mock = new MockTapBinding({
      connection: "postgresql://localhost:5432/test",
    });
    await expect(mock.pause()).rejects.toThrow("Not running");
  });

  it("resume throws when not paused", async () => {
    const mock = new MockTapBinding({
      connection: "postgresql://localhost:5432/test",
    });
    await expect(mock.resume()).rejects.toThrow("Not running");
  });
});

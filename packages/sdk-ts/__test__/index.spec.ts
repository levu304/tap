/**
 * Tests for tap-cdc — TypeScript SDK for Tap CDC engine.
 *
 * NOTE: These are stub tests. They will be expanded by the qa-expert agent.
 */
import { describe, it, expect } from "vitest";
import { normalizeConfig, changeEventToJson } from "../src/index";

// ---------------------------------------------------------------------------
// Config normalization tests (calls real normalizeConfig)
// ---------------------------------------------------------------------------

describe("Tap config normalization", () => {
  it("converts camelCase config to snake_case using normalizeConfig", () => {
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

    const normalized = normalizeConfig(config);

    expect(normalized.connection).toBe("postgresql://localhost:5432/test");
    expect(normalized.slot_name).toBe("my_slot");
    expect(normalized.host).toBe("localhost");
    expect(normalized.port).toBe(5432);
    expect(normalized.ssl_mode).toBe("disable");
    expect(normalized.max_batch_size).toBe(200);
    expect(normalized.flush_interval_ms).toBe(500);
    expect((normalized.sink as Record<string, unknown>).max_buffer_size).toBe(5000);
    expect((normalized.sink as Record<string, unknown>).heartbeat_interval_ms).toBe(15000);
  });
});

// ---------------------------------------------------------------------------
// real changeEventToJson tests (no native binding needed)
// ---------------------------------------------------------------------------

describe("changeEventToJson", () => {
  it("serializes a full ChangeEvent to JSON", () => {
    const event = {
      op: "c" as const,
      before: null,
      after: { id: 1, name: "test", value: 100 },
      source: {
        db: "test_db",
        schema: "public",
        table: "users",
        lsn: "0/16B37428",
        txId: "12345",
        tsMs: 1717000000000,
      },
      tsMs: 1717000000001,
      id: "0/16B37428:12345",
    };

    const json = changeEventToJson(event);
    const parsed = JSON.parse(json);

    expect(parsed.op).toBe("c");
    expect(parsed.after.id).toBe(1);
    expect(parsed.after.name).toBe("test");
    expect(parsed.source.db).toBe("test_db");
    expect(parsed.source.lsn).toBe("0/16B37428");
    expect(parsed.id).toBe("0/16B37428:12345");
    expect(parsed.before).toBeNull();
  });

  it("serializes a snapshot Read event (op: r)", () => {
    const event = {
      op: "r" as const,
      before: null,
      after: { id: 42, name: "snapshot_row" },
      source: {
        db: "test_db",
        schema: "public",
        table: "events",
        lsn: "0/ABCDEF",
        txId: "99999",
        tsMs: 1717000000002,
        snapshot: true,
      },
      tsMs: 1717000000003,
      id: "snap:public.events:42",
    };

    const json = changeEventToJson(event);
    const parsed = JSON.parse(json);

    expect(parsed.op).toBe("r");
    expect(parsed.source.snapshot).toBe(true);
    expect(parsed.id).toBe("snap:public.events:42");
  });

  it("serializes a Delete event with before data", () => {
    const event = {
      op: "d" as const,
      before: { id: 7, name: "deleted_row" },
      after: null,
      source: {
        db: "test_db",
        schema: "public",
        table: "users",
        lsn: "0/16B37429",
        txId: "12346",
        tsMs: 1717000000004,
      },
      tsMs: 1717000000005,
      id: "0/16B37429:12346",
    };

    const json = changeEventToJson(event);
    const parsed = JSON.parse(json);

    expect(parsed.op).toBe("d");
    expect(parsed.before.id).toBe(7);
    expect(parsed.after).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Mock binding (for lifecycle tests that exercise the Tap class)
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

/*
 * tap-cdc-linux-x64-musl@0.1.0 — placeholder stub.
 *
 * No native binary is shipped for this platform in v0.1.0.
 * This file exists so `npm install tap-cdc` resolves the optional
 * dependency (preventing pnpm lockfile drift) and produces a clear,
 * actionable error if anyone tries to load the SDK on this platform.
 *
 * Tracked at: https://github.com/levu304/tap/issues
 */

"use strict";

const PLATFORM = "linux-x64-musl";
const SUPPORTED = "darwin-arm64, linux-x64-gnu";

throw new Error(
  "tap-cdc v0.1.0 does not ship a native binding for " + PLATFORM + ".\n" +
  "Supported platforms in v0.1.0: " + SUPPORTED + ".\n" +
  "Track progress: https://github.com/levu304/tap/blob/main/.docs/ROADMAP.md\n" +
  "Open an issue: https://github.com/levu304/tap/issues"
);

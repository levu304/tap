/* eslint-disable */
/* tap-cdc — Platform-specific native binding resolver.
 *
 * Loads the correct `.node` binary for the current platform from the
 * platform-specific optional dependency (e.g. `tap-cdc-darwin-arm64`).
 * Falls back to a local development build when the platform package is
 * not installed (e.g. after `napi build --platform`).
 */

const { existsSync, readFileSync } = require("fs");
const { join } = require("path");

const { platform, arch } = process;

let nativeBinding = null;
let loadError = null;

/**
 * Detect whether the system uses musl libc (Alpine Linux et al.).
 *
 * Tries `process.report` first (Node.js 12+), then reads `/usr/bin/ldd`
 * as a fallback.
 */
function isMusl() {
  // Prefer process.report (reliable across Node versions)
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

  // Alpine Linux / musl-based fallback
  try {
    return readFileSync("/usr/bin/ldd", "utf8").includes("musl");
  } catch {
    // Cannot detect — assume glibc
    return false;
  }
}

const musl = platform === "linux" ? isMusl() : false;

// ---------------------------------------------------------------------------
// Try loading the platform-specific optional dependency
// ---------------------------------------------------------------------------
switch (platform) {
  case "darwin":
    switch (arch) {
      case "arm64": {
        try {
          nativeBinding = require("tap-cdc-darwin-arm64");
        } catch (e) {
          loadError = e;
        }
        break;
      }
      case "x64": {
        try {
          nativeBinding = require("tap-cdc-darwin-x64");
        } catch (e) {
          loadError = e;
        }
        break;
      }
      default:
        throw new Error(
          `Unsupported architecture on macOS: ${arch}. ` +
            `tap-cdc supports arm64 and x64. ` +
            `Please file an issue at https://github.com/levu304/tap/issues`,
        );
    }
    break;

  case "linux":
    switch (arch) {
      case "arm64": {
        if (musl) {
          throw new Error(
            "The platform linux-arm64-musl is not supported by tap-cdc. " +
              "Please file an issue at https://github.com/levu304/tap/issues",
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
            `Please file an issue at https://github.com/levu304/tap/issues`,
        );
    }
    break;

  default:
    throw new Error(
      `Unsupported OS: ${platform}, architecture: ${arch}. ` +
        `tap-cdc currently supports macOS (arm64, x64) and Linux (arm64, x64). ` +
        `Please file an issue at https://github.com/levu304/tap/issues`,
    );
}

// ---------------------------------------------------------------------------
// Fallback: local development build
// ---------------------------------------------------------------------------
if (!nativeBinding) {
  // Map platform+arch+libc to the file name produced by `napi build --platform`.
  //   darwin:  tap-sdk.darwin-arm64.node,  tap-sdk.darwin-x64.node
  //   linux:   tap-sdk.linux-arm64-gnu.node,  tap-sdk.linux-x64-gnu.node,
  //            tap-sdk.linux-x64-musl.node
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

// Try generic fallback name (some build tooling uses this convention)
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

// ---------------------------------------------------------------------------
// Final: resolve or error
// ---------------------------------------------------------------------------
if (!nativeBinding) {
  if (loadError) {
    throw loadError;
  }

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
      `  https://github.com/levu304/tap/issues`,
  );
}

module.exports = nativeBinding;

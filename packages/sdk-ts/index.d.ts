/**
 * tap-cdc — Platform-native binding entry point.
 *
 * This file re-exports all public types and the default `Tap` class from
 * the TypeScript source wrapper.  At runtime `index.js` resolves the
 * correct `.node` binary for the current platform.
 *
 * @example
 * ```ts
 * import { Tap, ChangeEvent, TapConfig } from "tap-cdc";
 *
 * const tap = new Tap({ connection: "postgresql://localhost/mydb" });
 * tap.onChange((event: ChangeEvent) => console.log(event.after));
 * await tap.start();
 * ```
 */

// Re-export all public types and functions from the TypeScript wrapper.
export * from "./src/index";

// The default export is the `Tap` class (re-exported from src/index.ts).
export { default } from "./src/index";

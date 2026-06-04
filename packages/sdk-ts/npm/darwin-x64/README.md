# tap-cdc-darwin-x64

Native binding for [tap-cdc](https://github.com/levu304/tap) on **macOS x64 (Intel)**.

## Status: not yet built in v0.1.0

This platform is declared in `tap-cdc@0.1.0` but no native binary is shipped yet. Requiring this package throws an error explaining which platforms are actually supported.

The stub exists so that `npm install tap-cdc` resolves the `optionalDependencies` entry and pnpm's lockfile stays in sync with `package.json`.

## Supported platforms in v0.1.0

Only `darwin-arm64` and `linux-x64-gnu` ship a working native binding.

## What you should do

If you're on Intel macOS and need this platform, please open an issue at https://github.com/levu304/tap/issues. Adding a `darwin-x64` build is on the v0.1.1 roadmap.

## More info

- Repo: https://github.com/levu304/tap
- Roadmap: https://github.com/levu304/tap/blob/main/.docs/ROADMAP.md
- Issues: https://github.com/levu304/tap/issues

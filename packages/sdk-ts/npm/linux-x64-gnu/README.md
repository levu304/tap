# tap-cdc-linux-x64-gnu

Native binding for [tap-cdc](https://github.com/levu304/tap) on **Linux x64 (glibc)** — the most common server platform (Ubuntu, Debian, CentOS, RHEL, Amazon Linux, etc.).

## Status: v0.1.0 placeholder

This package is a **stub** in v0.1.0. It contains no native binary; requiring it throws an error explaining which platforms are actually supported.

The stub exists so that `npm install tap-cdc` resolves the `optionalDependencies` entry and pnpm's lockfile stays in sync with `package.json`.

## Supported platforms in v0.1.0

Only `darwin-arm64` and `linux-x64-gnu` ship a working native binding.

## What you should do

- If you're on **Linux x64 with glibc** (most distros): your platform **is** supported. If you see the "not yet built" error, your install resolved the wrong package — try `rm -rf node_modules package-lock.json && npm install tap-cdc`.
- If you're on **Alpine Linux (musl libc)**: this platform is not yet built. Track [the v0.1.1 milestone](https://github.com/levu304/tap/blob/main/.docs/ROADMAP.md).

## More info

- Repo: https://github.com/levu304/tap
- Roadmap: https://github.com/levu304/tap/blob/main/.docs/ROADMAP.md
- Issues: https://github.com/levu304/tap/issues

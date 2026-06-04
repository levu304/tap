#!/usr/bin/env bash
# ===========================================================================
# Tap release build script
# Cross-compiles the CLI binary for 5 targets, builds the napi-rs SDK for
# the host platform, and packages everything into dist/ with tarballs,
# signatures, and checksums.
#
# Usage: ./scripts/release.sh <version>
# Example: ./scripts/release.sh 0.1.0
#
# Prerequisites (macOS):
#   brew install filosottile/musl-cross/musl-cross   # for x86_64-linux-musl
#   (or use zigbuild: cargo install cargo-zigbuild)
#
# Prerequisites (Linux):
#   apt install gcc-aarch64-linux-gnu musl-tools
# ===========================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------
VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 0.1.0"
    exit 1
fi
if ! echo "$VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+'; then
    echo "Error: version must be in semver format (e.g., 0.1.0)"
    exit 1
fi

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="$REPO_ROOT/dist"
CLI_CRATE="tap-cli"
# The cargo binary name for the package. If package had [[bin]] name = "tap"
# this would be "tap"; the default from package name "tap-cli" is "tap-cli".
BINARY_NAME="tap-cli"
HOST_OS="$(uname -s)"
HOST_ARCH="$(uname -m)"

# Detect checksum command
if command -v sha256sum &>/dev/null; then
    SHASUM="sha256sum"
else
    SHASUM="shasum -a 256"
fi

echo "=== Tap Release v${VERSION} ==="
echo "  Host: ${HOST_OS} ${HOST_ARCH}"
echo "  Repo: ${REPO_ROOT}"
echo ""

# ---------------------------------------------------------------------------
# Target definitions:  triple → output-name
# ---------------------------------------------------------------------------
declare -A TARGETS
TARGETS["aarch64-apple-darwin"]="tap-darwin-arm64"
TARGETS["x86_64-apple-darwin"]="tap-darwin-x64"
TARGETS["aarch64-unknown-linux-gnu"]="tap-linux-arm64-gnu"
TARGETS["x86_64-unknown-linux-gnu"]="tap-linux-x64-gnu"
TARGETS["x86_64-unknown-linux-musl"]="tap-linux-x64-musl"

# Deterministic iteration order
SORTED_TARGETS=(
    "aarch64-apple-darwin"
    "x86_64-apple-darwin"
    "aarch64-unknown-linux-gnu"
    "x86_64-unknown-linux-gnu"
    "x86_64-unknown-linux-musl"
)

# Clean and create dist directory
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

# ===========================================================================
# Step 1 — Install cross-compilation targets
# ===========================================================================
echo "--- Step 1/6: Installing Rust cross-compilation targets ---"
for target in "${SORTED_TARGETS[@]}"; do
    echo "  rustup target add ${target}"
    # Use CARGO_HOME or default; 2>&1 redirect to keep output clean
    rustup target add "$target" 1>/dev/null 2>&1 || echo "  (target may already be installed)"
done

# ===========================================================================
# Step 2 — Cross-compile CLI binary for every target
# ===========================================================================
echo ""
echo "--- Step 2/6: Cross-compiling CLI binary ---"
for target in "${SORTED_TARGETS[@]}"; do
    output_name="${TARGETS[$target]}"
    echo ""
    echo "  Building ${target} → ${output_name} ..."

    # Set RUSTFLAGS for musl
unset RUSTFLAGS
    if [[ "$target" == "x86_64-unknown-linux-musl" ]]; then
        RUSTFLAGS="$RUSTFLAGS -C target-feature=-crt-static"
    fi

    cargo build --release -p "$CLI_CRATE" --target "$target"

    src="$REPO_ROOT/target/$target/release/$BINARY_NAME"
    if [[ ! -f "$src" ]]; then
        echo "  ERROR: Binary not found at $src"
        exit 1
    fi

    cp "$src" "$DIST_DIR/$output_name"
    echo "  ✓ dist/${output_name}"
done

# Reset RUSTFLAGS so subsequent commands aren't affected
unset RUSTFLAGS

# ===========================================================================
# Step 3 — Build napi-rs SDK (host platform only)
# ===========================================================================
echo ""
echo "--- Step 3/6: Building napi-rs SDK ---"
SDK_DIR="$REPO_ROOT/packages/sdk-ts"

# Ensure the SDK package has its dependencies installed
pushd "$SDK_DIR" > /dev/null
if [[ ! -d "node_modules" ]]; then
    echo "  Installing SDK dependencies..."
    pnpm install --frozen-lockfile
fi

echo "  Building napi native binding for host platform..."
# napi build --platform places the .node binary into the correct
# npm/<platform>/ directory based on the current OS/arch.
npx --yes @napi-rs/cli build --platform --release
echo "  Running napi artifacts..."
npx --yes @napi-rs/cli artifacts
echo "  SDK build complete."
popd > /dev/null

# ===========================================================================
# Step 4 — Package artifacts into tarballs
# ===========================================================================
echo ""
echo "--- Step 4/6: Packaging artifacts ---"

# Package each CLI binary (tarball contains a single binary named "tap")
for target in "${SORTED_TARGETS[@]}"; do
    output_name="${TARGETS[$target]}"
    binary_path="$DIST_DIR/$output_name"
    tarball_name="${output_name}.tar.gz"

    echo "  Creating ${tarball_name} ..."
    tar -czf "$DIST_DIR/$tarball_name" -C "$DIST_DIR" "$output_name"
    echo "  ✓ dist/${tarball_name}"
done

# Package SDK platform artifacts (each npm/<platform>/ dir becomes a tarball)
echo ""
echo "  Packaging SDK platform artifacts..."
SDK_NPM_DIR="$SDK_DIR/npm"
for platform_dir in "$SDK_NPM_DIR"/*/; do
    [[ -d "$platform_dir" ]] || continue
    platform_name="$(basename "$platform_dir")"
    tarball_name="sdk-${platform_name}.tar.gz"
    echo "  Creating ${tarball_name} ..."
    tar -czf "$DIST_DIR/$tarball_name" -C "$SDK_NPM_DIR" "$platform_name"
    echo "  ✓ dist/${tarball_name}"
done

# ===========================================================================
# Step 5 — Sign binaries (macOS ad-hoc only; notarization deferred to v0.2+)
# ===========================================================================
echo ""
echo "--- Step 5/6: Signing binaries ---"
if [[ "$HOST_OS" == "Darwin" ]]; then
    for target in "${SORTED_TARGETS[@]}"; do
        output_name="${TARGETS[$target]}"
        binary_path="$DIST_DIR/$output_name"

        # Only sign macOS-native binaries
        if echo "$target" | grep -qE 'apple-darwin$'; then
            echo "  Signing ${output_name} (ad-hoc)..."
            codesign --force -s - "$binary_path"
        fi
    done
else
    echo "  Skipping macOS code signing (not on macOS) — v0.1.0 does not notarize."
fi

# ===========================================================================
# Step 6 — Verify & checksum
# ===========================================================================
echo ""
echo "--- Step 6/6: Verification and checksums ---"

cd "$DIST_DIR"

for target in "${SORTED_TARGETS[@]}"; do
    output_name="${TARGETS[$target]}"
    binary_path="$output_name"
    tarball_name="${output_name}.tar.gz"

    echo ""
    echo "  Checking ${output_name} ..."
    file_output="$(file "$binary_path")"
    echo "    type : $file_output"

    # Checksum binary
    echo "$($SHASUM "$binary_path")" >> SHA256SUMS
    echo "    sha256 : $($SHASUM "$binary_path" | cut -d' ' -f1)"

    # Checksum tarball
    echo "$($SHASUM "$tarball_name")" >> SHA256SUMS

    # Runtime verification for native-platform binaries only
    can_run=false
    case "${HOST_OS}:${HOST_ARCH}" in
        Darwin:arm64)
            [[ "$target" == "aarch64-apple-darwin" ]] && can_run=true
            ;;
        Darwin:x86_64)
            [[ "$target" == "x86_64-apple-darwin" ]] && can_run=true
            ;;
        Linux:x86_64)
            [[ "$target" == "x86_64-unknown-linux-gnu" || "$target" == "x86_64-unknown-linux-musl" ]] && can_run=true
            ;;
        Linux:aarch64)
            [[ "$target" == "aarch64-unknown-linux-gnu" ]] && can_run=true
            ;;
    esac

    if $can_run; then
        echo "    Running version check..."
        version_output="$("./$binary_path" --version 2>&1 || true)"
        echo "    version: ${version_output}"
    else
        echo "    Skipping runtime check (cross-compiled / different arch)"
    fi
done

# Checksum SDK artifact tarballs
for tarball in sdk-*.tar.gz; do
    [[ -f "$tarball" ]] || continue
    echo "$($SHASUM "$tarball")" >> SHA256SUMS
done

cd "$REPO_ROOT"

# ===========================================================================
# Summary
# ===========================================================================
echo ""
echo "=== Release v${VERSION} Summary ==="
echo ""
echo "  Output directory : $DIST_DIR"
echo "  SHA256SUMS       : $DIST_DIR/SHA256SUMS"
echo ""

echo "  CLI binaries:"
for target in "${SORTED_TARGETS[@]}"; do
    output_name="${TARGETS[$target]}"
    tarball_name="${output_name}.tar.gz"
    tarball_path="$DIST_DIR/$tarball_name"
    bin_path="$DIST_DIR/$output_name"
    tarball_size="$(du -h "$tarball_path" | cut -f1)"
    bin_size="$(du -h "$bin_path" | cut -f1)"
    printf "    %-35s %s (%s)  tarball: %s\n" "${target}" "$output_name" "$bin_size" "$tarball_name ($tarball_size)"
done

echo ""
echo "  SDK artifacts:"
for tarball in "$DIST_DIR"/sdk-*.tar.gz; do
    [[ -f "$tarball" ]] || continue
    tarball_size="$(du -h "$tarball" | cut -f1)"
    echo "    $(basename "$tarball") (${tarball_size})"
done

echo ""
echo "=== Done ==="

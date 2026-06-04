#!/bin/sh
# ===========================================================================
# tap installer — downloads the latest CLI binary from GitHub Releases
#
# Usage:
#   curl -fsSL https://levu304.github.io/tap/install.sh | sh
#   curl -fsSL https://levu304.github.io/tap/install.sh | sh -s -- -b /usr/local/bin
#
# Flags:
#   -b <path>    Install binary to <path> (default: $HOME/.local/bin)
#   -v <version> Install a specific version instead of "latest"
#   -h           Print help
#
# Environment variables:
#   TAP_INSTALL_DIR   Same as -b flag (alternative)
#   GITHUB_TOKEN      GitHub API token (avoids rate limiting on CI)
# ===========================================================================
set -e

TAP_REPO="levu304/tap"
BIN_DIR="${TAP_INSTALL_DIR:-${HOME}/.local/bin}"
VERSION="latest"

# ---------------------------------------------------------------------------
# Parse flags
# ---------------------------------------------------------------------------
while getopts "b:v:h" opt; do
  case "$opt" in
    b) BIN_DIR="$OPTARG" ;;
    v) VERSION="$OPTARG" ;;
    h)
      echo "tap installer — https://github.com/levu304/tap"
      echo ""
      echo "Usage: curl -fsSL https://levu304.github.io/tap/install.sh | sh [-- <flag> ...]"
      echo ""
      echo "Flags:"
      echo "  -b <path>    Install binary to <path> (default: \$HOME/.local/bin)"
      echo "  -v <version> Install a specific version (e.g. 0.1.0)"
      echo "  -h           Print this help"
      echo ""
      echo "Environment:"
      echo "  TAP_INSTALL_DIR    Same as -b"
      echo "  GITHUB_TOKEN       GitHub API token to avoid rate limiting"
      exit 0
      ;;
    *) exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Detect platform → asset name
# ---------------------------------------------------------------------------
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$OS" in
  darwin)  TARGET_OS="darwin"     ;;
  linux)   TARGET_OS="linux-gnu"  ;;
  *)
    echo "error: unsupported OS '$OS' (only darwin/linux)"
    exit 1
    ;;
esac

case "$ARCH" in
  x86_64|amd64)  TARGET_ARCH="x64"     ;;
  aarch64|arm64) TARGET_ARCH="arm64"    ;;
  *)
    echo "error: unsupported architecture '$ARCH' (only x86_64/aarch64)"
    exit 1
    ;;
esac

case "$OS" in
  darwin) ASSET_NAME="tap-darwin-${TARGET_ARCH}" ;;
  linux)  ASSET_NAME="tap-linux-${TARGET_ARCH}-gnu" ;;
esac

# ---------------------------------------------------------------------------
# Determine download tool
# ---------------------------------------------------------------------------
if command -v curl >/dev/null 2>&1; then
  download() { curl -fsSL "$@"; }
elif command -v wget >/dev/null 2>&1; then
  download() { wget -qO- "$@"; }
else
  echo "error: need curl or wget"
  exit 1
fi

# sha256sum on Linux, shasum on macOS
if command -v sha256sum >/dev/null 2>&1; then
  SHASUM="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  SHASUM="shasum -a 256"
else
  SHASUM=""
fi

# ---------------------------------------------------------------------------
# Resolve version
# ---------------------------------------------------------------------------
if [ "$VERSION" = "latest" ]; then
  echo "Fetching latest release info..."
  API_OPTS=""
  [ -n "$GITHUB_TOKEN" ] && API_OPTS="-H \"Authorization: Bearer ${GITHUB_TOKEN}\""

  RELEASE_JSON=$(download ${API_OPTS} "https://api.github.com/repos/${TAP_REPO}/releases/latest")
  TAG=$(echo "$RELEASE_JSON" | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')

  if [ -z "$TAG" ]; then
    echo "error: could not determine latest version from GitHub API"
    echo "  Try setting GITHUB_TOKEN (repo public, but rate limits apply without auth)"
    exit 1
  fi
  VERSION="${TAG#v}"
  echo "Latest release: v${VERSION}"
else
  echo "Installing version: v${VERSION}"
fi

# ---------------------------------------------------------------------------
# Locate asset URL in release
# ---------------------------------------------------------------------------
RELEASE_URL="https://github.com/${TAP_REPO}/releases/download/v${VERSION}"

echo "Downloading ${ASSET_NAME} (v${VERSION})..."
TAR_URL="${RELEASE_URL}/${ASSET_NAME}.tar.gz"
SHA_URL="${RELEASE_URL}/SHA256SUMS"

# ---------------------------------------------------------------------------
# Download to temp dir
# ---------------------------------------------------------------------------
TMPDIR=$(mktemp -d /tmp/tap-install.XXXXXX)
trap 'rm -rf "$TMPDIR"' EXIT

download "$TAR_URL" > "$TMPDIR/tap.tar.gz"
echo "  ✓ tarball downloaded"

download "$SHA_URL" > "$TMPDIR/SHA256SUMS"
echo "  ✓ checksum file downloaded"

# ---------------------------------------------------------------------------
# Verify checksum
# ---------------------------------------------------------------------------
if [ -n "$SHASUM" ]; then
  (
    cd "$TMPDIR"
    # Extract the expected line for this asset and verify
    grep "${ASSET_NAME}.tar.gz" SHA256SUMS | $SHASUM -c - >/dev/null 2>&1
  ) && echo "  ✓ checksum verified" || {
    echo "warning: checksum verification failed (continuing anyway)"
  }
else
  echo "warning: no sha256sum/shasum found — skipping checksum verification"
fi

# ---------------------------------------------------------------------------
# Extract and install
# ---------------------------------------------------------------------------
mkdir -p "$BIN_DIR"
tar -xzf "$TMPDIR/tap.tar.gz" -C "$TMPDIR"
mv "$TMPDIR/${ASSET_NAME}" "$BIN_DIR/tap"
chmod +x "$BIN_DIR/tap"

echo ""
echo "✓ Installed tap v${VERSION} to ${BIN_DIR}/tap"

# ---------------------------------------------------------------------------
# PATH check
# ---------------------------------------------------------------------------
case ":${PATH}:" in
  *:"${BIN_DIR}":*)
    echo "  ${BIN_DIR} is on your PATH — run 'tap --help' to get started"
    ;;
  *)
    echo ""
    echo "  ⚠  ${BIN_DIR} is not in your PATH."
    echo "     Add it to your shell profile:"
    echo ""
    echo "        echo 'export PATH=\"\${HOME}/.local/bin:\${PATH}\"' >> ~/.bashrc"
    echo "        source ~/.bashrc"
    echo ""
    echo "     Then run 'tap --help' to get started"
    ;;
esac

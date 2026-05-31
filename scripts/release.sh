#!/usr/bin/env bash
set -euo pipefail

# Tap release script — v0.1.0
# Usage: ./scripts/release.sh <version>
#
# Builds binaries for target platforms, packages npm artifacts,
# and prepares the GitHub release.
#
# TODO: Implement full release pipeline in P11.

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 0.1.0"
    exit 1
fi

echo "=== Tap Release v${VERSION} ==="
echo "Release automation not yet implemented."
echo "See .docs/plans/v0.1.0-implementation-plan.md P11 for details."

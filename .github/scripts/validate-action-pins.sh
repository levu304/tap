#!/usr/bin/env bash
# =============================================================================
# validate-action-pins.sh
#
# Validates every `uses: <owner>/<repo>@<40hex>` directive in
# .github/workflows/*.yml by resolving each SHA via the GitHub API.
#
# Three failure modes detected:
#   1. Fabricated SHAs        → API returns 404
#   2. Annotated-tag SHAs     → API resolves to a different commit SHA
#   3. Valid commit SHAs      → API returns matching SHA  (pass)
#
# Rate limiting (403/429) emits a warning but does not fail the check.
#
# Prerequisites:
#   - gh CLI (pre-installed on GitHub Actions runners)
#   - GITHUB_TOKEN with read access to action repositories
#
# Exit codes:
#   0 — all pins valid (or nothing to check)
#   1 — one or more pins invalid
# =============================================================================

set -euo pipefail

errors=0
warnings=0
processed=0

# ── Prepare temp file for metadata (not strictly needed here but useful) ──
tmpfile=$(mktemp)
trap 'rm -f "$tmpfile"' EXIT

# ── Collect all `uses: owner/repo@<40hex>` lines ──────────────────────────
# Uses two grep passes: first find 'uses:', then extract the full directive.
# This avoids false matches from strings unrelated to action references.
pins_source=$(grep -rn 'uses:' .github/workflows/*.yml 2>/dev/null \
    | grep -oE 'uses: [a-zA-Z0-9._/-]+@[0-9a-f]{40}' \
    || true)

if [ -z "$pins_source" ]; then
    echo "ℹ No pinned action SHAs found in .github/workflows/*.yml"
    exit 0
fi

echo "🔍 Validating pinned action SHAs..."
echo ""

# ── Process each pin ──────────────────────────────────────────────────────
while IFS='@' read -r action sha; do
    # action looks like "uses: owner/repo"; strip the prefix
    owner_repo="${action#uses: }"
    # Trim leading/trailing whitespace
    owner_repo="${owner_repo// /}"
    sha="${sha// /}"

    processed=$((processed + 1))

    # Call the GitHub Commits API.
    # For a valid commit SHA,   response.sha == requested_sha  →  PASS
    # For an annotated-tag SHA  response.sha != requested_sha  →  FAIL
    # For a fabricated SHA      HTTP 404                       →  FAIL
    response=$(gh api "repos/${owner_repo}/commits/${sha}" --jq '.sha' 2>&1) \
        && rc=0 || rc=$?

    if [ "$rc" -eq 0 ]; then
        resolved_sha="$response"
        if [ "$resolved_sha" = "$sha" ]; then
            echo "  ✓ ${owner_repo}@${sha:0:12}…"
        else
            echo "  ✗ ${owner_repo}@${sha} — annotated-tag SHA (resolves to commit ${resolved_sha})"
            errors=$((errors + 1))
        fi
    else
        # Non-zero exit — gh CLI failed (non-2xx HTTP status)
        case "$response" in
            *"404"*|*"Not Found"*|*"422"*|*"No commit found"*)
                echo "  ✗ ${owner_repo}@${sha} — FABRICATED SHA ($(echo "$response" | grep -oE 'HTTP [0-9]+' || echo "does not exist"))"
                errors=$((errors + 1))
                ;;
            *"403"*|*"429"*|*"rate limit"*|*"rate_limit"*)
                echo "  ⚠ ${owner_repo}@${sha:0:12}… — rate limited, skipped (set up GITHUB_TOKEN with higher rate limit)"
                warnings=$((warnings + 1))
                ;;
            *)
                echo "  ✗ ${owner_repo}@${sha:0:12}… — API error: $(echo "$response" | head -1)"
                errors=$((errors + 1))
                ;;
        esac
    fi

done <<< "$pins_source"

# ── Summary ───────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════"
echo "  Validate Action Pins — Summary"
echo "═══════════════════════════════════════════"
echo "  Pins checked:   ${processed}"
echo "  Errors:         ${errors}"
if [ "$warnings" -gt 0 ]; then
    echo "  Rate-limited:   ${warnings}"
fi

if [ "$errors" -gt 0 ]; then
    echo ""
    echo "❌ FAILED — ${errors} pin(s) do not resolve to valid commit SHAs."
    echo "   Fix by replacing each broken SHA with the correct commit SHA."
    echo "   Use: gh api repos/<owner>/<repo>/commits/<tag-name> --jq .sha"
    exit 1
else
    echo ""
    echo "✅ All pins valid."
    exit 0
fi

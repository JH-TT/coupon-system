#!/usr/bin/env bash

set -euo pipefail

CONTAINER_NAME="${1:-coupon-nginx}"
TAIL_LINES="${2:-1000}"

LOGS=$(docker logs --tail "$TAIL_LINES" "$CONTAINER_NAME" 2>&1 || true)

if [ -z "$LOGS" ]; then
  echo "No logs found. container=$CONTAINER_NAME tail=$TAIL_LINES"
  exit 0
fi

echo "[1/3] Upstream distribution (ua)"
printf '%s\n' "$LOGS" \
  | sed -n 's/.*ua="\([^"]*\)".*/\1/p' \
  | sort \
  | uniq -c \
  | sort -nr

echo
echo "[2/3] Upstream + status distribution"
printf '%s\n' "$LOGS" \
  | awk '
      {
        split($0, q, "\"");
        split(q[3], s);
        status=s[1];
        if (match($0, /ua="[^"]+"/)) {
          ua=substr($0, RSTART+4, RLENGTH-5);
          print ua " " status;
        }
      }
    ' \
  | sort \
  | uniq -c \
  | sort -nr

echo
echo "[3/3] 409-only upstream distribution"
printf '%s\n' "$LOGS" \
  | grep ' us="409"' \
  | sed -n 's/.*ua="\([^"]*\)".*/\1/p' \
  | sort \
  | uniq -c \
  | sort -nr || true

echo
echo "Done. container=$CONTAINER_NAME tail=$TAIL_LINES"
echo "Usage: bash scripts/lb-report.sh [container_name] [tail_lines]"

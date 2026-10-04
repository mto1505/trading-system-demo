#!/usr/bin/env bash
# Đăng ký connector sau khi Kafka Connect đã UP (chờ ~30s sau docker compose up)
set -euo pipefail
CONNECT=${CONNECT_URL:-http://localhost:8083}

for f in "$(dirname "$0")"/*-connector.json; do
  echo "Dang ky $(basename "$f")..."
  curl -sS -X POST -H 'Content-Type: application/json' --data @"$f" "$CONNECT/connectors" | jq . || true
done

echo "--- Trang thai connector ---"
curl -sS "$CONNECT/connectors?expand=status" | jq . || true

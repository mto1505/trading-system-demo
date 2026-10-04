#!/usr/bin/env bash
# Đặc quyền tối thiểu: mỗi service chỉ được đúng thứ nó cần.
set -euo pipefail
BS=${BOOTSTRAP:-kafka:9092}
CMD="kafka-acls.sh --bootstrap-server $BS --command-config /etc/kafka/admin.properties"

# --- market-gateway: chỉ ghi tick ---
$CMD --add --allow-principal User:market-gateway --operation Write --operation Describe --topic stock.ticks

# --- pricing-risk-service ---
$CMD --add --allow-principal User:pricing-risk-service --operation Read --operation Describe --topic stock.ticks
$CMD --add --allow-principal User:pricing-risk-service --operation Read --topic stock.ticks --group pricing-risk-group
$CMD --add --allow-principal User:pricing-risk-service --operation Write --operation Describe --topic stock.enriched
$CMD --add --allow-principal User:pricing-risk-service --operation Write --operation Describe --topic stock.ticks.DLT

# --- order-service ---
$CMD --add --allow-principal User:order-service --operation Read --operation Describe --topic stock.enriched
$CMD --add --allow-principal User:order-service --operation Read --topic stock.enriched --group order-service-group
$CMD --add --allow-principal User:order-service --operation Read --operation Describe --topic stock.executions
$CMD --add --allow-principal User:order-service --operation Read --topic stock.executions --group order-service-exec-group
$CMD --add --allow-principal User:order-service --operation Write --operation Describe --topic stock.orders

# --- matching-engine ---
$CMD --add --allow-principal User:matching-engine --operation Read --operation Describe --topic stock.orders
$CMD --add --allow-principal User:matching-engine --operation Read --topic stock.orders --group matching-engine-group
$CMD --add --allow-principal User:matching-engine --operation Write --operation Describe --topic stock.executions

echo "--- ACL hien tai ---"
$CMD --list

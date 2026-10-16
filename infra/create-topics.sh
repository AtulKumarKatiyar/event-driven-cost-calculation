#!/bin/bash
set -euo pipefail
for topic in cost.calculation.requested.v1 cost.calculation.lifecycle.v1 cost.calculation.final.v1 cost.first.requested.v1 cost.first.result.v1 cost.freight.requested.v1 cost.freight.result.v1 cost.handling.requested.v1 cost.handling.result.v1 cost.inventory.requested.v1 cost.inventory.result.v1 cost.calculation.requested.v1.dlt cost.calculation.lifecycle.v1.dlt cost.calculation.final.v1.dlt cost.first.requested.v1.dlt cost.first.result.v1.dlt cost.freight.requested.v1.dlt cost.freight.result.v1.dlt cost.handling.requested.v1.dlt cost.handling.result.v1.dlt cost.inventory.requested.v1.dlt cost.inventory.result.v1.dlt cost.calculation.aggregation.dlt; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done

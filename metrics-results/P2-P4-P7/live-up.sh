#!/usr/bin/env bash
# Throwaway Kafka 3.9 + FDB 8.0 (native CDC) on a private network. Arg: worktree to mount at /repo (a worktree, never your main checkout).
set -euo pipefail
REPO=$(cd "$1" && pwd); NET=kfsbench-net
SP=$REPO/.wayfinder/concurrent-cdc/research/stage-profile
docker network inspect $NET >/dev/null 2>&1 || docker network create $NET
docker rm -f -v kfsbench-kafka >/dev/null 2>&1 || true
docker run -d --name kfsbench-kafka --network $NET --network-alias kfsbench-kafka \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:19092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kfsbench-kafka:19092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_NUM_PARTITIONS=1 apache/kafka:3.9.0 >/dev/null
for _ in $(seq 60); do
  docker exec kfsbench-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kfsbench-kafka:19092 --list >/dev/null 2>&1 && break; sleep 2
done
# start-cluster.sh hardcodes devcontainer_devnet; make it overridable in this worktree's .wayfinder copy only (git-excluded).
sed -i '' 's/--network devcontainer_devnet/--network ${KFS_NET:-devcontainer_devnet}/' "$SP/start-cluster.sh"
KFS_NET=$NET bash "$SP/start-cluster.sh" kfsbench-fdb
docker exec kfsbench-fdb python3 -c "import confluent_kafka as c, google.protobuf as p; print(c.version(), c.libversion(), p.__version__)"

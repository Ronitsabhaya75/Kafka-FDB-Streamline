#!/usr/bin/env bash
# Remove the throwaway broker, FDB and network created by live-up.sh. Touches nothing else.
docker rm -f -v kfsbench-fdb kfsbench-kafka >/dev/null 2>&1 || true
docker network rm kfsbench-net >/dev/null 2>&1 || true

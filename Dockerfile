# ============================================================
# Kafka-FDB-Streamline: FDB 8.0 + CDC Python Bindings
# ============================================================
# Builds fdbserver, fdbcli, and libfdb_c from Trevor's
# python-native-cdc-bindings branch, rebased on latest main.
#
# Usage:
#   docker build -t fdb-cdc .
#   docker run -it fdb-cdc bash
# ============================================================

FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# ---------- 1. Build dependencies ----------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential clang cmake ninja-build git \
    libssl-dev liblz4-dev zlib1g-dev pkg-config \
    libjemalloc-dev \
    libc++-dev libc++abi-dev \
    python3 python3-dev python3-pip python3-venv \
    curl ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Use clang as the default compiler (matches FDB CI)
ENV CC=clang CXX=clang++

# ---------- 2. Clone Trevor's CDC branch ----------
RUN git clone --branch dev/tclinkenbeard/python-native-cdc-bindings \
    --single-branch --depth=50 \
    https://github.com/tclinkenbeard-oai/foundationdb.git /fdb

WORKDIR /fdb


# Rebase onto latest main for Boost / build fixes
ENV GIT_COMMITTER_NAME="docker" GIT_COMMITTER_EMAIL="docker@build" GIT_EDITOR=true
RUN git remote add upstream https://github.com/apple/foundationdb.git \
 && git fetch --depth=200 upstream main \
 && git rebase upstream/main \
    || (git checkout --theirs design/cdc.md \
        && git add design/cdc.md \
        && git rebase --continue)

# ---------- 3. Build FDB (only the targets we need) ----------
RUN mkdir build \
 && cd build \
 && cmake -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_C_COMPILER=clang \
      -DCMAKE_CXX_COMPILER=clang++ \
      -DCMAKE_CXX_FLAGS="-stdlib=libc++" \
      .. \
 && ninja fdbserver fdbcli fdb_c

# ---------- 4. Install Python CDC bindings ----------
RUN cd /fdb/bindings/python && pip3 install --break-system-packages -e .

# ============================================================
# Runtime image (smaller, no build toolchain)
# ============================================================
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv \
    libssl3t64 liblz4-1 zlib1g \
    ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*

# ---------- 5. Copy built binaries ----------
# Copy built binaries
COPY --from=builder /fdb/build/bin/fdbserver /usr/local/bin/
COPY --from=builder /fdb/build/bin/fdbcli    /usr/local/bin/
COPY --from=builder /fdb/build/lib/libfdb_c.so /usr/local/lib/

# Copy Python bindings source + install
COPY --from=builder /fdb/bindings/python /opt/fdb-python
RUN cd /opt/fdb-python && pip3 install --break-system-packages -e .

# Library + FDB config
ENV LD_LIBRARY_PATH=/usr/local/lib
RUN ldconfig

# Create default FDB data/log/config dirs
RUN mkdir -p /var/lib/foundationdb/data \
             /var/log/foundationdb \
             /etc/foundationdb

# Default cluster file (single-node dev setup)
RUN echo "docker:docker@127.0.0.1:4500" > /etc/foundationdb/fdb.cluster
ENV FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster

EXPOSE 4500

# Entrypoint: start fdbserver in background, drop into bash
COPY <<'EOF' /usr/local/bin/start-fdb.sh
#!/bin/bash
set -e

echo "Starting fdbserver..."
fdbserver \
  -p auto:4500 \
  -C /etc/foundationdb/fdb.cluster \
  -d /var/lib/foundationdb/data \
  -L /var/log/foundationdb \
  --knob_enable_native_cdc=1 &

sleep 2

echo "Configuring new database..."
fdbcli --exec "configure new single memory ; status" 2>/dev/null || true

echo ""
echo "============================================"
echo "  FDB 8.0 + CDC ready!"
echo "  Python:  import fdb; fdb.api_version(800)"
echo "  CLI:     fdbcli"
echo "============================================"
echo ""

exec "$@"
EOF
RUN chmod +x /usr/local/bin/start-fdb.sh

ENTRYPOINT ["/usr/local/bin/start-fdb.sh"]
CMD ["bash"]

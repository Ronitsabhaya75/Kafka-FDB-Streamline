FROM --platform=linux/amd64 ubuntu:24.04 AS runtime

ARG FDB_VERSION=8.0.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    python3 python3-pip python3-venv \
    libssl3t64 liblz4-1 zlib1g \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/lib/foundationdb/data \
    /var/log/foundationdb \
    /etc/foundationdb

# Download prebuilt FoundationDB 8.0.0 binaries and client library
RUN ARCH="$(uname -m)" && \
    case "$ARCH" in \
      x86_64)  FDB_ARCH="x86_64" ;; \
      aarch64) FDB_ARCH="aarch64" ;; \
      *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;; \
    esac && \
    curl -fsSL -o /usr/local/bin/fdbserver "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/fdbserver.${FDB_ARCH}" && \
    curl -fsSL -o /usr/local/bin/fdbcli "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/fdbcli.${FDB_ARCH}" && \
    curl -fsSL -o /usr/local/lib/libfdb_c.so "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/libfdb_c.${FDB_ARCH}.so" && \
    chmod +x /usr/local/bin/fdbserver /usr/local/bin/fdbcli && \
    ldconfig

# Install official 8.0.0 Python bindings with CDC support from PyPI
RUN pip3 install --break-system-packages --no-cache-dir foundationdb==${FDB_VERSION}

ENV FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
ENV LD_LIBRARY_PATH=/usr/local/lib

COPY <<'EOF' /usr/local/bin/start-fdb.sh
#!/bin/bash
set -e

if [ ! -f "$FDB_CLUSTER_FILE" ] || [ ! -s "$FDB_CLUSTER_FILE" ]; then
    echo "${FDB_CLUSTER_STRING:-docker:docker@127.0.0.1:4500}" > "$FDB_CLUSTER_FILE"
fi

echo "Starting fdbserver in auto-restart loop..."
(
  while true; do
    fdbserver \
      -p auto:4500 \
      -C /etc/foundationdb/fdb.cluster \
      -d /var/lib/foundationdb/data \
      -L /var/log/foundationdb \
      --knob_enable_native_cdc=1 || true
    echo "fdbserver exited. Restarting in 1s..."
    sleep 1
  done
) &

sleep 2

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
RUN chmod +x /usr/local/bin/start-fdb.sh && \
    echo 'export PS1="\[\e[1;36m\]🐳 [fdb-dev]\[\e[m\] \[\e[1;34m\]\w\[\e[m\] \[\e[1;32m\]#\[\e[m\] "' >> /etc/bash.bashrc && \
    echo 'export PS1="\[\e[1;36m\]🐳 [fdb-dev]\[\e[m\] \[\e[1;34m\]\w\[\e[m\] \[\e[1;32m\]#\[\e[m\] "' >> /root/.bashrc

WORKDIR /workspace

EXPOSE 4500

ENTRYPOINT ["/usr/local/bin/start-fdb.sh"]
CMD ["sleep", "infinity"]

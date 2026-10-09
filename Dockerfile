ARG BASE_IMAGE=ubuntu:24.04
FROM --platform=linux/amd64 ${BASE_IMAGE}

ARG FDB_VERSION=8.0.0
ARG FDB_RELEASE_URL=https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}
# sha256 of the x86_64 release assets; pinned to prevent tampered or broken binaries.
ARG FDBSERVER_SHA256=9ca9b1514650c3085eaf9a82f329d4dcc036a73ab2bb1fc9696ae2aa72033063
ARG FDBCLI_SHA256=912aeaa01bb25392c6460b0450d2905245cccc58863e658d171216b12e02f2ba
ARG LIBFDB_C_SHA256=91ebcb6ca043b11cb840c95037699604674bce584dd71bd7b4e8bfcdacbed022

# Install dependencies across Debian/Ubuntu (apt-get) and RHEL/CentOS (dnf)
RUN if command -v apt-get >/dev/null 2>&1; then \
        apt-get update && apt-get install -y --no-install-recommends \
            curl ca-certificates \
            python3 python3-pip python3-venv \
            libssl3t64 liblz4-1 zlib1g \
            && rm -rf /var/lib/apt/lists/*; \
    elif command -v dnf >/dev/null 2>&1; then \
        dnf install -y python3.12 python3.12-pip openssl-libs lz4-libs zlib && \
        dnf clean all && \
        ln -sf /usr/bin/python3.12 /usr/bin/python3 && \
        ln -sf /usr/bin/pip3.12 /usr/bin/pip3; \
    fi

RUN mkdir -p /var/lib/foundationdb/data \
    /var/log/foundationdb \
    /etc/foundationdb

# Download prebuilt FoundationDB 8.0.0 x86_64 binaries and client library
RUN curl -fsSL -o /usr/local/bin/fdbserver "${FDB_RELEASE_URL}/fdbserver.x86_64" && \
    curl -fsSL -o /usr/local/bin/fdbcli "${FDB_RELEASE_URL}/fdbcli.x86_64" && \
    curl -fsSL -o /usr/local/lib/libfdb_c.so "${FDB_RELEASE_URL}/libfdb_c.x86_64.so" && \
    printf '%s  %s\n' \
      "$FDBSERVER_SHA256" /usr/local/bin/fdbserver \
      "$FDBCLI_SHA256" /usr/local/bin/fdbcli \
      "$LIBFDB_C_SHA256" /usr/local/lib/libfdb_c.so \
      | sha256sum -c - && \
    chmod +x /usr/local/bin/fdbserver /usr/local/bin/fdbcli && \
    echo "/usr/local/lib" > /etc/ld.so.conf.d/fdb.conf && \
    ldconfig


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
    ([ -f /etc/bash.bashrc ] && echo 'export PS1="\[\e[1;36m\]🐳 [fdb-dev]\[\e[m\] \[\e[1;34m\]\w\[\e[m\] \[\e[1;32m\]#\[\e[m\] "' >> /etc/bash.bashrc || true) && \
    echo 'export PS1="\[\e[1;36m\]🐳 [fdb-dev]\[\e[m\] \[\e[1;34m\]\w\[\e[m\] \[\e[1;32m\]#\[\e[m\] "' >> /root/.bashrc

WORKDIR /workspace

EXPOSE 4500

ENTRYPOINT ["/usr/local/bin/start-fdb.sh"]
CMD ["sleep", "infinity"]

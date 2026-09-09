FROM --platform=linux/amd64 foundationdb/build:rockylinux9-latest AS builder

WORKDIR /build

RUN git clone https://github.com/tclinkenbeard-oai/foundationdb.git /fdb && \
    cd /fdb && \
    git checkout dev/tclinkenbeard/python-native-cdc-bindings
RUN cd /fdb && \
    mkdir build && \
    cd build && \
    cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DUSE_JEMALLOC=OFF \
    -DCMAKE_POLICY_DEFAULT_CMP0028=OLD \
    .. && \
    ninja fdbserver fdbcli fdb_c fdb_python && \
    find /fdb/build -name "fdboptions.py" -ls && \
    find /fdb/build -name "fdboptions.py" -exec cp {} /fdb/build/bindings/python/fdb/fdboptions.py \; 2>/dev/null; \
    if [ ! -f /fdb/build/bindings/python/fdb/fdboptions.py ]; then \
      echo "fdboptions.py not found via find, trying vexillographer generation..." && \
      python3 /fdb/bindings/python/fdb/fdboptions.py.cmake /fdb/fdbclient/vexillographer/fdb.options \
        > /fdb/build/bindings/python/fdb/fdboptions.py 2>/dev/null || true; \
    fi && \
    ls -la /fdb/build/bindings/python/fdb/

FROM --platform=linux/amd64 ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv \
    libssl3t64 liblz4-1 zlib1g \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/lib/foundationdb/data \
    /var/log/foundationdb \
    /etc/foundationdb \
    /opt/fdb-python

COPY --from=builder /fdb/build/bin/fdbserver /usr/local/bin/
COPY --from=builder /fdb/build/bin/fdbcli /usr/local/bin/
COPY --from=builder /fdb/build/lib/libfdb_c.so /usr/local/lib/
COPY --from=builder /fdb/build/bindings/python /opt/fdb-python

RUN chmod +x /usr/local/bin/fdbserver /usr/local/bin/fdbcli

RUN ldconfig

RUN cd /opt/fdb-python && pip3 install --break-system-packages --verbose .

RUN echo "docker:docker@127.0.0.1:4500" > /etc/foundationdb/fdb.cluster

ENV FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
ENV LD_LIBRARY_PATH=/usr/local/lib

COPY <<'EOF' /usr/local/bin/start-fdb.sh
#!/bin/bash
set -e

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

echo "Configuring new database..."
fdbcli --exec "configure new single memory ; configure native_cdc_enabled ; status" 2>/dev/null || true

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

FROM ubuntu:22.04 AS builder
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends       wget ca-certificates &&     wget -q https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-jammy.deb &&     apt-get install -y ./apache-arrow-apt-source-latest-jammy.deb &&     apt-get update
RUN apt-get install -y --no-install-recommends       build-essential pkg-config       libarrow-dev libparquet-dev       libmosquitto-dev libyaml-cpp-dev libsimdjson-dev &&     rm -rf /var/lib/apt/lists/*
WORKDIR /build
ARG GIT_HASH=unknown
COPY parquet_writer.cpp mqtt_ring.h Makefile ./
RUN make parquet_writer GIT_HASH="$GIT_HASH"
RUN ldd parquet_writer

FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends       wget ca-certificates &&     wget -q https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-jammy.deb &&     apt-get install -y ./apache-arrow-apt-source-latest-jammy.deb &&     apt-get update &&     apt-get install -y --no-install-recommends       libparquet2300 libmosquitto1 libyaml-cpp0.7 libsimdjson9 &&     rm -rf /var/lib/apt/lists/* /tmp/*.deb
COPY --from=builder /build/parquet_writer /usr/local/bin/parquet_writer
COPY entrypoint.sh /entrypoint.sh
COPY README.k3s.md /README.k3s.md
COPY site.yaml /site.yaml
COPY requirements.txt /python/requirements.txt
COPY *.py /python/
RUN chmod +x /entrypoint.sh
# Default MQTT broker — overridden at runtime by the MQTT_HOST secret
ARG DEFAULT_MQTT_HOST=localhost
ENV DEFAULT_MQTT_HOST=${DEFAULT_MQTT_HOST}
EXPOSE 8771
ENTRYPOINT ["/entrypoint.sh"]

# Build context is the fiducia.cloud root. Cross-repository path dependencies
# are fetched at reviewed commits rather than copied from a moving checkout.
FROM rust:1.97.0-bookworm@sha256:8fa55b2f3ddf97471ab6a767bfa3f37e6bad0986ba823e75fea57e2a2a5c3073 AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /workspace
ARG INTERFACES_REF=6e20a3f4df2e52b99a0ad6add83d4528262b5dbc
ARG CLIENTS_REF=5695b16a1577aadbfe414123927e45927f88a7f0
ARG MESSAGING_REF=cec4ea4f54162758858c6c284324c34a42f3f3d7
ARG TELEMETRY_REF=20ed56d9e725c9189deb7386a2dee91ea8b25fdb
RUN git init fiducia-interfaces \
    && git -C fiducia-interfaces remote add origin https://github.com/fiducia-cloud/fiducia-interfaces.git \
    && git -C fiducia-interfaces fetch --depth 1 origin "$INTERFACES_REF" \
    && git -C fiducia-interfaces checkout --detach FETCH_HEAD \
    && test "$(git -C fiducia-interfaces rev-parse HEAD)" = "$INTERFACES_REF"
RUN git init fiducia-clients \
    && git -C fiducia-clients remote add origin https://github.com/fiducia-cloud/fiducia-clients.git \
    && git -C fiducia-clients fetch --depth 1 origin "$CLIENTS_REF" \
    && git -C fiducia-clients checkout --detach FETCH_HEAD \
    && test "$(git -C fiducia-clients rev-parse HEAD)" = "$CLIENTS_REF"
RUN git init fiducia-messaging.rs \
    && git -C fiducia-messaging.rs remote add origin https://github.com/fiducia-cloud/fiducia-messaging.rs.git \
    && git -C fiducia-messaging.rs fetch --depth 1 origin "$MESSAGING_REF" \
    && git -C fiducia-messaging.rs checkout --detach FETCH_HEAD \
    && test "$(git -C fiducia-messaging.rs rev-parse HEAD)" = "$MESSAGING_REF"
RUN git init fiducia-telemetry.rs \
    && git -C fiducia-telemetry.rs remote add origin https://github.com/fiducia-cloud/fiducia-telemetry.rs.git \
    && git -C fiducia-telemetry.rs fetch --depth 1 origin "$TELEMETRY_REF" \
    && git -C fiducia-telemetry.rs checkout --detach FETCH_HEAD \
    && test "$(git -C fiducia-telemetry.rs rev-parse HEAD)" = "$TELEMETRY_REF"
COPY fiducia-ai-agent-manager.rs/ fiducia-ai-agent-manager.rs/
RUN cargo build --release --locked --manifest-path fiducia-ai-agent-manager.rs/Cargo.toml

# The runtime image needs git + gh + the agent CLIs on PATH in production; this
# base keeps Git/SSH/gh + certs. Layer your agent toolchain on top. This is an
# explicit tooling-runtime exception to the otherwise-distroless service policy: the
# worker must spawn Git and provider CLIs, but it still runs without root.
FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818
LABEL org.fiducia.runtime-profile="tool-runner-nonroot"
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates gh git openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 nonroot \
    && useradd --uid 65532 --gid 65532 --home-dir /home/nonroot --create-home \
        --shell /usr/sbin/nologin nonroot \
    && install -d -o 65532 -g 65532 /home/node/workspace/repo /home/node/workspace/outputs
COPY --from=build --chown=65532:65532 /workspace/fiducia-ai-agent-manager.rs/target/release/fiducia-ai-agent-manager /usr/local/bin/fiducia-ai-agent-manager
ENV HOME=/home/nonroot
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/fiducia-ai-agent-manager"]

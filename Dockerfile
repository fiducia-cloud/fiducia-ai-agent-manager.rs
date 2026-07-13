# Build context is the fiducia.cloud root. Cross-repository path dependencies
# are fetched at reviewed commits rather than copied from a moving checkout.
FROM rust:1.95.0-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /workspace
ARG INTERFACES_REF=bbd8b52ce729ec34b0a9bff4dda6d0a448181797
ARG CLIENTS_REF=051b332843fb005006be0d564e98ba46b825785c
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
COPY fiducia-ai-agent-manager.rs/ fiducia-ai-agent-manager.rs/
RUN cargo build --release --locked --manifest-path fiducia-ai-agent-manager.rs/Cargo.toml

# The runtime image needs git + gh + the agent CLIs on PATH in production; this
# base keeps Git/SSH/gh + certs. Layer your agent toolchain on top. This is an
# explicit tooling-runtime exception to the otherwise-distroless service policy: the
# worker must spawn Git and provider CLIs, but it still runs without root.
FROM debian:bookworm-slim
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

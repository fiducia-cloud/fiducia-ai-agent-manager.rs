# Build context is the fiducia.cloud root (path deps to sibling crates). The
# locked dependency set requires Rust 1.88 or newer.
FROM rust:1.88-bookworm AS build
WORKDIR /workspace
COPY fiducia-interfaces/ fiducia-interfaces/
COPY fiducia-clients/ fiducia-clients/
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

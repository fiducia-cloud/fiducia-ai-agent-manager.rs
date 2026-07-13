# Build context is the fiducia.cloud root (path deps to sibling crates).
FROM rust:1.85-bookworm AS build
WORKDIR /workspace
COPY fiducia-interfaces/ fiducia-interfaces/
COPY fiducia-clients/ fiducia-clients/
COPY fiducia-ai-agent-manager.rs/ fiducia-ai-agent-manager.rs/
RUN cargo build --release --locked --manifest-path fiducia-ai-agent-manager.rs/Cargo.toml

# The runtime image needs git + gh + the agent CLIs on PATH in production; this
# base keeps git + certs. Layer your agent toolchain on top.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /workspace/fiducia-ai-agent-manager.rs/target/release/fiducia-ai-agent-manager /app
EXPOSE 8080
ENTRYPOINT ["/app"]

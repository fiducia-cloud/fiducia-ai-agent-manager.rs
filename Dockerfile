# Build context is the fiducia.cloud root. Cross-repository path dependencies
# are fetched at reviewed commits rather than copied from a moving checkout.
FROM rust:1.97.0-bookworm@sha256:7d0723df719e7f213b69dc7c8c595985c3f4b060cfbee4f7bc0e347a86fe3b6a AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /workspace
ARG INTERFACES_REF=487e470c45ab5851e8f6f3b1dc048fe067fbf408
ARG CLIENTS_REF=bcf2f868697a96d82151c0e4bf0efae258b234e9
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
FROM debian:bookworm-slim@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df
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

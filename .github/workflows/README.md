# Workflows

Continuous-integration workflows for formatting, tests, audits, and container
hardening. Keep actions commit-pinned and Cargo commands locked; delivery or
telemetry outages must not be hidden with fail-open workflow steps.

CI checks out clients, interfaces, messaging, and telemetry at the same
immutable revisions verified by the Dockerfile so sibling path dependencies do
not accidentally follow a moving branch.

## Security baseline

Every executable workflow uses explicit least-privilege permissions, immutable
third-party action or container references, non-persisted checkout credentials,
concurrency control, and a job timeout. The main CI workflow validates this
directory with the digest-pinned actionlint container. Environment mutation is
forbidden unless this README documents a repository-specific platform exception.

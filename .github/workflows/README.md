# Workflows

Continuous-integration workflows for formatting, tests, audits, and container
hardening. Keep actions commit-pinned and Cargo commands locked; delivery or
telemetry outages must not be hidden with fail-open workflow steps.

CI checks out clients, interfaces, messaging, and telemetry at the same
immutable revisions verified by the Dockerfile so sibling path dependencies do
not accidentally follow a moving branch.

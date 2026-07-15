# Agent manager source

The service is split into HTTP/task orchestration, governed control-plane
authority, local replay/log storage, and NATS delivery. `fiducia-node` fencing
decides who may mutate external systems; NATS is only event delivery. New
modules should emit structured tracing and expose operational failures rather
than discard them.

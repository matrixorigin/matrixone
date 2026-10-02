# Retire transaction data trace — revision 1

Owner: engineering-quality issue #29249; implementation: separate retirement PR (link recorded on delivery). Base: 529ee099e32. This design implements the user's decision to completely retire `pkg/txn/trace` runtime while preserving catalog data and configuration parsing.

## Problem and existing owners

CN construction captures an absent trace service in its transaction-open callback before bootstrap registers the real service. Dynamic read/write/action probes remain, yielding incomplete traces. Every CN still creates four million-slot event queues and seven tasks. Enabled row-data serialization, blocking channel sends, CSV writes and retrying uploads/imports add business-path backpressure and failure coupling. Bootstrap owns debug-table creation; the retained definitions are also consumed by historical bootstrap entries and system-database protection. Ordinary spans, statement statistics, Explain/analyzers, reader summaries and transaction correctness/recovery have separate owners.

## Decision and invariants

Retire the runtime completely: remove service construction/registration/close, transaction callback attachment, every `GetService` probe, trace-only clocks/sequence lookups/formatting, channels/workers/filters/loaders and exclusive tests. Keep no dummy service, fallback, disabled branch or replacement collector. Remove trace-only late-materialization fallback; preserve the independent reader-summary fallback and all performance counters and transaction semantics.

Preserve `mo_debug` table names, column types, initialization SQL and existing rows. Keep the existing bootstrap definitions as a catalog-only package; no operational API remains there. Normal startup never reads trace feature/filter state, writes events, imports CSV, uploads trace objects, or removes existing trace files/directories. This change issues no DROP/TRUNCATE/DELETE or migration. Ordinary SQL may still query historical rows under existing authorization. Existing bootstrap entries remain for catalog compatibility, with their SQL unchanged.

Keep all existing `Txn.Trace` TOML fields and values parseable, including enable=true, custom paths, capacities and load-to-mo. They are explicitly deprecated and inert: no normalization for a trace directory/capacity, no path resolution/validation, no resource allocation or I/O. Remove internal wiring and the trace-only Go option. `mo_ctl('cn','txn-trace',...)` retains its command dispatch solely to return an explicit NotSupported retirement error for every argument; it never reports OK or reads/mutates debug tables.

Close/start/failed-bootstrap behavior continues through existing CN owners. There is no trace generation, publication, recovery or pending-file replay after retirement. Old files are historical artifacts, left untouched. No new persistent state, journal, network protocol, cache, polling, dual reads/writes or compatibility execution branch is introduced. Crash recovery, transaction retries, cancellation and ordinary observability keep their existing paths.

## Alternatives and resource model

Repairing the full collector retains expensive data serialization, a parallel storage/control plane and blocking failure modes without a demonstrated operational consumer. Replacing it with a sampled/bounded collector would create a new feature and ownership model; no such requirement is approved. Retaining an inert Service API keeps probes and stale assumptions on hot paths. Complete runtime deletion with only catalog/config admission retained gives the lowest steady-state cost and a clear user contract.

There is no interoperability standard for this internal debug collector. Existing SQL/TOML and catalog contracts are the relevant boundaries. Runtime budgets after retirement are zero trace queues, workers, atomic flags, service lookups, per-row serialization, polling SQL or trace file/network I/O. The trace-specific safety tradeoff is loss of future row-level forensic capture, explicitly accepted by the user; standard metrics/spans remain. No throughput percentage is claimed without measurements.

## Verification and design review

Review revision 1 before implementation: root design review PASS, no subagent. The user approved complete retirement, table/data preservation and inert configuration; clear errors replace the obsolete operational command. The compatibility exception is narrowly catalog/config admission, not executable fallback. Persistence, ownership, resources, alternatives and failure boundaries are closed; no unresolved design blocker remains.

Verify exact pre/post catalog SQL equality and bootstrap consumers; extend an existing real cluster fixture with minimal historical rows, a marker file, enable=true configuration, retirement-command error, successful transactions and unchanged historical state across ordinary restart. Use focused parser/config tests for deprecated options. Remove exclusive runtime tests together; retain broader lifecycle assertions without trace fixtures. Run affected owning packages and actual CLI/embed consumers, relevant CN lifecycle race checks, and configured SCA. Do not add timing-dependent absence assertions or a new collector mock. Record implementation/test/docs deltas separately and review every final hunk.

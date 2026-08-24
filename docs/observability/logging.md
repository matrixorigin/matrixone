# MatrixOne application logging contract

Application logs are an operational interface. A record should answer: what
happened, where, which operation it belongs to, and whether it is repeating.
Logs are not a row dump, SQL history, or a substitute for tracing and profiles.

## The standard for new diagnostics

Define one package-level `logutil.Event` per operational outcome, then reuse
it. Its `Name` is a constant, lowercase dot-separated incident class; its
`Message` is a constant human explanation. Do not put task IDs, table/object
names, SQL, endpoints, or error text in the event name.

```go
var eventDestinationNotEmpty = logutil.Event{
    Name:    "objectio.vector.destination-not-empty",
    Message: "ObjectIO destination vector must be readonly or empty",
}

eventDestinationNotEmpty.WarnLazy(func() []zap.Field {
    return []zap.Field{
        zap.Bool("need-dup", dst.NeedDup()),
        zap.Int("allocated-bytes", dst.Allocated()),
        zap.Int("input-bytes", len(input)),
    }
})
```

This shape gives operators a stable query dimension (`event`), the violated
assumption, and enough bounded context to decide whether the caller or an
ObjectIO path is wrong. It does not emit a vector, object contents, or a
dynamic event key.

`Event.Debug`, `Info`, `Warn`, and `Error` use one bounded process-wide budget
per event: the first three observations are emitted; later observations are
emitted at most once per ten seconds with `occurrence` and `suppressed`.
The retained event-state map is bounded. Therefore Debug is a diagnostic budget
rather than a request/row/packet verbosity switch.

Service and module code that already owns a `*log.MOLogger` uses the matching
`DebugEvent`, `InfoEvent`, `WarnEvent`, `ErrorEvent` and `*Lazy` methods with
the same `logutil.Event`. `With`, `Named`, `WithContext`, and
`GetModuleLogger` share one limiter, so deriving a child logger cannot evade a
retry or queue-pressure event's budget. This is the required form for proxy,
RPC, task and service control loops; do not fall back to a second ad-hoc
sampling scheme.

Use the direct methods only with immediately available scalar fields. For
`fmt.Sprintf`, `String`, `Error`, hashing, snapshots, iteration, serialization,
or any allocation, use `*Lazy`: its builder runs only after the level is
enabled and the event has passed its budget. This is the no-hidden-work rule.

## Severity is an operational decision

| Situation | Level | Required evidence |
| --- | --- | --- |
| Durable DDL, configuration, lifecycle or protection transition | Info | operation/stage and durable outcome |
| Normal high-volume data-plane detail | Debug | bounded terminal summary, never per row/request |
| Recoverable inconsistency, retry or fallback | Warn | failed assumption, attempt/fallback and correlation |
| Failed operation requiring attention | Error | operation, bounded causal context and error class |
| Process cannot continue | Panic/Fatal | invariant and ownership context |

Do not lower a low-frequency but operator-actionable DDL or failure merely to
reduce volume. Keep its severity and control its cost with a stable event,
bounded summary, and lazy fields.

## Retention and causal correlation

Shared logs must not retain SQL, paths, object/table/database names, endpoints,
payloads, or untrusted error text by default. Use
`logutil.StringFingerprintFields(name, value)` for a deterministic
`<name>-sha256` plus `<name>-bytes` correlation pair, and
`logutil.ErrorFingerprintFields(name, err)` for an error fingerprint, concrete
type, and MatrixOne error code where available. Use these inside `*Lazy` when
the source value is not already a trivial scalar.

Metrics count every occurrence; logs retain bounded exemplars and the decision
context. Traces and authorized pprof/artifacts carry per-request or raw detail.

## Migration policy

Legacy `logutil.Info/Warn/Error` remains supported. Do not make a repository
wide mechanical rewrite. Migrate one component at a time when it has a retry,
loop, control-plane, invariant, or incident-debugging need, and add a focused
test if its no-hidden-work guarantee could regress.

The initial adoption batch is ObjectIO vector invariants, CCPR retry and lease
control, the Frontend-to-taskservice CDC restart lifecycle (including the
replacement-generation readiness acknowledgement before `Running` is persisted)
and connection-close transitions, and proxy backend
health/connection/rebalance-queue control paths. Follow-up component selection should be driven by
incident evidence; issue #26185 owns the checklist and explicitly remains
separate from the memory-root-cause investigation in #26172.

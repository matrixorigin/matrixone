# Embedded Sirius bridge

This package owns the ABI-v1 engine/query/input/result handles from Sirius's
generated `sirius_c.h`. CN constructs the bridge and adapts it to the compiler's
backend interface; the bridge and compiler do not import each other.

CPU builds return an explicit configuration error. The native build requires
Linux amd64 and CGo:

```sh
MO_SIRIUS=1 make build-with-prebuilt-native \
  SIRIUS_SDK=/absolute/path/to/embedding-sdk \
  SIRIUS_MERGED_REF=origin/upstream-dev-merge
```

The default is release provenance: the SDK must record a clean source SHA
reachable from the supplied merged branch, plus hashes of its verified C
consumer and complete static/device-link artifacts. Local integration before
merge requires explicit `SIRIUS_BUILD_MODE=development`. The SDK supplies the
C compiler verified against CMake/Ninja's C smoke consumer, C++ linker, and
complete ordered response file. `MO_SIRIUS=1` sets both CGo `CC` and `CXX` to
these SDK compilers so C compilation uses the same sysroot as final linking.
The SDK manifest fingerprint also enters CGo's compile flags, invalidating Go's
cache when external SDK headers or archives change at the same filesystem path.
`MO_CL_CUDA=1` can be enabled
independently; it does not enable Sirius implicitly.

Packaging derives shared dependencies from the verified native consumer,
checks their hashes, and copies them with their original permissions into the
binary's adjacent `lib/`. It rewrites the binary search path to `$ORIGIN/lib`
and every reachable staged library's search path to `$ORIGIN`, including MO's
baseline native libraries supplied by `mo-stage-native-libs`. Missing baseline
files are errors. Patching uses independent temporary copies, so hardlinks or
symlinks cannot modify the SDK or original baseline artifacts. It excludes
host glibc and the NVIDIA driver, verifies that every other resolved dependency
is local to the package, and records binary, SDK, and baseline library hashes
in `lib/sirius-provenance.json`. Neither native compilation nor a container
rebuild is performed by this packaging step.

CN configuration selects `backend="embedded"`, `input-mode="mo"`, and an
explicit `native-config-path`. Native configuration owns GPU selection and
finite host/GPU/spill capacity. Streams default to 2 (maximum 128), waiting
queries to 16 (maximum 16). MO input needs no Flight certificates, resolver or
direct-TAE leases. Flight remains the default backend.

Preparation validates typed read/output descriptors before starting any lazy
producer. Input capacity is acquired before synchronous copies; a producer
must handle `IsNotNeeded` and stop on cancellation. Results retain their native
lease until the output callback finishes. The ABI copies result payloads into
a bounded Go buffer; CN copies vectors into the normal MO memory pool before
calling the existing output function. This is bounded transport, not zero-copy.

Before any C allocation, the bridge caps transient query descriptors (including
plan bytes, every repeated string/manifest copy, and conservative descriptor
array charges) at 64 MiB. This is separate from native metadata admission,
which defaults to 256 MiB and charges its own expansion/copy factors. Schemas
are limited to 1024 columns, output names to 1 MiB in total, and each read's
identity/schema metadata to a conservative 1 MiB envelope. The existing 16 MiB
plan limit remains available; native plan expansion is not double-counted in
the transient CGo arena budget.

Go reserves one of `1 + max-waiting-queries` slots before entering native
preparation or allocating that arena. Preparing calls and prepared/running
query owners share this limit; a slot returns only after query cleanup. The
default is 17, matching one active query and sixteen waiting requests.

Cancellation has an independent native call path. Close first cancels and
joins producers, in-flight calls and cancellation subscriptions; only then
does it destroy handles and release admitted resources. Failed cleanup keeps
ownership for a retry and seals new admission.

This PR supplies the execution boundary and service owner. SQL reader admission
and actual MO reader wiring are the following PR; attempts to select embedded
SQL execution remain explicit errors until that wiring is present. Direct TAE
admission is also a separate integration, not an implicit fallback.

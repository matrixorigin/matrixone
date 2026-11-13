# R3 local feasibility evidence

This package makes the reported tests inspectable. It supports the architecture
discussion in [R3](../20261027-m0-read-boundary-r3.md); it does not supply the
unresolved domain-classification or historical compatibility decision.
[tests.patch](tests.patch) contains the exact three diagnostic test sources,
without changing or replacing product code. The tests are not installed into
the normal suite by this documentation change.

## Identity and method

| Item | Original execution |
| --- | --- |
| Product source | `a44bc4b37cc221938dd29da63cf965185c75ab5c` |
| Documentation at execution | `b40841dc5968a127fb37d3f5467e5f8ce0b3a2be`; no product-code difference from the product source |
| Default-reader baseline | `2465dae7a4ada1b277113cca5afff6fe10149b96` |
| Toolchain | Go `1.27.1`, Darwin/arm64, CGO enabled, `GOMAXPROCS=2`, `-p=2`, `-count=1` |
| Tags | `mo_m0` for the real reader and SQL tests; no tag for either ordinary-reader comparison |
| Native inputs | No difference in `Makefile`, `cgo`, `thirdparties`, `go.mod` or `go.sum` between baseline and product source |
| Native artifacts | Existing product-worktree owner build; `libmo.dylib`: `f02fa174b073e4e23e7fd4fe85b0770a147ee63c00bdbb216d68a81fe1212120`; `libusearch_c.dylib`: `0894f60d1542212286cef9a7eabb1a724aa28762947da060e197734a04724f0d` |
| Isolation | Separate process-local Go overlays for candidate/baseline; shared owner native artifacts and Go cache. The baseline view restored 38 modified files and hid 63 candidate-only files, with restored content checked against Git. Not independent native builds. |

The native hashes match the prior main-conflict owner-build receipt. No
cross-worktree provenance stamp was fabricated. Temporary test executables
were not retained, so their binary hashes are unavailable. These are focused
local executions, not Linux or release-upgrade qualification.

## Recorded observations

| Test | Result and exact scope |
| --- | --- |
| `TestR3RealClassificationAndReader` | PASS, six subtests, actual tag-installed classifier and reader. A legal suffix becomes two NULLs; inconsistent version returns not-M0 then fails at ObjectIO; wrong type/prefix and a proof bound to another object are rejected. |
| Synthetic layout-compatible foreign source | The same reader test accepts an independently written compatible object supplied by the fixture. The fixture bypasses real relation selection, so this establishes caller responsibility, not a production cross-table defect. |
| `TestR3LocalOrdinaryMissingColumn`, candidate default | PASS: present `int32` values `[11,22]`; absent seqnum 1 rejected in `ReadOneBlock`, without a proof marker. |
| Same test, baseline default | PASS: same bytes and present values; absent seqnum 1 returns two `int32 NULL`s. |
| `TestR3CreateCopyCloneRestartIdentity` | PASS, 7.74 s (package 10.454 s), two-row real SQL, single CN/TN. The observations below apply to one COPY before any M0 ADD and an orderly checkpoint/restart. |

The SQL test observed:

| Stage | CN / TN version | Relation and object observation |
| --- | --- | --- |
| CREATE + flush | 0 / 0 | New relation, user seqnums 0-5, composite PK seqnum 6, object version 0 and maxseq 6 |
| COPY ADD + flush | 0 / 0 | New relation and object, added column seqnum 6, composite PK seqnum 7, object version 0 and maxseq 7 |
| clone | 0 / 0 | New relation, same column layout and object references as the COPY parent |
| checkpoint + orderly CN/TN restart | parent 0 / 0; child 0 / 0 | Relation IDs, layouts and object references retained; NULL row-count checks passed |
| child M0 ADD | 1 / 1 | Child relation and old object unchanged, new column seqnum 8, two old rows read NULL; parent lacks the child column |

CN/TN values above were recorded in the log. The source exposes which values
are assertions and which are observations: it checks IDs, layout/object
equality, CN version transitions, row/NULL counts and parent-column isolation;
it does not assert a full-row content hash or every recorded TN value. The
reader fixture also asserts one source close and zero tracked mpool bytes.

The compatibility object was 459 bytes, SHA-256
`0eec5198c0ed71e5b4de0f251e14b08e5cbf127f13ee995ce42ea75d8de9b682`,
unchanged before/after both reads. It is synthetic, with no historical-release
provenance. A regenerated object can have a different UUID/hash; compare the
same bytes within each candidate/baseline pair. This proves a reader-contract
difference, not a broken supported historical upgrade. The inconsistent-schema
test likewise does not establish a production-reachable bad history. Pre-read
pruning may act before ObjectIO and is not covered by that final-read rejection.

## Reproduce the test sources

Original successful wrapper invocations are recorded below with only the
machine-local evidence directory replaced by `$R3_EVIDENCE`. They ran from the
native-owning product checkout; this block records the overlay executions,
not an immediately usable download of the private overlays.

```sh
GOTOOLCHAIN=go1.27.1 GOMAXPROCS=2 .agents/skills/mo-dev/scripts/mo-cgo-test -overlay="$R3_EVIDENCE/candidate/overlay.json" -tags=mo_m0 -p=2 -count=1 -timeout=120s -run '^TestR3RealClassificationAndReader$' -v ./pkg/vm/engine/readutil
GOTOOLCHAIN=go1.27.1 GOMAXPROCS=2 .agents/skills/mo-dev/scripts/mo-cgo-test -overlay="$R3_EVIDENCE/candidate/create-copy-overlay.json" -tags=mo_m0 -p=2 -count=1 -timeout=300s -run '^TestR3CreateCopyCloneRestartIdentity$' -v ./pkg/tests/sqlintegration
R3_OBJECT_DIR="$R3_EVIDENCE/object" R3_OBJECT_NAME_FILE="$R3_EVIDENCE/object-name.txt" R3_CREATE=1 R3_EXPECT=reject GOTOOLCHAIN=go1.27.1 GOMAXPROCS=2 .agents/skills/mo-dev/scripts/mo-cgo-test -overlay="$R3_EVIDENCE/candidate/overlay.json" -p=2 -count=1 -timeout=120s -run '^TestR3LocalOrdinaryMissingColumn$' -v ./pkg/objectio
R3_OBJECT_DIR="$R3_EVIDENCE/object" R3_OBJECT_NAME_FILE="$R3_EVIDENCE/object-name.txt" R3_CREATE=0 R3_EXPECT=fill GOTOOLCHAIN=go1.27.1 GOMAXPROCS=2 .agents/skills/mo-dev/scripts/mo-cgo-test -overlay="$R3_EVIDENCE/baseline/overlay.json" -p=2 -count=1 -timeout=120s -run '^TestR3LocalOrdinaryMissingColumn$' -v ./pkg/objectio
```

The original commands used machine-local `-overlay=.../candidate/overlay.json`,
`-overlay=.../baseline/overlay.json` and, for SQL,
`-overlay=.../candidate/create-copy-overlay.json`. Those paths are not portable.
The following equivalent source recipe applies the unchanged tests to two
scratch checkouts. It has not been rerun for this publication; no new PASS is
claimed from it. The patch is intended for the exact source revisions below.

Start from the checkout containing this evidence package. Fetch the two fixed
revisions if they are unavailable locally. Create private scratch checkouts:

```sh
set -eu
EVIDENCE_DIR="$(git rev-parse --show-toplevel)/docs/design/m0-r3-evidence"
R3_SCRATCH=$(mktemp -d)
git worktree add --detach "$R3_SCRATCH/candidate" a44bc4b37cc221938dd29da63cf965185c75ab5c
git worktree add --detach "$R3_SCRATCH/baseline" 2465dae7a4ada1b277113cca5afff6fe10149b96
git -C "$R3_SCRATCH/candidate" apply --check "$EVIDENCE_DIR/tests.patch"
git -C "$R3_SCRATCH/candidate" apply "$EVIDENCE_DIR/tests.patch"
git -C "$R3_SCRATCH/baseline" apply --check --include=pkg/objectio/r3_local_compat_test.go "$EVIDENCE_DIR/tests.patch"
git -C "$R3_SCRATCH/baseline" apply --include=pkg/objectio/r3_local_compat_test.go "$EVIDENCE_DIR/tests.patch"
```

Prepare matching native dependencies in each scratch checkout using the
repository Makefile/build instructions, or use reuse admitted by the repository
CGo wrapper. Do not copy an unverified stamp or treat these platform-specific
hashes as portable artifacts. Missing dependencies stop reproduction rather
than authorizing a product change. Test sources reuse helpers already present
at the indicated candidate revision; the baseline receives only the standalone
ordinary-reader test. Use Go 1.27.1 and a writable private object directory:

```sh
export GOTOOLCHAIN=go1.27.1
export GOMAXPROCS=2
export R3_OBJECT_DIR="$R3_SCRATCH/object"
export R3_OBJECT_NAME_FILE="$R3_SCRATCH/object-name.txt"
mkdir -p "$R3_OBJECT_DIR"
cd "$R3_SCRATCH/candidate"
.agents/skills/mo-dev/scripts/mo-cgo-test -tags=mo_m0 -p=2 -count=1 -timeout=120s -run '^TestR3RealClassificationAndReader$' -v ./pkg/vm/engine/readutil > "$R3_SCRATCH/reader.log" 2>&1
.agents/skills/mo-dev/scripts/mo-cgo-test -tags=mo_m0 -p=2 -count=1 -timeout=300s -run '^TestR3CreateCopyCloneRestartIdentity$' -v ./pkg/tests/sqlintegration > "$R3_SCRATCH/sql.log" 2>&1
R3_CREATE=1 R3_EXPECT=reject .agents/skills/mo-dev/scripts/mo-cgo-test -p=2 -count=1 -timeout=120s -run '^TestR3LocalOrdinaryMissingColumn$' -v ./pkg/objectio > "$R3_SCRATCH/candidate.log" 2>&1
cd "$R3_SCRATCH/baseline"
R3_CREATE=0 R3_EXPECT=fill .agents/skills/mo-dev/scripts/mo-cgo-test -p=2 -count=1 -timeout=120s -run '^TestR3LocalOrdinaryMissingColumn$' -v ./pkg/objectio > "$R3_SCRATCH/baseline.log" 2>&1
```

Run sequentially and stop at the first failure. Confirm nonempty test selection,
all expected subtests, matching `R3_OBJECT` hashes across the two ordinary reads,
and the expected `R3_MISSING` outcomes. Keep scratch sources, object and logs
until reviewed; cleanup applies only to those private checkouts and processes.
The SQL case's checkpoint/normal restart is not crash recovery. It does not
cover COPY after M0 ADD, arbitrary historical sources or all CREATE/COPY forms.

## Source and receipt hashes

The patch contains the original source bytes with these SHA-256 values:

| Source file in patch | SHA-256 |
| --- | --- |
| `pkg/objectio/r3_local_compat_test.go` | `ce0becd2e68f1b649e63f6ac228cad3e61051d3193262880adf7cc8d18cd5592` |
| `pkg/vm/engine/readutil/r3_local_real_reader_test.go` | `d4dfa504f491f32ca30db4b5b617b2e072e966864e0bfa2ca440db5085c5c5e2` |
| `pkg/tests/sqlintegration/r3_create_copy_sql_test.go` | `065b02806c8748d60818f7a6ed2d4aeb57da7f98538e0f37e49d3e60d60a3b7e` |

| Retained original log | SHA-256 |
| --- | --- |
| `candidate-reader-r4.log` | `6fe46970b2ac16a6ef10a8742c607e6a9ae3eaa4100e3011e5948542fff43f12` |
| `candidate-ordinary.log` | `fd3278e8288980158e4cfa246e28a6daceecd19812ef8c445e0f0a8445269d93` |
| `baseline-ordinary.log` | `25eaf0995a4761f8b37064da514ce52020a81fb4b9b0fae28a039b6b8b9fe3b8` |
| `create-copy-sql.log` | `8cc09852f61298e86e030bb75dfa84bf18c7f8d6741a96e1e43b17d6f1f8d98d` |

The raw logs remain local; the tables above are the public sanitized result
summary, not raw log copies. Earlier toolchain-selection and temporary-test
compile/cleanup failures remain retained separately. They were corrected in
the diagnostic harness before the recorded passes; no product fix was made.

## Remaining design decision

The tests do not supply a reliable ordinary/M0/inconsistent classifier for all
supported clone/recovery histories. They also do not decide which historical
missing-column sources the default build must preserve. The current CR asks
owners to resolve that semantic ownership and compatibility contract, including
shutdown upgrade and backup/restore. Mixed-version cluster operation and
downgrade are outside the current approval requirement; no extra qualification
matrix is requested here. A new persistent marker is not assumed necessary.

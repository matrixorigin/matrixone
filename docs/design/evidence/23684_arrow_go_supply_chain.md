# #23684 Arrow-Go supply-chain review

Review date: 2026-09-04. Candidate base: `up/main@bf63172c06`. The dependency is
pinned to `github.com/apache/arrow-go/v18 v18.7.0`; no floating version is used.

## Arrow runtime closure

`go list -deps` for the imported `arrow/array` and `arrow/ipc` packages reports
eight non-standard modules:

| Module | Version | Detected license |
| --- | --- | --- |
| `github.com/apache/arrow-go/v18` | `v18.7.0` | Apache-2.0; upstream NOTICE present |
| `github.com/goccy/go-json` | `v0.10.6` | MIT |
| `github.com/google/flatbuffers` | `v25.12.19+incompatible` | Apache-2.0 |
| `github.com/klauspost/compress` | `v1.19.0` | BSD-3-Clause |
| `github.com/pierrec/lz4/v4` | `v4.1.27` | BSD-2-Clause |
| `github.com/zeebo/xxh3` | `v1.1.0` | BSD-2-Clause |
| `golang.org/x/exp` | `7ab1446f8b90` | BSD-3-Clause |
| `golang.org/x/sys` | `v0.47.0` | BSD-3-Clause |

No copyleft or source-availability license was found in this closure. Final
NOTICE aggregation remains a release-packaging owner action.

## SBOM and size

CycloneDX `cyclonedx-gomod v1.12.0` generated a 1.6 binary SBOM from the local
Darwin/arm64 `mo-service`: 219 components and 220 dependency entries. The Arrow
component and all eight closure modules were present. Reproducible command:

```text
cyclonedx-gomod bin -json -std -version <release-version> \
  -output mo-service.cdx.json ./mo-service
```

The final local SBOM SHA-256 is
`ab787ad79e00c4f048de8390ebfaf7b91ab9ff5bfd13e3be2fd2ce02bd531282`.
The candidate and identically built baseline binary SHA-256 values are
`7db369ce401480e4dcca98d1463646cdebb86db3d3dfd438adf37568c291640c` and
`308cb694d524206e68ff6957cd54cf74ab60e5f9d778968524397660b7e1f9d8`,
respectively. These hashes identify local evidence artifacts, not signed
release artifacts.

The local candidate binary was 261,955,890 bytes (249.82 MiB), versus
254,982,450 bytes (243.17 MiB) for an identically built `up/main` binary. The
delta is 6,973,440 bytes, or 2.73%. Packaging owners must decide whether that
increase is acceptable for the release image.

## Vulnerability review

`govulncheck -mode=binary` found 17 reachable advisories in the candidate and
18 in the identical `up/main` baseline. The candidate introduced no new
advisory ID and removed the baseline's `GO-2026-4762` result through its module
graph update. The remaining results are repository/toolchain debt in the Go
1.26.4 standard library, gRPC, `x/net`, `x/text`, AWS SDK v2, Avro, and pgx.

A source scan of `arrowbridge`, `arrowipc`, and `external/arrowio` still reaches
`GO-2026-5764` through the existing FileService AWS SDK (`service/s3 v1.68.0`,
fixed in `v1.97.3`). Arrow-Go itself was not named by a reachable advisory.
Because the full candidate scan is non-zero, security approval is **blocked**
until the repository/toolchain findings are upgraded, waived by the security
owner with scope and rationale, or proven unreachable in the release build.

## Platforms and compatibility

MatrixOne's native build matrix names Linux amd64 and Linux arm64; the local
host is Darwin arm64. The imported Arrow `array` and `ipc` packages compile for
all three targets with `CGO_ENABLED=0`. The complete Darwin/arm64 `mo-service`
build and affected CGo test closure pass. Linux image builds remain an exact
release-CI artifact requirement because MatrixOne's native dependencies cannot
be proven by a pure-Go cross compile alone.

This review is complete as evidence, but its result is not an approval: CVE,
binary-size, Linux artifact, NOTICE, and owner sign-off gates remain explicit.

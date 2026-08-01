# MongoDB dependency and build report

Baseline: `2d0adac64ea6c72028c5443813428f84fbcd1575` (`up/main`, 2026-07-27).

- Direct dependency: `go.mongodb.org/mongo-driver/v2 v2.8.0`.
- Newly recorded transitive modules: `github.com/xdg-go/pbkdf2 v1.0.0`, `github.com/xdg-go/scram v1.2.0`, `github.com/xdg-go/stringprep v1.0.4`, and `github.com/youmark/pkcs8` at `a2c0da244d78`. Existing graph versions of `klauspost/compress` and `golang/snappy` are reused. Driver v2.8.0 does not require `montanaflynn/stats`, so that review-time candidate is not added to the final module graph.
- The current main branch has no tracked `vendor/` directory. Consequently the readonly-module change has five net-new `go.mod` requirements and eleven `go.sum` lines; the two already-used modules `golang/snappy` and `klauspost/compress` are also promoted from indirect to direct by `go mod tidy`. No synthetic vendor tree is introduced.
- `license-eye -c .licenserc.yml dep check` and header check pass locally.
- A readonly `make build` succeeds. The local binary is 209,261,234 bytes versus 203,071,186 bytes in the adjacent pre-change workspace build (+6,190,048 bytes, about 3.05%). These binaries came from different branches/build timestamps, so this is an indicative upper bound, not a controlled release-size benchmark.
- The downloaded v2 driver module occupies 8,840 KiB in the local module cache; module-cache size is not shipped binary size.

Release CI still owns the controlled clean-build time/size comparison, SBOM publication, musl TLS/SRV smoke and security-owner approval.

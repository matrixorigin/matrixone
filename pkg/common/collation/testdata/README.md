# Independent MySQL oracle

`mysql_8_0_45.json.gz` contains inputs and answers from the official MySQL 8.0.45
aarch64 image, pinned by digest inside the fixture and generator. For each of
205 strings, the `order` arrays contain all ordered-pair comparisons in row-major
order, stored as bytes `0/1/2` for `less/equal/greater` (base64 in JSON).
`WEIGHT_STRING` digests cover all valid BMP scalars followed by 2,048 supplementary
samples at `0x10000 + i * 509`. These are mapping oracles, not PAD SPACE comparison
keys. Native `0900_ai_ci` answers are retained to distinguish MO's existing alias.

To regenerate, use a dedicated task-owned Docker container with no published
ports and the pinned image. The example server accepts an empty root password
only inside its isolated container:

```sh
docker --context YOUR_TASK_CONTEXT run -d --name YOUR_TASK_CONTAINER --network none -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mysql@sha256:4af1f8815716546f5b12410f7621f37f93db8dd11a184706ef59111930b8c2ff
# Wait for docker exec ... mysqladmin -uroot ping to succeed.
python3 pkg/common/collation/testdata/regenerate.py --context YOUR_TASK_CONTEXT --container YOUR_TASK_CONTAINER --evidence-dir artifacts/issue-28164-d1/oracle
```

The generator verifies the image/build, creates/replaces tables in the isolated
`mo28164_oracle` database, saves raw SQL/results and replaces the fixture. It
uses previous fixture inputs only; it obtains every answer freshly from MySQL.
Remove the task-owned container afterward. Never point it at a shared server.

Run `go test ./pkg/common/collation` for native-oracle comparisons and the
independent exhaustive PAD SPACE ordering test. See the design document
`docs/design/issue-28164-weight-key-v1.md` for the final byte format and scope.

## V1 extension (2026-09-16)

`mysql_8_0_45_extended.json.gz` preserves the parent fixture by SHA-256 and adds
0900-bin, long inputs, expansions, ignorables, combining sequences and character
prefixes. It has 242 strings and 58,564 ordered comparisons per domain. Of the
234,256 results, 126,075 are unchanged parent answers and 108,181 were newly
queried. Both native domains include exact sample WEIGHT_STRING bytes and
length-delimited scalar mapping digests; these answers come from MySQL, not Go.

Run `extend_oracle.py` with the same explicit `--context`, `--container` and
`--evidence-dir` arguments as above. It writes only the extension fixture and
its task-owned `mo28164_freeze` database. It never changes the parent oracle.
Normal Go tests read the committed fixtures offline; Docker is needed only to
collect new oracle answers. The V1 regression hashes must not be regenerated
merely to make a changed implementation pass.

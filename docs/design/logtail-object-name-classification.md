# Classify logtail object-list names without regexp work

Owner: disttae/logtailreplay utils.go. Tracking #29562. This is a cumulative phase-6 optimization alongside role initialization batching and clone-owned catalog metadata.

## Evidence, contract and consumers

Fresh main full issues race profile identifies regexp bitState.reset as about 0.82GiB allocation, of which IsMetaEntry paths account for about 0.48GiB (not additive with IsDataObjectList/IsTombstoneObjectList). Current two patterns are unanchored ASCII-digit languages `_\d+_data_meta` and `_\d+_tombstone_meta`. Classification feeds logtail replay, partition entry dispatch, statistics and replay policy. Producers use object-list labels; consumer compatibility deliberately accepts containing substrings, leading zeros, arbitrary digit length, and arbitrary surrounding bytes. Do not narrow these accepted names to numeric uint64 IDs, anchored/prefix/suffix-only labels or current producer output.

Replace the two regex variables and calls with a shared private pure matcher. Search for each fixed suffix (`_data_meta` / `_tombstone_meta`) using strings.Index, walk backward across ASCII digits, and accept iff at least one digit is immediately preceded by underscore. If a suffix occurrence fails, continue through subsequent occurrences. UTF-8 validity is irrelevant; regex \d is ASCII [0-9]. The suffixes cannot overlap themselves; advancing past the located suffix does not hide any match. Each scanned digit belongs to its suffix occurrence, giving linear total work and zero allocations. Preserve IsMetaEntry short-circuit and the public classifiers. Remove regexp import and unused compiled patterns. No state/cache/pool/owner/lifecycle changes and no wire/disk/name format changes.

## Alternatives and negation

Keeping regex preserves semantics but pays synchronization/reset/race cost for a tiny fixed grammar. An anchored prefix/suffix check or strconv parsing fails surrounding bytes/overflow compatibility. A negative-only contains guard helps non-object labels but retains positive hot-path cost. A generic regex replacement framework would add unnecessary complexity. Select the minimal shared grammar helper with two actual consumers.

Independent expected-result cases cover valid emitted names, zero/leading zero/huge ID, invalid empty digits/sign/separator/non-ASCII digits, embedded matches, repeated invalid then valid suffixes, both suffixes and invalid UTF-8. Differential oracle against the exact previous RE2 patterns covers bounded deterministic generated strings and a seed fuzz test; retain all existing logtail/partition/engine public oracles. Error/cancel/cleanup ownership is unchanged because these functions have no resources or failure API.

## Validation and budget

Exact gpt-6.1-sol/xhigh design approval before production edit. Add focused semantic UT and normal/race benchmarks for ordinary catalog labels, emitted data/tombstone labels, long ID and invalid repeated suffixes. Build unchanged classifier test binary before edit. Keep only with measured normal/race improvement and no ordinary-case regression. Run owning logtailreplay package normal/race, disttae dependent normal/race, real distributed engine logtail cases and complete issues race final head. Complete issues measurement remains same tags/native/NVMe/8 CPU and same test identities; head additionally pays new bootstrap assertions. Capture allocation counts/space, wall/CPU/RSS separately; no claimed CI minutes from regex microbenchmarks.

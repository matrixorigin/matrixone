// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package idxcron carries the shared cuvs idxcron Updatable body.
//
// Lives one directory away from pkg/vectorindex/idxcron (the cron
// consumer/executor) to avoid a test-only import cycle on the GPU
// tag: pkg/vectorindex/idxcron tests pull in pkg/sql/plan via
// testengine, pkg/sql/plan pulls in pkg/indexplugin/all, and on the
// GPU tag that pulls in pkg/vectorindex/cagra/plugin/idxcron which
// needs the shared helper. Putting the helper in a separate package
// breaks the cycle.
package idxcron

import (
	"fmt"
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// CuvsUpdatableSpec carries the per-algorithm configuration the shared
// CuvsUpdatable body needs. Struct (not positional args) so future
// knobs can be added without churning every call site — e.g. an
// absolute MinRecordCount floor, a ThresholdMultiplier, alternate
// CountStrategy (records vs chunks vs bytes), etc.
type CuvsUpdatableSpec struct {
	// StorageTableType is the IndexAlgoTableType holding the tag=1
	// CDC event log (catalog.Cagra_TblType_Storage /
	// catalog.Ivfpq_TblType_Storage).
	StorageTableType string

	// ThresholdParam is the indexAlgoParams key whose int64 value
	// sets the minimum delta-record count needed before the
	// cron-triggered rebuild fires. For CAGRA:
	// catalog.IntermediateGraphDegree; for IVF-PQ:
	// catalog.IndexAlgoParamLists.
	ThresholdParam string

	// MinSizeDefault is the cuvs library default for ThresholdParam,
	// applied as a min-size floor when indexAlgoParams[ThresholdParam]
	// is 0/missing. CAGRA: 128 (cuvs.DefaultCagraBuildParams().
	// IntermediateGraphDegree). IVF-PQ: 1024 (cuvs.DefaultIvfPqBuildParams().
	// NLists). The min-size check is the load-bearing gate — even if
	// the user didn't specify a build param, the rebuild needs at
	// least the cuvs default count of records to produce a usable
	// index.
	MinSizeDefault int64
}

// runSelectChunkSql runs the SELECT used to fetch tag=1 chunk data.
// Stubbed as a var so tests can replace it.
var runSelectChunkSql = sqlexec.RunSql

// CuvsUpdatable counts CDC delta records in the index's tag=1 chunks
// and compares against the threshold (read fresh from
// indexAlgoParams[spec.ThresholdParam] every tick). The shared body
// that CAGRA's and IVF-PQ's Updatable hooks delegate to.
//
// Returns:
//   - (true,  "",     nil) when delta record count >= threshold
//   - (false, reason, nil) when delta < threshold (rebuild deferred)
//   - (_, _, err)          on SQL / decode failures
//
// When tag=1 is empty (no CDC events since last rebuild) the count is
// zero and the gate skips. Likewise when the threshold is missing or
// non-positive in indexAlgoParams — the rebuild is deferred until the
// user supplies a sensible threshold.

// MaxOverflowSize is the brute-force overflow ceiling. Once the
// per-cron-tick count exceeds this, the gate fires regardless of the
// per-algo minimum threshold (lists / intermediate_graph_degree).
//
// Sizing rationale: GPU brute-force (cuvs.GpuBruteForce, fp32, D≈768
// on A100/L40S) stays comfortably under 5ms per query at 200K
// vectors, then grows roughly linearly with overflow size. 200K
// keeps brute-force latency well inside typical search SLAs while
// still letting the GPU absorb a meaningful amount of CDC traffic
// between rebuilds. Safe across all reasonable GPU targets including
// older cards (T4/V100) and higher embedding dimensions.
//
// Note: the cadence gate (createdAt+interval check above) is the
// operator's contract and is still honored — the safety cap only
// overrides the per-algo threshold, not the cadence. Hardcoded
// because there's no algo-specific reason to differ today; lift to a
// spec field if that changes.
const MaxOverflowSize = 200_000

func CuvsUpdatable(
	in idxcronplugin.UpdatableInput,
	spec CuvsUpdatableSpec,
) (ok bool, reason string, err error) {
	if spec.StorageTableType == "" || spec.ThresholdParam == "" {
		return false, "", moerr.NewInternalErrorNoCtxf(
			"CuvsUpdatable: spec must set StorageTableType and ThresholdParam")
	}

	// Cadence: skip when the last rebuild is still within the
	// configured interval. The executor enforces createdAt+interval
	// universally; this is the per-run cadence that used to live in
	// the executor's listsAware=false branch. This gate is operator-
	// configurable and is the user's contract for "don't rebuild too
	// often" — even the MaxOverflowSize safety cap below respects it.
	if in.LastUpdateAt != nil {
		last := time.Unix(in.LastUpdateAt.Unix(), 0)
		now := time.Now()
		if last.Add(in.Interval).After(now) {
			return false, fmt.Sprintf(
				"current time < interval after lastUpdateAt (%v + %v > %v)",
				last.Format("2006-01-02 15:04:05"), in.Interval, now.Format("2006-01-02 15:04:05")), nil
		}
	}

	// Locate the storage IndexDef + read threshold from its
	// indexAlgoParams.
	var storageTbl, algoParams string
	for _, idx := range in.TableDef.Indexes {
		if idx.IndexName == in.IndexName && idx.IndexAlgoTableType == spec.StorageTableType {
			storageTbl = idx.IndexTableName
			algoParams = idx.IndexAlgoParams
			break
		}
	}
	if storageTbl == "" {
		return false, "", moerr.NewInternalErrorNoCtxf(
			"CuvsUpdatable: no IndexDef found for index %q with table-type %q",
			in.IndexName, spec.StorageTableType)
	}

	// Read and validate threshold up front — cheap, lets us fail fast
	// on a malformed indexAlgoParams without doing the SQL count.
	threshold, err := readInt64Param(algoParams, spec.ThresholdParam)
	if err != nil {
		return false, "", err
	}
	if threshold < 0 {
		return false, "", moerr.NewInternalErrorNoCtxf(
			"CuvsUpdatable: indexAlgoParams[%q] negative threshold %d",
			spec.ThresholdParam, threshold)
	}
	// threshold == 0 means indexAlgoParams didn't carry a positive value
	// for the build param. Fall back to the cuvs library default
	// (spec.MinSizeDefault) so the cron still gates on a sensible
	// minimum — the build-side min-size requirement applies whether the
	// user named a value or accepted the cuvs default.
	if threshold == 0 {
		threshold = spec.MinSizeDefault
	}

	// Count brute-force-overflow growth straight from the CDC tail chunk
	// headers (n_inserts + n_upserts) — no per-record decode, so no
	// record-shape (dim / includeBytesPerRow) needs deriving here.
	count, err := countTag1Records(in.Sqlproc, in.TableDef.DbName, storageTbl)
	if err != nil {
		return false, "", err
	}

	// Safety cap: when brute-force overflow exceeds MaxOverflowSize,
	// GPU brute-force search starts to noticeably degrade query
	// latency — fire rebuild now (cadence already passed above).
	// This overrides the per-algo minimum threshold, but NOT the
	// cadence: operators set the interval intentionally and the
	// cap is just a tighter override on top of the min threshold.
	if count >= MaxOverflowSize {
		return true, fmt.Sprintf(
			"brute-force overflow %d >= MaxOverflowSize %d", count, MaxOverflowSize), nil
	}

	if count < threshold {
		return false, fmt.Sprintf(
			"CDC delta records %d < threshold %d (param %s)",
			count, threshold, spec.ThresholdParam), nil
	}
	return true, "", nil
}

// readInt64Param extracts a int64 value at key from a JSON
// indexAlgoParams blob, returning 0 if the key is absent. A parse
// error on a present key surfaces; a missing key is benign (0).
func readInt64Param(algoParams, key string) (int64, error) {
	if algoParams == "" {
		return 0, nil
	}
	ast, err := sonic.Get([]byte(algoParams), key)
	if err != nil {
		// Key absent — not an error, just no threshold configured.
		return 0, nil
	}
	v, err := ast.Int64()
	if err != nil {
		return 0, moerr.NewInternalErrorNoCtxf(
			"CuvsUpdatable: indexAlgoParams[%q] is not int64: %v", key, err)
	}
	return v, nil
}

// countTag1Records runs the chunk-fetch SQL, unframes each row's
// chunk data, and sums the per-op counts carried in the chunk frame
// header. It returns n_inserts + n_upserts — the brute-force-overflow
// growth that the rebuild would fold into a fresh main index (deletes
// don't grow the overflow; see the body comment).
func countTag1Records(
	sqlproc *sqlexec.SqlProcess,
	dbName, storageTbl string,
) (int64, error) {
	sql := fmt.Sprintf(
		"SELECT data FROM %s WHERE index_id = %s AND tag = %d",
		sqlquote.QualifiedIdent(dbName, storageTbl), sqlquote.String(vectorindex.CdcTailId), vectorindex.Tag_CdcEvents)

	res, err := runSelectChunkSql(sqlproc, sql)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	var totalInserts, totalDeletes, totalUpserts int64
	for _, bat := range res.Batches {
		if bat.RowCount() == 0 {
			continue
		}
		dataVec := bat.Vecs[0]
		for i := 0; i < bat.RowCount(); i++ {
			framed := dataVec.GetBytesAt(i)
			if len(framed) == 0 {
				continue
			}
			_, _, nIns, nDel, nUps, err := cuvscdc.UnframeCdcChunk(framed)
			if err != nil {
				return 0, moerr.NewInternalErrorNoCtxf(
					"countTag1Records: unframe chunk: %v", err)
			}
			totalInserts += int64(nIns)
			totalDeletes += int64(nDel)
			totalUpserts += int64(nUps)
		}
	}
	// Brute-force-overflow growth = inserts + upserts. DELETEs apply to
	// the main cuvs index (via the filter / deleted set) and don't
	// affect the brute-force overflow size, so they're irrelevant to
	// the rebuild-trigger decision — the gate fires when overflow has
	// grown enough to be worth folding into a fresh main index.
	// UPSERTs count toward growth because empirically most MO UPSERTs
	// are first-time inserts; the rare replay-from-corruption case
	// over-triggers at worst (wasted rebuild, not a correctness bug).
	// The n_upserts / n_deletes breakdown is preserved in the chunk
	// header for logging and audits.
	_ = totalDeletes
	return totalInserts + totalUpserts, nil
}

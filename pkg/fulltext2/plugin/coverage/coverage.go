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

// Package coverage answers, for a fulltext2 index, whether its CDC-maintained
// state is current enough to be used as a MANDATORY filter at a given snapshot.
//
// fulltext2 is always-async: an ISCP consumer tails the base table and appends
// segments. The index therefore trails the base table by some lag, and an index
// probe ANDed into an ordinary predicate would drop rows written inside that
// lag. The check here is what makes such a probe safe.
//
// It rests on one ordering property, which is why the answer can be trusted:
// the consumer advances the watermark in the SAME TRANSACTION as the segment
// INSERTs (fulltext2_consumer.go — UpdateWatermark is called with sqlctx.Txn()).
// So a persisted watermark is never ahead of durable, visible index data. A
// stale read of it is safe — it only makes this decline.
package coverage

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/coverage"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
)

// ISCP job states, mirrored from pkg/iscp/types.go. They are duplicated rather
// than imported because pkg/iscp reaches pkg/sql/plan transitively, and the
// planner — which calls this hook — sits below both. Only these two mean "the
// index is being kept current"; every other state must decline.
const (
	iscpJobStateRunning   int8 = 2 // ISCPJobState_Running
	iscpJobStateCompleted int8 = 3 // ISCPJobState_Completed
)

// execWithResult runs one internal SQL statement in the caller's transaction.
// A thin local copy of the same helper pkg/iscp and pkg/publication each keep,
// for the import reason above. It is a var so a test can drive the decision
// logic below without a live CN runtime.
var execWithResult = execWithResultImpl

func execWithResultImpl(ctx context.Context, sql, cnUUID string, txn client.TxnOperator) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(cnUUID).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("fulltext2 coverage: no internal sql executor")
	}
	// same options the ISCP helper uses: this read is part of the caller's
	// statement, so it must not increment the statement counter
	opts := executor.Options{}.WithDisableIncrStatement().WithTxn(txn)
	return v.(executor.SQLExecutor).Exec(ctx, sql, opts)
}

// Hooks implements coverage.Hooks for fulltext2.
type Hooks struct{}

var _ coverage.Hooks = Hooks{}

// jobNameForIndex mirrors the ISCP job identity that CreateIndexCdcTask
// registers for an index ("index_" + index name). Kept as a tiny local copy
// rather than an import: pkg/sql/compile owns the writer side and importing it
// here would be a cycle.
func jobNameForIndex(indexName string) string { return "index_" + indexName }

// parseWatermark decodes the "physical-logical" watermark the ISCP executor
// writes. A value it cannot read is not an error to propagate — it is simply no
// evidence of freshness.
func parseWatermark(s string) (types.TS, bool) {
	physical, logical, found := strings.Cut(s, "-")
	if !found {
		return types.TS{}, false
	}
	p, err := strconv.ParseInt(physical, 10, 64)
	if err != nil {
		return types.TS{}, false
	}
	l, err := strconv.ParseUint(logical, 10, 32)
	if err != nil {
		return types.TS{}, false
	}
	ts := types.BuildTS(p, uint32(l))
	return ts, !ts.IsEmpty()
}

// CoversSnapshot reports whether the index generation a probe would search reaches the
// read's coverage bar.
//
// Two independent conditions, both required and both fail closed:
//   - liveness: there is a live (running/completed, not dropped) ISCP maintenance job,
//     so the index is being kept current at all;
//   - coverage: build_ts >= bar, where build_ts is MAX(metadata.build_ts) over base +
//     cdc_tail of the generation the search will actually use -- the loaded generation
//     from the cache when warm, else the durable metadata a fresh load would see. Reading
//     the searched generation's own build_ts (not a global watermark) is what prevents a
//     stale warm cache from over-reporting coverage. The bar is the snapshot TS for a
//     historical read (fixed past state) and SourceCommitTS for a current read.
//
// An empty bar, an unknown build_ts (0), or any lookup error declines.
func (Hooks) CoversSnapshot(ctx context.Context, req coverage.Request) (bool, error) {
	if req.IndexDef == nil || req.Txn == nil || req.TableID == 0 {
		return false, nil
	}
	// The bar build_ts must reach. A historical read sees a fixed past state, so the bar
	// is the snapshot TS itself (build_ts >= snapshot ⇒ every source commit up to it is
	// indexed); SourceCommitTS is not used for a snapshot read. A current read uses the
	// max outstanding source commit the query CN observes.
	bar := req.SourceCommitTS
	if req.ScanSnapshotTS != nil {
		bar = types.TimestampToTS(*req.ScanSnapshotTS)
	}
	if bar.IsEmpty() {
		return false, nil
	}
	live, err := indexJobLive(ctx, req)
	if err != nil || !live {
		return false, err
	}
	covered := types.BuildTS(searchedBuildTS(ctx, req), 0)
	return !covered.LT(&bar), nil
}

// IndexBuildTS returns build_ts of the generation a probe would search (0 = unknown),
// for the planner's partial-plan gap decision (SourceCommitTS - build_ts). It does NOT
// check liveness -- that is CoversSnapshot's job; this only exposes the coverage point.
func (Hooks) IndexBuildTS(ctx context.Context, req coverage.Request) types.TS {
	if req.IndexDef == nil || req.Txn == nil || req.TableID == 0 {
		return types.TS{}
	}
	return types.BuildTS(searchedBuildTS(ctx, req), 0)
}

// searchedBuildTS reads build_ts of the generation a probe would actually search, the SAME
// way for both sources: a {snapshot=...}/AS OF read uses the snapshot-bound cache key
// (index_table@snapshot) AND reads the durable metadata as of that snapshot; a current read
// uses the plain key AND the current txn. Deriving both from this one choice keeps the warm
// (cache) and cold (metadata) paths from disagreeing about which generation they measure.
// Warm: the loaded generation's build_ts from the cache; cold: MAX(build_ts) from metadata.
// 0 = unknown.
func searchedBuildTS(ctx context.Context, req coverage.Request) int64 {
	key, metaTxn := req.IndexStorageTable, req.Txn
	if req.ScanSnapshotTS != nil {
		key = veccache.SnapshotKey(req.IndexStorageTable, *req.ScanSnapshotTS)
		metaTxn = req.Txn.CloneSnapshotOp(*req.ScanSnapshotTS)
	}
	buildTS, ok := veccache.Cache.GetBuildTS(key)
	if !ok {
		buildTS = maxDurableBuildTS(ctx, req, metaTxn)
	}
	return buildTS
}

// indexJobLive reports whether the index has a live ISCP maintenance job: at least one
// non-dropped row, and every non-dropped row running or completed. Fails closed.
func indexJobLive(ctx context.Context, req coverage.Request) (bool, error) {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return false, err
	}
	// The ISCP log lives in the system tenant and carries account_id as an ordinary
	// column, so the tenant is named in the predicate, not inherited from the context.
	sysCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	sql := fmt.Sprintf(
		"SELECT job_state, drop_at FROM mo_catalog.mo_iscp_log"+
			" WHERE account_id = %d AND table_id = %d AND job_name = %s",
		accountID, req.TableID, sqlquote.String(jobNameForIndex(req.IndexDef.IndexName)),
	)
	res, err := execWithResult(sysCtx, sql, req.CNUUID, req.Txn)
	if err != nil {
		return false, err
	}
	defer res.Close()

	// (account, table, job_name) is not unique — job_id completes the key, so a dropped
	// job and its replacement both appear. Dropped rows say nothing; every live row must be
	// running/completed, and there must be at least one, or there is no maintenance to rely on.
	sawLive, live := false, true
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if len(cols) < 2 {
			live = false
			return false
		}
		states := vector.MustFixedColWithTypeCheck[int8](cols[0])
		for i := 0; i < rows; i++ {
			if !cols[1].IsNull(uint64(i)) {
				continue // dropped
			}
			sawLive = true
			if states[i] != iscpJobStateRunning && states[i] != iscpJobStateCompleted {
				live = false
				return false
			}
		}
		return true
	})
	return sawLive && live, nil
}

// maxDurableBuildTS reads MAX(build_ts) over the index's metadata table -- base + cdc_tail --
// on txn, the operator the caller already snapshot-aligned to the generation being measured
// (the current txn, or one cloned at the read's snapshot). Returns 0 (declines) on a missing
// column (pre-migration index), an unresolved table, or any read error: a safe under-report.
func maxDurableBuildTS(ctx context.Context, req coverage.Request, txn client.TxnOperator) int64 {
	if req.IndexMetadataDB == "" || req.IndexMetadataTable == "" {
		return 0
	}
	sql := fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s",
		catalog.FullText2Index_TblCol_Metadata_Build_Ts,
		sqlquote.QualifiedIdent(req.IndexMetadataDB, req.IndexMetadataTable))
	res, err := execWithResult(ctx, sql, req.CNUUID, txn)
	if err != nil {
		return 0
	}
	defer res.Close()
	var ts int64
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 || len(cols) < 1 || cols[0].IsNull(0) {
			return false
		}
		ts = vector.GetFixedAtNoTypeCheck[int64](cols[0], 0)
		return false
	})
	return ts
}

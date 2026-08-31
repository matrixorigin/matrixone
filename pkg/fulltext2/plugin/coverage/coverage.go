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

// CoversSnapshot reports whether the index's watermark has reached req.Snapshot.
//
// Fails closed everywhere: a missing job, a dropped one, a job that is not
// running cleanly, a NULL/unparsable watermark, or any lookup error all report
// false. Only an explicit "watermark >= snapshot" on a live job returns true.
func (Hooks) CoversSnapshot(ctx context.Context, req coverage.Request) (bool, error) {
	if req.IndexDef == nil || req.Txn == nil || req.TableID == 0 || req.Snapshot.IsEmpty() {
		return false, nil
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return false, err
	}

	// The ISCP log lives in the system tenant and carries account_id as an
	// ordinary column, so the tenant is named in the predicate, not inherited
	// from the context.
	sysCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	sql := fmt.Sprintf(
		"SELECT watermark, job_state, drop_at FROM mo_catalog.mo_iscp_log"+
			" WHERE account_id = %d AND table_id = %d AND job_name = %s",
		accountID, req.TableID, sqlquote.String(jobNameForIndex(req.IndexDef.IndexName)),
	)

	res, err := execWithResult(sysCtx, sql, req.CNUUID, req.Txn)
	if err != nil {
		return false, err
	}
	defer res.Close()

	// (account, table, job_name) is not unique — job_id completes the key, so a
	// dropped job and its replacement both appear. Dropped rows say nothing
	// about current content and are ignored; every LIVE row must be covered,
	// and there must be at least one, or there is no maintenance to rely on.
	sawLive, covered := false, true
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if len(cols) < 3 {
			covered = false
			return false
		}
		states := vector.MustFixedColWithTypeCheck[int8](cols[1])
		for i := 0; i < rows; i++ {
			if !cols[2].IsNull(uint64(i)) {
				continue // dropped
			}
			sawLive = true
			// pending / error / canceled: not being kept current
			if states[i] != iscpJobStateRunning && states[i] != iscpJobStateCompleted {
				covered = false
				return false
			}
			if cols[0].IsNull(uint64(i)) {
				covered = false
				return false
			}
			// the log stores the watermark in the "physical-logical" form the
			// ISCP executor writes; parsed here rather than with
			// types.StringToTS, which PANICS on anything malformed and would
			// take the planner down over a corrupt catalog row
			wm, ok := parseWatermark(cols[0].GetStringAt(i))
			if !ok || wm.LT(&req.Snapshot) {
				covered = false
				return false
			}
		}
		return true
	})
	return sawLive && covered, nil
}

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

// Package coverage reports whether the resident HNSW generation contains all
// rows visible to a read. HNSW is maintained asynchronously by ISCP, so a
// search result is only safe for the planner's top-k rewrite when its build
// watermark reaches the source table's commit watermark.
package coverage

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	indexcoverage "github.com/matrixorigin/matrixone/pkg/indexplugin/coverage"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
)

type Hooks struct{}

var _ indexcoverage.Hooks = Hooks{}

var execWithResult = execWithResultImpl

func execWithResultImpl(ctx context.Context, sql, cnUUID string, txn client.TxnOperator) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(cnUUID).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("hnsw coverage: no internal sql executor")
	}
	opts := executor.Options{}.WithDisableIncrStatement().WithTxn(txn)
	return v.(executor.SQLExecutor).Exec(ctx, sql, opts)
}

// BuildCoversSource is kept small and deterministic so the timestamp invariant
// can be tested without constructing a live SQL process. HNSW currently stores
// the physical part of the CDC upper bound. (buildTS, 0) is a conservative
// lower bound on the generation HLC and must cover the full source watermark.
func BuildCoversSource(buildTS int64, sourceTS types.TS) bool {
	if buildTS <= 0 || sourceTS.IsEmpty() {
		return false
	}
	built := types.BuildTS(buildTS, 0)
	return !built.LT(&sourceTS)
}

// BuildCoversSnapshot adds the read-snapshot bound to BuildCoversSource. A
// newer resident generation is not safe for an older read: updates may change
// the vector used for ranking even when the base-table join hides future rows.
// Equal physical times are ambiguous because build_ts has lost its logical
// component; only a strictly older physical generation is provably not newer.
func BuildCoversSnapshot(buildTS int64, snapshotTS, sourceTS types.TS) bool {
	if buildTS <= 0 || snapshotTS.IsEmpty() {
		return false
	}
	return buildTS < snapshotTS.Physical() && BuildCoversSource(buildTS, sourceTS)
}

// CoversSnapshot claims coverage for the generation execution will search. A
// warm cache reports its resident generation; a cold cache uses the durable
// metadata in the planning transaction, which is also what execution loads.
// Any missing provenance or read error fails closed.
func (Hooks) CoversSnapshot(ctx context.Context, req indexcoverage.Request) (bool, types.TS, error) {
	if req.IndexDef == nil || req.TableID == 0 || req.Txn == nil ||
		req.IndexStorageTable == "" || req.SourceCommitTS == nil ||
		req.IndexMetadataDB == "" || req.IndexMetadataTable == "" {
		return false, types.TS{}, nil
	}

	buildTS := searchedBuildTS(ctx, req)
	if buildTS <= 0 {
		return false, types.TS{}, nil
	}

	sourceTS, err := req.SourceCommitTS(ctx, types.BuildTS(buildTS, 0))
	if err != nil || !BuildCoversSnapshot(buildTS, req.Snapshot, sourceTS) {
		return false, types.BuildTS(buildTS, 0), err
	}
	return true, types.BuildTS(buildTS, 0), nil
}

func searchedBuildTS(ctx context.Context, req indexcoverage.Request) int64 {
	key := req.IndexStorageTable
	txn := req.Txn
	if req.ScanSnapshotTS != nil {
		key = veccache.SnapshotKey(key, *req.ScanSnapshotTS)
		txn = req.Txn.CloneSnapshotOp(*req.ScanSnapshotTS)
	}
	if buildTS, ok := veccache.Cache.GetBuildTS(key); ok {
		return buildTS
	}
	return maxDurableBuildTS(ctx, req, txn)
}

func maxDurableBuildTS(ctx context.Context, req indexcoverage.Request, txn client.TxnOperator) int64 {
	sql := fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s",
		catalog.Hnsw_TblCol_Metadata_Build_Ts,
		sqlquote.QualifiedIdent(req.IndexMetadataDB, req.IndexMetadataTable))
	res, err := execWithResult(ctx, sql, req.CNUUID, txn)
	if err != nil {
		return 0
	}
	defer res.Close()
	var buildTS int64
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 || len(cols) < 1 || cols[0].IsNull(0) {
			return false
		}
		buildTS = vector.GetFixedAtNoTypeCheck[int64](cols[0], 0)
		return false
	})
	return buildTS
}

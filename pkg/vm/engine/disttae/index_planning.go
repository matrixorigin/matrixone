// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ engine.IndexPlanningRelation = (*txnTableDelegate)(nil)

// This view borrows the canonical ordinary relation only for a synchronous
// root statement. It never reads a catalog/partition or creates an owner.
func (tbl *txnTableDelegate) planningRead(ctx context.Context, read client.CatalogReadStamp) (*txnTable, *Transaction, bool) {
	if ctx == nil || context.Cause(ctx) != nil || tbl == nil || tbl.origin == nil || tbl.isMock || tbl.shard.is || tbl.combined.is || read.Op == nil || read.Op.IsSnapOp() || read.Snapshot.IsEmpty() {
		return nil, nil, false
	}
	t := tbl.origin
	txn, ok := read.Workspace.(*Transaction)
	if !ok || txn == nil || txn.engine == nil || txn.op != read.Op || !txn.Readonly() || txn.haveDDL.Load() || t.db == nil || t.db.op != read.Op || t.eng != txn.engine || t.partitioned != 0 || t.remoteWorkspace || t.fake || t.createdInTxn || t.db.databaseType != "" || t.db.accountId != t.accountId || t.tableId == 0 || t.db.databaseId == 0 || t.tableDef == nil || t.tableDef.IsTemporary || t.tableDef.IsDynamic || (t.relKind != catalog.SystemOrdinaryRel && t.relKind != catalog.SystemIndexRel) {
		return nil, nil, false
	}
	account, err := defines.GetAccountId(ctx)
	if err != nil || account != t.accountId || t.tableDef.TblId != t.tableId || t.tableDef.Version != t.version {
		return nil, nil, false
	}
	reader, ok := read.Op.(interface {
		TryCatalogReadStamp() (client.CatalogReadStamp, bool)
	})
	if !ok {
		return nil, nil, false
	}
	stamp, ok := reader.TryCatalogReadStamp()
	// Reject a changed domain before observing its workspace, preserving the
	// storage owner's original sampling order and optional fallback policy.
	if !ok || !read.MatchesRead(stamp.BindRead(read.Op, read.Visibility), client.CatalogSnapshotSemantic) {
		return nil, nil, false
	}
	visibility, stable := txn.CatalogVisibility()
	return t, txn, stable && visibility == read.Visibility && context.Cause(ctx) == nil
}

func planningReadStillCurrent(tbl *txnTableDelegate, ctx context.Context, read client.CatalogReadStamp, t *txnTable, txn *Transaction) bool {
	current, workspace, ok := tbl.planningRead(ctx, read)
	return ok && current == t && workspace == txn
}
func planningContextCause(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return context.Cause(ctx)
}

// ReadIndexCostStats copies one ordinary observation under its existing GS
// lock. Unbound metadata is a heuristic; no number authorizes executable rows.
func (tbl *txnTableDelegate) ReadIndexCostStats(ctx context.Context, read client.CatalogReadStamp, observed *pb.StatsInfo, column string) (engine.IndexCostStats, bool, error) {
	var out engine.IndexCostStats
	t, txn, ok := tbl.planningRead(ctx, read)
	if !ok || observed == nil || len(column) == 0 || len(column) > engine.MaxIndexPlanningNameBytes {
		return out, false, planningContextCause(ctx)
	}
	gs := txn.engine.globalStats
	if gs == nil || gs.ctx == nil || context.Cause(gs.ctx) != nil || !gs.mu.TryLock() {
		return out, false, planningContextCause(ctx)
	}
	key := pb.StatsInfoKey{AccId: t.accountId, DatabaseID: t.db.databaseId, TableID: t.tableId, DbName: t.db.databaseName, TableName: t.tableName}
	version := t.version
	current, complete, incompatible := gs.statsInfoForTableVersionLocked(key, &version, false)
	boundVersion, versioned := gs.mu.tableDefVersions[key]
	// Ordinary metadata publications are intentionally unbound, matching
	// GetAtTableVersion. These copied estimates have no schema authority;
	// an explicitly bound incompatible observation still declines.
	ok = complete && !incompatible && (!versioned || boundVersion == version) && current == observed
	if ok {
		var minOK, maxOK, ndvOK, kindOK, nullOK bool
		var kind uint64
		out.Rows, out.Blocks = current.TableCnt, current.BlockNumber
		out.Min, minOK = current.MinValMap[column]
		out.Max, maxOK = current.MaxValMap[column]
		out.NDV, ndvOK = current.NdvMap[column]
		kind, kindOK = current.DataTypeMap[column]
		out.Kind = types.T(kind)
		out.Nulls, nullOK = current.NullCntMap[column]
		ok = minOK && maxOK && ndvOK && kindOK && nullOK && uint64(out.Kind) == kind
	}
	gs.mu.Unlock()
	ok = ok && context.Cause(gs.ctx) == nil && planningReadStillCurrent(tbl, ctx, read, t, txn)
	return out, ok, planningContextCause(ctx)
}

// VerifyIndexPair checks the two actual ordinary-resolver relations at
// the final plan publication seam. It performs no catalog lookup or storage IO.
func (tbl *txnTableDelegate) VerifyIndexPair(ctx context.Context, read client.CatalogReadStamp, check engine.IndexPairCheck, budget *int) (bool, error) {
	t, txn, ok := tbl.planningRead(ctx, read)
	hidden, native := check.Hidden.(*txnTableDelegate)
	if !ok || !native || hidden == nil || check.Index == nil || check.BaseFull == nil || check.BaseScan == nil || check.HiddenFull == nil || budget == nil || *budget <= 0 {
		return false, planningContextCause(ctx)
	}
	h, hiddenTxn, ok := hidden.planningRead(ctx, check.HiddenRead)
	if !ok || hiddenTxn != txn || !read.MatchesRead(check.HiddenRead, client.CatalogSnapshotSemantic) || h.eng != t.eng || h.accountId != t.accountId || h.db.databaseId != t.db.databaseId || h.relKind != catalog.SystemIndexRel || h.tableName != check.Index.IndexTableName || h.extraInfo == nil || h.extraInfo.GetParentTableID() != t.tableId || !indexPlanningRefMatches(check.BaseRef, t) || !indexPlanningRefMatches(check.HiddenRef, h) || !indexPlanningHiddenLayout(h.tableDef) || !indexPlanningHiddenLayout(check.HiddenFull) {
		return false, planningContextCause(ctx)
	}
	if len(check.BaseFull.Indexes) > engine.MaxIndexPlanningDefinitions {
		return false, planningContextCause(ctx)
	}
	belongs := false
	for _, index := range check.BaseFull.Indexes {
		if *budget <= 0 {
			return false, planningContextCause(ctx)
		}
		*budget--
		if index == check.Index {
			belongs = true
		}
	}
	if !belongs {
		return false, planningContextCause(ctx)
	}
	samples := []string{check.Column}
	bound, ok := copyIndexBaseSchemaWithBudget(check.BaseFull, check.Index.IndexTableName, samples, budget)
	if !ok || bound.BaseID != t.tableId || bound.BaseVersion != t.version {
		return false, planningContextCause(ctx)
	}
	actual, ok := copyIndexBaseSchemaWithBudget(t.tableDef, check.Index.IndexTableName, samples, budget)
	if !ok || actual != bound || !matchIndexProjectionWithBudget(check.BaseFull, check.BaseScan, &bound, budget) {
		return false, planningContextCause(ctx)
	}
	pair, ok := copyIndexSchemaPairWithBudget(t.tableDef, h.tableDef, check.Index.IndexTableName, samples, budget)
	if !ok || pair.BaseSchema != bound || pair.HiddenID != h.tableId || pair.HiddenVersion != h.version {
		return false, planningContextCause(ctx)
	}
	resolved, ok := copyIndexSchemaPairWithBudget(check.BaseFull, check.HiddenFull, check.Index.IndexTableName, samples, budget)
	valid := ok && resolved == pair && *budget > 0 && planningReadStillCurrent(tbl, ctx, read, t, txn) && planningReadStillCurrent(hidden, ctx, check.HiddenRead, h, txn)
	return valid, planningContextCause(ctx)
}

func indexPlanningRefMatches(ref *plan.ObjectRef, t *txnTable) bool {
	return ref != nil && ref.PubInfo == nil && ref.SubscriptionName == "" && (ref.Snapshot == nil || (ref.Snapshot.TS == nil || ref.Snapshot.TS.IsEmpty()) && ref.Snapshot.Tenant == nil && ref.Snapshot.ExtraInfo == nil) && ref.Obj > 0 && uint64(ref.Obj) == t.tableId && ref.ObjName == t.tableName && ref.SchemaName == t.db.databaseName
}
func indexPlanningHiddenLayout(def *plan.TableDef) bool {
	return def != nil && len(def.Cols) >= 2 && def.Cols[0] != nil && def.Cols[1] != nil && !types.IsUnicodeCollation(uint8(def.Cols[0].Typ.Charset)) && def.Cols[0].Name == catalog.IndexTableIndexColName && def.Cols[1].Name == catalog.IndexTablePrimaryColName
}

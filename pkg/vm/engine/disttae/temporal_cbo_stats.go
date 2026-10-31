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

var _ engine.TemporalIndexRelation = (*txnTableDelegate)(nil)

// This view borrows the canonical ordinary relation only for a synchronous
// root statement. It never reads a catalog/partition or creates an owner.
func (tbl *txnTableDelegate) temporalRead(ctx context.Context, read engine.TemporalRead) (*txnTable, *Transaction, bool) {
	if ctx == nil || context.Cause(ctx) != nil || tbl == nil || tbl.origin == nil || tbl.isMock || tbl.shard.is || tbl.combined.is || read.Op == nil || read.Op.IsSnapOp() || read.Stamp.Snapshot.IsEmpty() {
		return nil, nil, false
	}
	t := tbl.origin
	txn, ok := read.Stamp.Workspace.(*Transaction)
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
	if !ok || stamp.Workspace != txn || stamp.TransactionID != read.Stamp.TransactionID || !stamp.Snapshot.Equal(read.Stamp.Snapshot) || stamp.Revision != read.Stamp.Revision {
		return nil, nil, false
	}
	visibility, stable := txn.CatalogVisibility()
	return t, txn, stable && visibility == read.Visibility && context.Cause(ctx) == nil
}

func (tbl *txnTableDelegate) ValidateTemporalRead(ctx context.Context, read engine.TemporalRead) bool {
	_, _, ok := tbl.temporalRead(ctx, read)
	return ok
}

func temporalReadStillCurrent(tbl *txnTableDelegate, ctx context.Context, read engine.TemporalRead, t *txnTable, txn *Transaction) bool {
	current, workspace, ok := tbl.temporalRead(ctx, read)
	return ok && current == t && workspace == txn
}
func temporalContextCause(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return context.Cause(ctx)
}

// ReadTemporalCostStats copies one ordinary observation under its existing GS
// lock. Unbound metadata is a heuristic; no number authorizes executable rows.
func (tbl *txnTableDelegate) ReadTemporalCostStats(ctx context.Context, read engine.TemporalRead, observed *pb.StatsInfo, column string) (engine.TemporalStats, bool, error) {
	var out engine.TemporalStats
	t, txn, ok := tbl.temporalRead(ctx, read)
	if !ok || observed == nil || len(column) == 0 || len(column) > engine.MaxTemporalNameBytes {
		return out, false, temporalContextCause(ctx)
	}
	gs := txn.engine.globalStats
	if gs == nil || gs.ctx == nil || context.Cause(gs.ctx) != nil || !gs.mu.TryLock() {
		return out, false, temporalContextCause(ctx)
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
		if distribution := current.ShuffleRangeMap[column]; distribution != nil {
			out.Overlap, out.HasOverlap = distribution.Overlap, true
		}
		out.Min, minOK = current.MinValMap[column]
		out.Max, maxOK = current.MaxValMap[column]
		out.NDV, ndvOK = current.NdvMap[column]
		kind, kindOK = current.DataTypeMap[column]
		out.Kind = types.T(kind)
		out.Nulls, nullOK = current.NullCntMap[column]
		ok = minOK && maxOK && ndvOK && kindOK && nullOK && uint64(out.Kind) == kind
	}
	gs.mu.Unlock()
	ok = ok && context.Cause(gs.ctx) == nil && temporalReadStillCurrent(tbl, ctx, read, t, txn)
	return out, ok, temporalContextCause(ctx)
}

// VerifyTemporalIndexPair checks the two actual ordinary-resolver relations at
// the final plan publication seam. It performs no catalog lookup or storage IO.
func (tbl *txnTableDelegate) VerifyTemporalIndexPair(ctx context.Context, read engine.TemporalRead, check engine.TemporalPairCheck, budget *int) (bool, error) {
	t, txn, ok := tbl.temporalRead(ctx, read)
	hidden, native := check.Hidden.(*txnTableDelegate)
	if !ok || !native || hidden == nil || check.Index == nil || check.BaseFull == nil || check.BaseScan == nil || check.HiddenFull == nil || budget == nil || *budget <= 0 {
		return false, temporalContextCause(ctx)
	}
	h, hiddenTxn, ok := hidden.temporalRead(ctx, check.HiddenRead)
	if !ok || hiddenTxn != txn || read.Op != check.HiddenRead.Op || read.Visibility != check.HiddenRead.Visibility || read.Stamp.Revision != check.HiddenRead.Stamp.Revision || read.Stamp.TransactionID != check.HiddenRead.Stamp.TransactionID || !read.Stamp.Snapshot.Equal(check.HiddenRead.Stamp.Snapshot) || h.eng != t.eng || h.accountId != t.accountId || h.db.databaseId != t.db.databaseId || h.relKind != catalog.SystemIndexRel || h.tableName != check.Index.IndexTableName || h.extraInfo == nil || h.extraInfo.GetParentTableID() != t.tableId || !temporalBoundRefMatches(check.BaseRef, t) || !temporalBoundRefMatches(check.HiddenRef, h) || !temporalCBOHiddenLayout(h.tableDef) || !temporalCBOHiddenLayout(check.HiddenFull) {
		return false, temporalContextCause(ctx)
	}
	if len(check.BaseFull.Indexes) > engine.MaxTemporalIndexDefinitions {
		return false, temporalContextCause(ctx)
	}
	belongs := false
	for _, index := range check.BaseFull.Indexes {
		if *budget <= 0 {
			return false, temporalContextCause(ctx)
		}
		*budget--
		if index == check.Index {
			belongs = true
		}
	}
	if !belongs {
		return false, temporalContextCause(ctx)
	}
	samples := []string{check.Column}
	bound, ok := copyTemporalBaseSchemaWithBudget(check.BaseFull, check.Index.IndexTableName, samples, budget)
	if !ok || bound.BaseID != t.tableId || bound.BaseVersion != t.version {
		return false, temporalContextCause(ctx)
	}
	actual, ok := copyTemporalBaseSchemaWithBudget(t.tableDef, check.Index.IndexTableName, samples, budget)
	if !ok || actual != bound || !matchTemporalProjectionWithBudget(check.BaseFull, check.BaseScan, &bound, budget) {
		return false, temporalContextCause(ctx)
	}
	pair, ok := copyTemporalSchemaPairWithBudget(t.tableDef, h.tableDef, check.Index.IndexTableName, samples, budget)
	if !ok || pair.BaseSchema != bound || pair.HiddenID != h.tableId || pair.HiddenVersion != h.version {
		return false, temporalContextCause(ctx)
	}
	resolved, ok := copyTemporalSchemaPairWithBudget(check.BaseFull, check.HiddenFull, check.Index.IndexTableName, samples, budget)
	valid := ok && resolved == pair && *budget > 0 && temporalReadStillCurrent(tbl, ctx, read, t, txn) && temporalReadStillCurrent(hidden, ctx, check.HiddenRead, h, txn)
	return valid, temporalContextCause(ctx)
}

func temporalBoundRefMatches(ref *plan.ObjectRef, t *txnTable) bool {
	return ref != nil && ref.PubInfo == nil && ref.SubscriptionName == "" && (ref.Snapshot == nil || (ref.Snapshot.TS == nil || ref.Snapshot.TS.IsEmpty()) && ref.Snapshot.Tenant == nil && ref.Snapshot.ExtraInfo == nil) && ref.Obj > 0 && uint64(ref.Obj) == t.tableId && ref.ObjName == t.tableName && ref.SchemaName == t.db.databaseName
}
func temporalCBOHiddenLayout(def *plan.TableDef) bool {
	return def != nil && len(def.Cols) >= 2 && def.Cols[0] != nil && def.Cols[1] != nil && !types.IsUnicodeCollation(uint8(def.Cols[0].Typ.Charset)) && def.Cols[0].Name == catalog.IndexTableIndexColName && def.Cols[1].Name == catalog.IndexTablePrimaryColName
}

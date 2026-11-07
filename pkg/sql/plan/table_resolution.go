// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const maxBorrowedRelations = 64

// TableResolution is the optional borrowed result of the ordinary resolver.
// The caller owns this output, while the engine/transaction owns its relation.
type TableResolution struct{ Borrow *BorrowedTable }

// Reset clears stale evidence before every resolution attempt. A nil output
// means the caller does not request a borrowed relation.
func (output *TableResolution) Reset() {
	if output != nil {
		output.Borrow = nil
	}
}

// BorrowedTable belongs to the ordinary resolver's transaction. It is valid
// only while the current root query builder is optimizing; it owns no resource.
type BorrowedTable struct {
	Relation engine.Relation
	Read     client.CatalogReadStamp
	Owner    any
}

// scanBorrow connects the same resolver result to the actual scan that
// was created for it. Optimizer replacement cannot inherit another scan's read.
type scanBorrow struct {
	scanID          int32
	scan            *Node
	tag             int32
	database, table string
	full            *TableDef
	borrow          *BorrowedTable
}

func (builder *QueryBuilder) rememberScanBorrow(scanID int32, ref *ObjectRef, full *TableDef, borrow *BorrowedTable) {
	if builder.borrowResolutionDisabled || borrow == nil || borrow.Relation == nil || borrow.Read.Op == nil ||
		borrow.Read.Workspace == nil || borrow.Read.Snapshot.IsEmpty() || ref == nil || full == nil ||
		ref.PubInfo != nil || ref.SubscriptionName != "" || IsSnapshotValid(ref.Snapshot) || full.IsTemporary ||
		len(ref.SchemaName) == 0 || len(ref.SchemaName) > engine.MaxIndexPlanningNameBytes ||
		len(ref.ObjName) == 0 || len(ref.ObjName) > engine.MaxIndexPlanningNameBytes ||
		ref.Obj <= 0 || uint64(ref.Obj) != full.TblId || scanID < 0 || int(scanID) >= len(builder.qry.Nodes) {
		return
	}
	scan := builder.qry.Nodes[scanID]
	if scan == nil || scan.NodeType != planpb.Node_TABLE_SCAN || len(scan.BindingTags) == 0 {
		return
	}
	for _, previous := range builder.scanBorrows {
		if previous.full.TblId == full.TblId {
			builder.clearScanBorrows()
			builder.borrowResolutionDisabled = true
			return
		}
	}
	if len(builder.scanBorrows) >= maxBorrowedRelations {
		builder.clearScanBorrows()
		builder.borrowResolutionDisabled = true
		return
	}
	builder.scanBorrows = append(builder.scanBorrows, scanBorrow{
		scanID: scanID, scan: scan, tag: scan.BindingTags[0], database: ref.SchemaName, table: ref.ObjName, full: full, borrow: borrow,
	})
}

func (builder *QueryBuilder) scanBorrow(scanID int32) (*TableDef, *BorrowedTable, bool) {
	if builder.borrowResolutionDisabled || scanID < 0 || int(scanID) >= len(builder.qry.Nodes) {
		return nil, nil, false
	}
	scan := builder.qry.Nodes[scanID]
	for _, source := range builder.scanBorrows {
		if source.scanID == scanID && source.scan == scan && scan != nil && scan.ObjRef != nil && scan.TableDef != nil &&
			len(scan.BindingTags) != 0 && scan.BindingTags[0] == source.tag && scan.ObjRef.PubInfo == nil &&
			scan.ObjRef.SubscriptionName == "" && !IsSnapshotValid(scan.ObjRef.Snapshot) && scan.ObjRef.Obj > 0 &&
			uint64(scan.ObjRef.Obj) == source.full.TblId && scan.TableDef.TblId == source.full.TblId &&
			scan.TableDef.Version == source.full.Version && scan.ObjRef.SchemaName == source.database && scan.ObjRef.ObjName == source.table {
			return source.full, source.borrow, true
		}
	}
	return nil, nil, false
}

func (builder *QueryBuilder) clearScanBorrows() {
	clear(builder.scanBorrows)
	builder.scanBorrows = nil
}

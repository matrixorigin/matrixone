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
	"sync/atomic"
	"time"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const maxTemporalNativeRelations = 64

// TemporalPlanningIntent describes one execution, not a reusable plan.
type TemporalPlanningIntent struct {
	Placement     schedule.TemporalAccessIntent
	ParameterFree bool
	TimeZone      *time.Location
	UnixTime      int64
}

// MatchesProcess rejects changes to execution state used by candidate estimation.
func (intent TemporalPlanningIntent) MatchesProcess(proc *process.Process) bool {
	if proc == nil || proc.Base == nil || intent.TimeZone != proc.GetSessionInfo().TimeZone || intent.UnixTime != proc.Base.UnixTime {
		return false
	}
	if proc.Base.SessionInfo.ApplySQLSelectLimit {
		resolve := proc.GetResolveVariableFunc()
		if resolve == nil {
			return false
		}
		value, err := resolve(SQLSelectLimitVariable, true, false)
		limit, ok := value.(uint64)
		if err != nil || !ok || limit != ^uint64(0) {
			return false
		}
	}
	return true
}

// BorrowedTable belongs to the ordinary resolver's transaction. It is valid
// only while the current root query builder is optimizing; it owns no resource.
type BorrowedTable struct {
	Relation engine.Relation
	Read     engine.TemporalRead
	Owner    any
}

// BorrowingTableResolver returns the descriptor and optional native read from
// one ordinary resolution. The frontend supplies it only for its exact compiler.
type BorrowingTableResolver interface {
	IsSameCompiler(CompilerContext) bool
	ResolveBaseForPlanning(string, string, *Snapshot) (*ObjectRef, *TableDef, *BorrowedTable, error)
	ResolveIndexForPlanning(*ObjectRef, string, *Snapshot) (*ObjectRef, *TableDef, *BorrowedTable, error)
	BorrowStillCurrent(CompilerContext, *process.Process, *BorrowedTable) bool
}

// TemporalPlanningInput is passed only through transparent root wrappers. Each
// optimizer attempt claims it once; it is never retained in the resulting plan.
type TemporalPlanningInput struct {
	root     *tree.Select
	intent   TemporalPlanningIntent
	resolver BorrowingTableResolver
	claimed  atomic.Bool
}

func NewTemporalPlanningInput(root *tree.Select, intent TemporalPlanningIntent, resolver BorrowingTableResolver) *TemporalPlanningInput {
	if root == nil {
		return nil
	}
	return &TemporalPlanningInput{root: root, intent: intent, resolver: resolver}
}

func (input *TemporalPlanningInput) claim(root *tree.Select) (TemporalPlanningIntent, BorrowingTableResolver, bool) {
	if input == nil || root == nil || input.root != root || !input.claimed.CompareAndSwap(false, true) {
		return TemporalPlanningIntent{}, nil, false
	}
	return input.intent, input.resolver, true
}

// temporalScanBorrow connects the same resolver result to the actual scan that
// was created for it. Optimizer replacement cannot inherit another scan's read.
type temporalScanBorrow struct {
	scanID          int32
	scan            *Node
	tag             int32
	database, table string
	full            *TableDef
	borrow          *BorrowedTable
}

func (builder *QueryBuilder) rememberTemporalScan(scanID int32, ref *ObjectRef, full *TableDef, borrow *BorrowedTable) {
	if builder.temporalBorrowDisabled || borrow == nil || borrow.Relation == nil || borrow.Read.Op == nil ||
		borrow.Read.Stamp.Workspace == nil || borrow.Read.Stamp.Snapshot.IsEmpty() || ref == nil || full == nil ||
		ref.PubInfo != nil || ref.SubscriptionName != "" || IsSnapshotValid(ref.Snapshot) || full.IsTemporary ||
		len(ref.SchemaName) == 0 || len(ref.SchemaName) > engine.MaxTemporalNameBytes ||
		len(ref.ObjName) == 0 || len(ref.ObjName) > engine.MaxTemporalNameBytes ||
		ref.Obj <= 0 || uint64(ref.Obj) != full.TblId || scanID < 0 || int(scanID) >= len(builder.qry.Nodes) {
		return
	}
	scan := builder.qry.Nodes[scanID]
	if scan == nil || scan.NodeType != planpb.Node_TABLE_SCAN || len(scan.BindingTags) == 0 {
		return
	}
	for _, previous := range builder.temporalScanBorrows {
		if previous.full.TblId == full.TblId {
			builder.clearTemporalBorrows()
			builder.temporalBorrowDisabled = true
			return
		}
	}
	if len(builder.temporalScanBorrows) >= maxTemporalNativeRelations {
		builder.clearTemporalBorrows()
		builder.temporalBorrowDisabled = true
		return
	}
	builder.temporalScanBorrows = append(builder.temporalScanBorrows, temporalScanBorrow{
		scanID: scanID, scan: scan, tag: scan.BindingTags[0], database: ref.SchemaName, table: ref.ObjName, full: full, borrow: borrow,
	})
}

func (builder *QueryBuilder) temporalScanBorrow(scanID int32) (*TableDef, *BorrowedTable, bool) {
	if builder.temporalBorrowDisabled || scanID < 0 || int(scanID) >= len(builder.qry.Nodes) {
		return nil, nil, false
	}
	scan := builder.qry.Nodes[scanID]
	for _, source := range builder.temporalScanBorrows {
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

func (builder *QueryBuilder) clearTemporalBorrows() {
	clear(builder.temporalScanBorrows)
	builder.temporalScanBorrows = nil
}

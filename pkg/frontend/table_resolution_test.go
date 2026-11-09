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

package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/txn/client"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	mockfrontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestTemporalNativeSnapshotOverridePreservesCurrentReads(t *testing.T) {
	for _, snapshot := range []*pbplan.Snapshot{nil, {}, {TS: &timestamp.Timestamp{}}} {
		require.False(t, borrowSnapshotOverride(snapshot))
	}
	for _, snapshot := range []*pbplan.Snapshot{
		{TS: &timestamp.Timestamp{PhysicalTime: 1}},
		{Tenant: &pbplan.SnapshotTenant{}},
		{ExtraInfo: &pbplan.SnapshotExtraInfo{}},
	} {
		require.True(t, borrowSnapshotOverride(snapshot))
	}
}

type temporalCaptureRelation struct{ engine.Relation }

func (*temporalCaptureRelation) ReadIndexCostStats(context.Context, client.CatalogReadStamp, *statsinfo.StatsInfo, string) (engine.IndexCostStats, bool, error) {
	return engine.IndexCostStats{}, false, nil
}
func (*temporalCaptureRelation) ObserveIndexBlockInput(context.Context, client.CatalogReadStamp, engine.IndexBlockInputRequest, *int) (engine.IndexBlockInput, bool, error) {
	return engine.IndexBlockInput{}, false, nil
}
func (*temporalCaptureRelation) VerifyIndexPair(context.Context, client.CatalogReadStamp, engine.IndexPairCheck, *int) (bool, error) {
	return false, nil
}

func TestTemporalNativeReadIsLexicalAndRejectsChangedOwner(t *testing.T) {
	for _, scenario := range []string{"current", "snapshot changed", "snapshot ABA", "cancelled", "deadline", "nested resolve", "nested execution restored", "execution replaced", "handler removed", "session removed"} {
		t.Run(scenario, func(t *testing.T) {
			f := newViewSchemaOwnedFixture(t)
			f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 3})
			var capture tableReadCapture
			capture.begin(f.parent, f.op)
			require.True(t, capture.valid)
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			relation := &temporalCaptureRelation{}
			ref := &pbplan.ObjectRef{Obj: 42, SchemaName: "db", ObjName: "t"}
			full := &pbplan.TableDef{TblId: 42, Name: "t"}
			switch scenario {
			case "snapshot changed":
				f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 4})
			case "snapshot ABA":
				f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 4})
				f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 3})
			case "cancelled":
				cancel()
			case "deadline":
				var cancelDeadline context.CancelFunc
				ctx, cancelDeadline = context.WithDeadline(f.ctx, time.Now().Add(-time.Second))
				defer cancelDeadline()
			case "nested resolve":
				var nested tableReadCapture
				nested.begin(f.parent, f.op)
				require.NotNil(t, nested.finish(f.parent, ctx, relation, ref, full))
			case "nested execution restored":
				previous := f.parent.execCtx
				f.parent.SetExecCtx(&ExecCtx{reqCtx: f.ctx, ses: f.session, proc: f.proc})
				var nested tableReadCapture
				nested.begin(f.parent, f.op)
				nestedBorrow := nested.finish(f.parent, ctx, relation, ref, full)
				require.NotNil(t, nestedBorrow)
				f.parent.SetExecCtx(previous)
				require.False(t, f.parent.BorrowedTableStillCurrent(f.proc, nestedBorrow))
			case "execution replaced":
				previous := f.parent.execCtx
				f.parent.SetExecCtx(&ExecCtx{reqCtx: f.ctx, ses: f.session, proc: f.proc})
				defer f.parent.SetExecCtx(previous)
			case "handler removed":
				handler := f.session.GetTxnHandler()
				f.session.mu.Lock()
				f.session.txnHandler = nil
				f.session.mu.Unlock()
				defer func() {
					f.session.mu.Lock()
					f.session.txnHandler = handler
					f.session.mu.Unlock()
				}()
			case "session removed":
				session := f.parent.execCtx.ses
				f.parent.execCtx.ses = nil
				defer func() { f.parent.execCtx.ses = session }()
			}
			borrow := capture.finish(f.parent, ctx, relation, ref, full)
			if scenario == "current" || scenario == "nested resolve" || scenario == "nested execution restored" {
				require.NotNil(t, borrow)
				require.Same(t, relation, borrow.Relation)
				require.Same(t, f.op, borrow.Read.Op)
				require.Same(t, f.workspace, borrow.Read.Workspace)
				require.True(t, f.parent.BorrowedTableStillCurrent(f.proc, borrow))
			} else {
				require.Nil(t, borrow)
			}
		})
	}
}

func TestPlanningResolverRejectsPromotedWrapper(t *testing.T) {
	compiler := &TxnCompilerContext{}
	wrapper := &viewSchemaCompilerContext{TxnCompilerContext: compiler}
	require.Same(t, compiler, planningCompiler(compiler))
	require.Nil(t, planningCompiler(wrapper))
	// This method is promoted by Go embedding; it must not turn the wrapper's
	// publisher-specific Resolve into an ordinary compiler lookup.
	require.Nil(t, planningCompiler(nil))
	var typedNil *TxnCompilerContext
	require.Nil(t, planningCompiler(typedNil))
}

func TestPlanningResolverUsesOneOrdinaryLookup(t *testing.T) {
	for _, index := range []bool{false, true} {
		t.Run(map[bool]string{false: "base", true: "index"}[index], func(t *testing.T) {
			f := newViewSchemaOwnedFixture(t)
			f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 3})
			ctrl := gomock.NewController(t)
			storage := mockfrontend.NewMockEngine(ctrl)
			db := mockfrontend.NewMockDatabase(ctrl)
			relation := mockfrontend.NewMockRelation(ctrl)
			f.session.txnHandler.storage = storage
			definition := &pbplan.TableDef{TblId: 42, Name: "events", TableType: catalog.SystemOrdinaryRel}
			storage.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, f.op).Return(db, nil).Times(1)
			db.EXPECT().Relation(gomock.Any(), "events", nil).Return(&temporalCaptureRelation{Relation: relation}, nil).Times(1)
			relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(42)).Times(1)
			relation.EXPECT().GetTableDef(gomock.Any()).Return(definition).Times(1)
			var obj *pbplan.ObjectRef
			var def *pbplan.TableDef
			var borrow *plan.BorrowedTable
			var err error
			if index {
				base := &pbplan.ObjectRef{Obj: 1, SchemaName: catalog.MO_CATALOG, ObjName: "base"}
				var resolvedBorrow1 plan.TableResolution
				obj, def, err = f.parent.ResolveIndexTableByRef(base, "events", nil, &resolvedBorrow1)
				borrow = resolvedBorrow1.Borrow
			} else {
				var resolvedBorrow2 plan.TableResolution
				obj, def, err = f.parent.Resolve(catalog.MO_CATALOG, "events", nil, &resolvedBorrow2)
				borrow = resolvedBorrow2.Borrow
			}
			require.NoError(t, err)
			require.EqualValues(t, 42, obj.Obj)
			require.EqualValues(t, 42, def.TblId)
			require.NotNil(t, borrow)
			require.Same(t, f.op, borrow.Read.Op)
			require.True(t, f.parent.BorrowedTableStillCurrent(f.proc, borrow))
		})
	}
}

// Catalog validation may inspect optimizer-generated index nodes, but it must
// not turn that metadata authority into a borrowed user relation.
func TestBorrowedResolutionPreservesIndexAdmission(t *testing.T) {
	for _, catalogOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "user denied", true: "catalog identity only"}[catalogOnly], func(t *testing.T) {
			f := newViewSchemaOwnedFixture(t)
			f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 3})
			f.session.SetFromRealUser(true)
			f.session.SetTenantInfo(&TenantInfo{Tenant: "sys", User: "reader", DefaultRole: "public"})
			ctrl := gomock.NewController(t)
			storage := mockfrontend.NewMockEngine(ctrl)
			db := mockfrontend.NewMockDatabase(ctrl)
			relation := mockfrontend.NewMockRelation(ctrl)
			f.session.txnHandler.storage = storage
			definition := &pbplan.TableDef{TblId: 42, Name: "events", TableType: catalog.SystemIndexRel}
			storage.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, f.op).Return(db, nil).Times(1)
			db.EXPECT().Relation(gomock.Any(), "events", nil).Return(&temporalCaptureRelation{Relation: relation}, nil).Times(1)
			relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(42)).Times(1)
			relation.EXPECT().GetTableDef(gomock.Any()).Return(definition).Times(1)
			output := plan.TableResolution{Borrow: &plan.BorrowedTable{Owner: f.parent.execCtx}}
			resolve := f.parent.Resolve
			if catalogOnly {
				resolve = f.parent.resolveForCatalogValidation
			}
			obj, def, err := resolve(catalog.MO_CATALOG, "events", nil, &output)
			require.Nil(t, output.Borrow)
			if catalogOnly {
				require.NoError(t, err)
				require.EqualValues(t, 42, obj.Obj)
				require.EqualValues(t, 42, def.TblId)
			} else {
				require.ErrorContains(t, err, "do not have privilege")
				require.Nil(t, obj)
				require.Nil(t, def)
			}
		})
	}
}

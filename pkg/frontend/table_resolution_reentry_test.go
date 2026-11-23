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
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	mockfrontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestPlanningResolverNestedActualLookupKeepsOwner(t *testing.T) {
	f := newViewSchemaOwnedFixture(t)
	f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 3})
	ctrl := gomock.NewController(t)
	storage := mockfrontend.NewMockEngine(ctrl)
	db := mockfrontend.NewMockDatabase(ctrl)
	outerMock := mockfrontend.NewMockRelation(ctrl)
	innerMock := mockfrontend.NewMockRelation(ctrl)
	outer := &temporalCaptureRelation{Relation: outerMock}
	inner := &temporalCaptureRelation{Relation: innerMock}
	f.session.txnHandler.storage = storage
	storage.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, f.op).Return(db, nil).Times(2)
	innerMock.EXPECT().GetTableID(gomock.Any()).Return(uint64(43)).Times(1)
	innerMock.EXPECT().GetTableDef(gomock.Any()).Return(&pbplan.TableDef{TblId: 43, Name: "inner"}).Times(1)
	outerMock.EXPECT().GetTableID(gomock.Any()).Return(uint64(42)).Times(1)
	outerMock.EXPECT().GetTableDef(gomock.Any()).Return(&pbplan.TableDef{TblId: 42, Name: "outer"}).Times(1)
	db.EXPECT().Relation(gomock.Any(), "inner", nil).Return(inner, nil).Times(1)
	db.EXPECT().Relation(gomock.Any(), "outer", nil).DoAndReturn(func(_ context.Context, _ string, _ any) (engine.Relation, error) {
		previous := f.parent.execCtx
		f.parent.SetExecCtx(&ExecCtx{reqCtx: f.ctx, ses: f.session, proc: f.proc})
		defer f.parent.SetExecCtx(previous)
		var resolvedBorrow1 plan.TableResolution
		ref, def, err := f.parent.Resolve(catalog.MO_CATALOG, "inner", nil, &resolvedBorrow1)
		borrow := resolvedBorrow1.Borrow
		require.NoError(t, err)
		require.EqualValues(t, 43, ref.Obj)
		require.EqualValues(t, 43, def.TblId)
		require.NotNil(t, borrow)
		require.Same(t, inner, borrow.Relation)
		require.True(t, f.parent.BorrowedTableStillCurrent(f.proc, borrow))
		return outer, nil
	}).Times(1)
	var resolvedBorrow2 plan.TableResolution
	ref, def, err := f.parent.Resolve(catalog.MO_CATALOG, "outer", nil, &resolvedBorrow2)
	borrow := resolvedBorrow2.Borrow
	require.NoError(t, err)
	require.EqualValues(t, 42, ref.Obj)
	require.EqualValues(t, 42, def.TblId)
	require.NotNil(t, borrow)
	require.Same(t, outer, borrow.Relation)
	require.True(t, f.parent.BorrowedTableStillCurrent(f.proc, borrow))
}

func TestPlanningResolverCancellationDuringActualLookup(t *testing.T) {
	f := newViewSchemaOwnedFixture(t)
	f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 3})
	ctx, cancel := context.WithCancelCause(f.ctx)
	defer cancel(nil)
	f.parent.SetContext(ctx)
	ctrl := gomock.NewController(t)
	storage := mockfrontend.NewMockEngine(ctrl)
	db := mockfrontend.NewMockDatabase(ctrl)
	mock := mockfrontend.NewMockRelation(ctrl)
	f.session.txnHandler.storage = storage
	storage.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, f.op).Return(db, nil).Times(1)
	cause := errors.New("stop while resolving")
	db.EXPECT().Relation(gomock.Any(), "events", nil).DoAndReturn(func(context.Context, string, any) (engine.Relation, error) {
		cancel(cause)
		return &temporalCaptureRelation{Relation: mock}, nil
	}).Times(1)
	mock.EXPECT().GetTableDef(gomock.Any()).Return(&pbplan.TableDef{TblId: 42, Name: "events"}).Times(1)
	resolvedBorrow3 := plan.TableResolution{Borrow: &plan.BorrowedTable{Owner: f.parent.execCtx}}
	ref, def, err := f.parent.Resolve(catalog.MO_CATALOG, "events", nil, &resolvedBorrow3)
	borrow := resolvedBorrow3.Borrow
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ref)
	require.Nil(t, def)
	require.Nil(t, borrow)
	require.ErrorIs(t, context.Cause(ctx), cause)
}

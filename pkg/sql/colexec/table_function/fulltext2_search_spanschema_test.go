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

package table_function

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func ftSpanSchemaDef(version uint32) *plan.TableDef {
	return &plan.TableDef{
		TblId:         42,
		Version:       version,
		TableType:     catalog.SystemOrdinaryRel,
		Cols:          []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, Primary: true}},
		Name2ColIndex: map[string]int32{"id": 0},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
	}
}

// TestProbeTailSpansSchema drives the runtime schema-version span check the operator uses to choose the
// behind-tail vs the full-scan fallback. It resolves the source's TableDef at the searched generation
// and at the read via table_changes' own helper and compares them, fail-closed.
func TestProbeTailSpansSchema(t *testing.T) {
	// No engine on the context: cannot resolve, so fail closed to a span (=> fallback).
	t.Run("no engine", func(t *testing.T) {
		proc := testutil.NewProc(t)
		defer proc.Free()
		require.True(t, probeTailState().probeTailSpansSchema(proc, 100))
	})

	setup := func(t *testing.T) (*process.Process, *mock_frontend.MockEngine, *mock_frontend.MockDatabase, *mock_frontend.MockRelation, *gomock.Controller) {
		ctrl := gomock.NewController(t)
		proc := testutil.NewProc(t)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		eng := mock_frontend.NewMockEngine(ctrl)
		db := mock_frontend.NewMockDatabase(ctrl)
		rel := mock_frontend.NewMockRelation(ctrl)
		txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(5000, 0).ToTimestamp()).AnyTimes()
		txnOp.EXPECT().CloneSnapshotOp(gomock.Any()).Return(txnOp).AnyTimes()
		proc.Base.TxnOperator = txnOp
		proc.Ctx = context.WithValue(proc.Ctx, defines.EngineKey{}, engine.Engine(eng))
		return proc, eng, db, rel, ctrl
	}

	// Both endpoints resolve to the same TblId+Version: one schema version, no span => tail (false).
	t.Run("same version", func(t *testing.T) {
		proc, eng, db, rel, ctrl := setup(t)
		defer ctrl.Finish()
		defer proc.Free()
		eng.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(db, nil).Times(2)
		db.EXPECT().Relation(gomock.Any(), "t", nil).Return(rel, nil).Times(2)
		rel.EXPECT().CopyTableDef(gomock.Any()).Return(ftSpanSchemaDef(3)).Times(2)
		require.False(t, probeTailState().probeTailSpansSchema(proc, 100))
	})

	// A schema-version change between the searched generation and the read: span => fallback (true).
	t.Run("different version", func(t *testing.T) {
		proc, eng, db, rel, ctrl := setup(t)
		defer ctrl.Finish()
		defer proc.Free()
		eng.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(db, nil).Times(2)
		db.EXPECT().Relation(gomock.Any(), "t", nil).Return(rel, nil).Times(2)
		gomock.InOrder(
			rel.EXPECT().CopyTableDef(gomock.Any()).Return(ftSpanSchemaDef(2)),
			rel.EXPECT().CopyTableDef(gomock.Any()).Return(ftSpanSchemaDef(3)),
		)
		require.True(t, probeTailState().probeTailSpansSchema(proc, 100))
	})

	// A non-BadDB resolve error is fail-closed to a span (true) so the operator takes the safe fallback.
	t.Run("resolve error", func(t *testing.T) {
		proc, eng, _, _, ctrl := setup(t)
		defer ctrl.Finish()
		defer proc.Free()
		eng.EXPECT().Database(gomock.Any(), "db", gomock.Any()).
			Return(nil, moerr.NewInternalErrorNoCtx("boom")).Times(2)
		require.True(t, probeTailState().probeTailSpansSchema(proc, 100))
	})
}

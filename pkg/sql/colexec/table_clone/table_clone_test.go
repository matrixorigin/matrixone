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

package table_clone

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestTableCloneOperatorMetadata(t *testing.T) {
	tc := &TableClone{}
	var buf bytes.Buffer

	require.NotPanics(t, func() {
		vm.String(tc, &buf)
	})
	require.Equal(t, "TableClone", buf.String())
	require.NotEqual(t, vm.Top, tc.OpType())
	require.Equal(t, "TableClone", tc.OpType().String())
}

type autoIncrementTestRelation struct {
	engine.Relation
	tableID uint64
	name    string
	def     *plan.TableDef
}

func (r *autoIncrementTestRelation) GetTableID(context.Context) uint64 {
	return r.tableID
}

func (r *autoIncrementTestRelation) GetTableDef(context.Context) *plan.TableDef {
	return r.def
}

func (r *autoIncrementTestRelation) GetTableName() string {
	return r.name
}

func TestUpdateDstAutoIncrColumnsReconcilesAllSafeBounds(t *testing.T) {
	tests := []struct {
		name      string
		requested uint64
		copiedMax uint64
		srcOffset uint64
		want      uint64
	}{
		{name: "requested offset wins", requested: 99, copiedMax: 40, srcOffset: 50, want: 99},
		{name: "copied maximum wins", requested: 99, copiedMax: 200, srcOffset: 50, want: 200},
		{name: "source allocator wins", requested: 99, copiedMax: 40, srcOffset: 300, want: 300},
		{name: "empty source", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			ctrl := gomock.NewController(t)
			incrSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
			proc.Base.IncrService = incrSvc

			def := &plan.TableDef{
				TblId: 42,
				Cols: []*plan.ColDef{{
					Name: "id",
					Typ:  plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
				}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
			}
			tc := &TableClone{
				Ctx: &TableCloneCtx{
					RequestedAutoIncrOffset: tt.requested,
					SrcAutoIncrMaxValues:    map[string]uint64{"id": tt.copiedMax},
					SrcAutoIncrOffsets:      map[string]uint64{"id": tt.srcOffset},
				},
				dstMasterRel: &autoIncrementTestRelation{tableID: def.TblId, def: def},
			}

			incrSvc.EXPECT().SetOffset(
				gomock.Any(), def.TblId, "id", tt.want, gomock.Any(),
			)
			require.NoError(t, tc.updateDstAutoIncrColumns(proc.Ctx, proc))
		})
	}
}

func TestUpdateDstAutoIncrColumnsKeepsHiddenAllocatorIndependent(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctrl := gomock.NewController(t)
	incrSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	proc.Base.IncrService = incrSvc

	def := &plan.TableDef{
		TblId: 42,
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
			{Name: "__mo_fake_pk_col", Hidden: true, Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
	}
	tc := &TableClone{
		Ctx: &TableCloneCtx{
			RequestedAutoIncrOffset: 999,
			SrcAutoIncrMaxValues:    map[string]uint64{"id": 40},
			SrcAutoIncrOffsets: map[string]uint64{
				"id":               50,
				"__mo_fake_pk_col": 40,
			},
		},
		dstMasterRel: &autoIncrementTestRelation{tableID: def.TblId, def: def},
	}

	gomock.InOrder(
		incrSvc.EXPECT().SetOffset(gomock.Any(), def.TblId, "id", uint64(999), gomock.Any()),
		incrSvc.EXPECT().SetOffset(gomock.Any(), def.TblId, "__mo_fake_pk_col", uint64(40), gomock.Any()),
	)
	require.NoError(t, tc.updateDstAutoIncrColumns(proc.Ctx, proc))
}

func TestUpdateDstAutoIncrColumnsReconcilesClonedIndexAllocator(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctrl := gomock.NewController(t)
	incrSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	proc.Base.IncrService = incrSvc

	const indexTableName = "__mo_index_fulltext_target"
	def := &plan.TableDef{
		TblId: 84,
		Name:  indexTableName,
		Cols: []*plan.ColDef{{
			Name:   "__mo_fake_pk_col",
			Hidden: true,
			Typ:    plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "__mo_fake_pk_col"},
	}
	tc := &TableClone{
		Ctx: &TableCloneCtx{
			IndexAutoIncrStates: map[string]AutoIncrementState{
				"ftidx.": {
					MaxValues: map[string]uint64{"__mo_fake_pk_col": 120},
					Offsets:   map[string]uint64{"__mo_fake_pk_col": 200},
				},
				"p0.ftidx.": {
					MaxValues: map[string]uint64{"__mo_fake_pk_col": 300},
					Offsets:   map[string]uint64{"__mo_fake_pk_col": 250},
				},
				"p1.ftidx.": {
					MaxValues: map[string]uint64{"__mo_fake_pk_col": 350},
					Offsets:   map[string]uint64{"__mo_fake_pk_col": 400},
				},
			},
		},
		dstIdxRel: map[string]engine.Relation{
			"ftidx.":    &autoIncrementTestRelation{tableID: 84, name: def.Name, def: def},
			"p0.ftidx.": &autoIncrementTestRelation{tableID: 85, name: def.Name, def: def},
			"p1.ftidx.": &autoIncrementTestRelation{tableID: 86, name: def.Name, def: def},
		},
	}

	incrSvc.EXPECT().SetOffset(gomock.Any(), uint64(84), "__mo_fake_pk_col", uint64(200), gomock.Any())
	incrSvc.EXPECT().SetOffset(gomock.Any(), uint64(85), "__mo_fake_pk_col", uint64(300), gomock.Any())
	incrSvc.EXPECT().SetOffset(gomock.Any(), uint64(86), "__mo_fake_pk_col", uint64(400), gomock.Any())
	require.NoError(t, tc.updateDstAutoIncrColumns(proc.Ctx, proc))
}

func TestUpdateDstAutoIncrColumnsRejectsOutOfRangeOffset(t *testing.T) {
	tests := []struct {
		name    string
		oid     types.T
		value   uint64
		wantErr bool
	}{
		{name: "uint8 maximum", oid: types.T_uint8, value: math.MaxUint8},
		{name: "uint8 overflow", oid: types.T_uint8, value: math.MaxUint8 + 1, wantErr: true},
		{name: "int8 maximum", oid: types.T_int8, value: math.MaxInt8},
		{name: "int8 overflow", oid: types.T_int8, value: math.MaxInt8 + 1, wantErr: true},
		{name: "uint64 maximum", oid: types.T_uint64, value: math.MaxUint64},
		{name: "int64 maximum", oid: types.T_int64, value: math.MaxInt64},
		{name: "int64 overflow", oid: types.T_int64, value: uint64(math.MaxInt64) + 1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			ctrl := gomock.NewController(t)
			incrSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
			proc.Base.IncrService = incrSvc
			def := &plan.TableDef{
				TblId: 42,
				Cols: []*plan.ColDef{{
					Name: "id",
					Typ:  plan.Type{Id: int32(tt.oid), AutoIncr: true},
				}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
			}
			tc := &TableClone{
				Ctx: &TableCloneCtx{
					RequestedAutoIncrOffset: tt.value,
					SrcAutoIncrMaxValues:    map[string]uint64{"id": 0},
				},
				dstMasterRel: &autoIncrementTestRelation{tableID: def.TblId, def: def},
			}

			if !tt.wantErr {
				incrSvc.EXPECT().SetOffset(
					gomock.Any(), def.TblId, "id", tt.value, gomock.Any(),
				).DoAndReturn(func(context.Context, uint64, string, uint64, client.TxnOperator) error {
					return nil
				})
			}

			err := tc.updateDstAutoIncrColumns(proc.Ctx, proc)
			if tt.wantErr {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

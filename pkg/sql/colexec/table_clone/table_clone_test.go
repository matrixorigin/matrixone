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

package table_clone

import (
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type autoIncrementTestRelation struct {
	engine.Relation
	tableID uint64
	def     *plan.TableDef
}

func (r *autoIncrementTestRelation) GetTableID(context.Context) uint64 {
	return r.tableID
}

func (r *autoIncrementTestRelation) GetTableDef(context.Context) *plan.TableDef {
	return r.def
}

func TestUpdateDstAutoIncrColumnsReconcilesRequestedOffsetWithCopiedMaximum(t *testing.T) {
	tests := []struct {
		name      string
		requested uint64
		copiedMax uint64
		want      uint64
	}{
		{name: "requested offset wins", requested: 99, copiedMax: 40, want: 99},
		{name: "copied maximum wins", requested: 99, copiedMax: 200, want: 200},
		{name: "empty or deleted source", requested: 99, copiedMax: 0, want: 99},
		{name: "zero request on empty source", requested: 0, copiedMax: 0, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			ctrl := gomock.NewController(t)
			incrSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
			proc.Base.IncrService = incrSvc

			def := &plan.TableDef{
				TblId:          42,
				Version:        7,
				AutoIncrEpoch:  3,
				AutoIncrOffset: tt.requested,
				Cols: []*plan.ColDef{{
					Name: "id",
					Typ:  plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
				}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
			}
			rel := &autoIncrementTestRelation{tableID: def.TblId, def: def}
			tc := &TableClone{
				Ctx: &TableCloneCtx{
					RequestedAutoIncrOffset: tt.requested,
					SrcAutoIncrMaxValues:    map[string]uint64{"id": tt.copiedMax},
				},
				dstMasterRel: rel,
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
		TblId:          42,
		Version:        7,
		AutoIncrEpoch:  3,
		AutoIncrOffset: 999,
		Cols: []*plan.ColDef{
			{
				Name: "id",
				Typ:  plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
			},
			{
				Name:   "__mo_fake_pk_col",
				Hidden: true,
				Typ:    plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
			},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
	}
	tc := &TableClone{
		Ctx: &TableCloneCtx{
			RequestedAutoIncrOffset: 999,
			SrcAutoIncrMaxValues:    map[string]uint64{"id": 40},
			SrcAutoIncrOffsets:      map[string]uint64{"__mo_fake_pk_col": 40},
		},
		dstMasterRel: &autoIncrementTestRelation{tableID: def.TblId, def: def},
	}

	gomock.InOrder(
		incrSvc.EXPECT().SetOffset(gomock.Any(), def.TblId, "id", uint64(999), gomock.Any()),
		incrSvc.EXPECT().SetOffset(gomock.Any(), def.TblId, "__mo_fake_pk_col", uint64(40), gomock.Any()),
	)

	require.NoError(t, tc.updateDstAutoIncrColumns(proc.Ctx, proc))
}

func TestUpdateDstAutoIncrColumnsUsesSourceAllocatorOffset(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctrl := gomock.NewController(t)
	incrSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	proc.Base.IncrService = incrSvc

	def := &plan.TableDef{
		TblId:         42,
		AutoIncrEpoch: 3,
		Cols: []*plan.ColDef{{
			Name: "id",
			Typ:  plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
	}
	tc := &TableClone{
		Ctx: &TableCloneCtx{
			SrcAutoIncrMaxValues: map[string]uint64{"id": 40},
			SrcAutoIncrOffsets:   map[string]uint64{"id": 50},
		},
		dstMasterRel: &autoIncrementTestRelation{tableID: def.TblId, def: def},
	}

	incrSvc.EXPECT().SetOffset(gomock.Any(), def.TblId, "id", uint64(50), gomock.Any())

	require.NoError(t, tc.updateDstAutoIncrColumns(proc.Ctx, proc))
}

func TestUpdateDstAutoIncrColumnsRejectsOutOfRangeEffectiveOffset(t *testing.T) {
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
				TblId:         42,
				Version:       7,
				AutoIncrEpoch: 3,
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

			setOffsetCalls := 0
			if !tt.wantErr {
				incrSvc.EXPECT().SetOffset(
					gomock.Any(), def.TblId, "id", tt.value, gomock.Any(),
				).DoAndReturn(func(context.Context, uint64, string, uint64, client.TxnOperator) error {
					setOffsetCalls++
					return nil
				})
			}

			err := tc.updateDstAutoIncrColumns(proc.Ctx, proc)
			if tt.wantErr {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
				require.ErrorContains(t, err, "AUTO_INCREMENT value")
				require.Zero(t, setOffsetCalls)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, setOffsetCalls)
			}
		})
	}
}

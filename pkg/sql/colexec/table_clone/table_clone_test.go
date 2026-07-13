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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
					SrcAutoIncrMaxValues:    map[int32]uint64{0: tt.copiedMax},
				},
				dstMasterRel: rel,
			}

			incrSvc.EXPECT().InsertValues(
				gomock.Any(), def.TblId, def.Version, gomock.Any(), 1, int64(1),
			).DoAndReturn(func(
				_ context.Context,
				_ uint64,
				_ uint32,
				vecs []*vector.Vector,
				_ int,
				_ int64,
			) (uint64, error) {
				require.Equal(t, tt.want, vector.MustFixedColWithTypeCheck[uint64](vecs[0])[0])
				return 0, nil
			})

			require.NoError(t, tc.updateDstAutoIncrColumns(proc.Ctx, proc))
		})
	}
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
				TblId:   42,
				Version: 7,
				Cols: []*plan.ColDef{{
					Name: "id",
					Typ:  plan.Type{Id: int32(tt.oid), AutoIncr: true},
				}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
			}
			tc := &TableClone{
				Ctx: &TableCloneCtx{
					RequestedAutoIncrOffset: tt.value,
					SrcAutoIncrMaxValues:    map[int32]uint64{0: 0},
				},
				dstMasterRel: &autoIncrementTestRelation{tableID: def.TblId, def: def},
			}

			insertCalls := 0
			incrSvc.EXPECT().InsertValues(
				gomock.Any(), def.TblId, def.Version, gomock.Any(), 1, int64(1),
			).DoAndReturn(func(
				_ context.Context,
				_ uint64,
				_ uint32,
				vecs []*vector.Vector,
				_ int,
				_ int64,
			) (uint64, error) {
				insertCalls++
				switch tt.oid {
				case types.T_uint8:
					require.Equal(t, uint8(tt.value), vector.MustFixedColWithTypeCheck[uint8](vecs[0])[0])
				case types.T_int8:
					require.Equal(t, int8(tt.value), vector.MustFixedColWithTypeCheck[int8](vecs[0])[0])
				case types.T_uint64:
					require.Equal(t, tt.value, vector.MustFixedColWithTypeCheck[uint64](vecs[0])[0])
				case types.T_int64:
					require.Equal(t, int64(tt.value), vector.MustFixedColWithTypeCheck[int64](vecs[0])[0])
				}
				return 0, nil
			}).AnyTimes()

			err := tc.updateDstAutoIncrColumns(proc.Ctx, proc)
			if tt.wantErr {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
				require.ErrorContains(t, err, "AUTO_INCREMENT value")
				require.Zero(t, insertCalls)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, insertCalls)
			}
		})
	}
}

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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

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

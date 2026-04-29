// Copyright 2024 Matrix Origin
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

package deletion

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPartitionDeleteCallDoesNotMutateSharedObjectRef(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc, eng := prepareDeletionTest(t, ctrl, false)

	raw := &Deletion{
		DeleteCtx: &DeleteCtx{
			Ref: &plan.ObjectRef{
				SchemaName: "testDb",
				ObjName:    "testTable",
			},
			Engine:        eng,
			PrimaryKeyIdx: 1,
		},
	}
	require.NoError(t, raw.Prepare(proc))

	op := &PartitionDelete{
		raw: raw,
		meta: partition.PartitionMetadata{
			Partitions: []partition.Partition{
				{
					PartitionID:        1,
					PartitionTableName: "partition_table",
					Expr: &pbplan.Expr{
						Typ: pbplan.Type{Id: int32(types.T_bool)},
						Expr: &pbplan.Expr_Lit{
							Lit: &pbplan.Literal{Value: &pbplan.Literal_Bval{Bval: true}},
						},
					},
				},
			},
		},
	}
	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{"row_id", "pk"})
	bat.Vecs[0] = testutil.MakeRowIdVector([]types.Rowid{types.BuildTestRowid(1, 1)}, nil)
	bat.Vecs[1] = testutil.MakeInt64Vector([]int64{1}, nil)
	bat.SetRowCount(1)
	op.AppendChild(
		colexec.NewMockOperator().WithBatchs([]*batch.Batch{
			bat,
		}),
	)

	_, err := op.Call(proc)
	require.NoError(t, err)
	require.Equal(t, "testTable", raw.DeleteCtx.Ref.ObjName)

	raw.Free(proc, false, nil)
	proc.Free()
}

// Copyright 2021-2024 Matrix Origin
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

package insert

import (
	"bytes"
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestPartitionInsertString(t *testing.T) {
	op := &PartitionInsert{}
	buf := new(bytes.Buffer)
	op.String(buf)
	require.Equal(t, "insert: partition_insert", buf.String())
}

func TestNewPartitionInsertFrom(t *testing.T) {
	ps := &PartitionInsert{
		raw:     &Insert{},
		tableID: 1,
	}
	op := NewPartitionInsertFrom(ps)
	require.Equal(t, ps.raw.InsertCtx, op.(*PartitionInsert).raw.InsertCtx)
	require.Equal(t, ps.raw.ToWriteS3, op.(*PartitionInsert).raw.ToWriteS3)
	require.Equal(t, ps.tableID, op.(*PartitionInsert).tableID)
}

func TestPartitionInsertCallDoesNotMutateSharedObjectRef(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	database := mock_frontend.NewMockDatabase(ctrl)
	relation := mock_frontend.NewMockRelation(ctrl)
	var relationNames []string

	eng.EXPECT().Hints().Return(engine.Hints{}).AnyTimes()
	eng.EXPECT().Database(gomock.Any(), "testDb", gomock.Any()).Return(database, nil).AnyTimes()
	database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, name string, _ any) (engine.Relation, error) {
			relationNames = append(relationNames, name)
			return relation, nil
		},
	).AnyTimes()
	relation.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	relation.EXPECT().Reset(gomock.Any()).Return(nil).AnyTimes()

	proc := testutil.NewProc(t)
	proc.Base.TxnClient = txnClient
	proc.Base.TxnOperator = txnOperator
	proc.Ctx = ctx

	raw := &Insert{
		InsertCtx: &InsertCtx{
			Ref: &plan.ObjectRef{
				SchemaName: "testDb",
				ObjName:    "testTable",
			},
			Engine: eng,
			Attrs:  []string{"a"},
		},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
		ctr: container{state: vm.Build},
	}
	require.NoError(t, raw.Prepare(proc))

	bat := batch.NewWithSize(1)
	bat.Attrs = []string{"a"}
	bat.Vecs[0] = testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp())
	bat.SetRowCount(1)

	op := &PartitionInsert{
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
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat}))

	_, err := op.Call(proc)
	require.NoError(t, err)
	require.Equal(t, "testTable", raw.InsertCtx.Ref.ObjName)
	require.Contains(t, relationNames, "testTable")
	require.Contains(t, relationNames, "partition_table")

	raw.Free(proc, false, nil)
	proc.Free()
}

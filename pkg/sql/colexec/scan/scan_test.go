// Copyright 2023 Matrix Origin
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

package scan

import (
	"context"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestSimpleScan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New().Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)

	proc := testutil.NewProc()
	proc.TxnClient = txnClient
	proc.Ctx = ctx
	/*
		assert.NoError(t, eng.Create(ctx, "db", txnOperator))
		db, err := eng.Database(ctx, "db", txnOperator)
		assert.NoError(t, err)
		err = db.Create(ctx, "t1", []engine.TableDef{
			&engine.AttributeDef{
				Attr: engine.Attribute{
					ID:   0,
					Name: "a",
					Type: types.T_varchar.ToType(),
				},
			},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					ID:   1,
					Name: "b",
					Type: types.T_varchar.ToType(),
				},
			},
		})
		assert.NoError(t, err)
		oriRel, err := db.Relation(ctx, "t1")
		assert.NoError(t, err)
		bat := &batch.Batch{
			Attrs: []string{"a", "b"},
			Vecs: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"a1", "a2", "a3"}, nil),
				testutil.MakeVarcharVector([]string{"b1", "b2", "b3"}, nil),
			},
			Zs: []int64{1, 1, 1},
		}
		assert.NoError(t, oriRel.Write(ctx, bat))
	*/
	_, _, rel, err := eng.GetRelationById(ctx, txnOperator, 0)
	assert.NoError(t, err)
	ranges, err := rel.Ranges(ctx, nil)
	assert.NoError(t, err)
	readers, err := rel.NewReader(ctx, 1, nil, ranges)
	assert.NoError(t, err)
	args := make([]Argument, len(readers))
	for i, r := range readers {
		args[i] = Argument{
			Reader:  r,
			ColName: []string{"int64_column"},
		}
	}
	_, err = Call(0, proc, args[0], false, false)
	assert.NoError(t, err)
}

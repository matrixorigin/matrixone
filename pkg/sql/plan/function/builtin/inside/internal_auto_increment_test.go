// Copyright 2021 - 2022 Matrix Origin
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

package inside

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestInternalAutoIncrement(t *testing.T) {
	columnNames := []string{"a", "b"}
	plan2Type1 := makePlan2Type(types.T_int32.ToType())
	plan2Type1.AutoIncr = true
	plan2Type2 := makePlan2Type(types.T_varchar.ToType())
	plan2Type2.Width = 15
	columnTypes := []plan.Type{plan2Type1, plan2Type2}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProc()

	var mu sync.Mutex
	proc.SessionInfo.AutoIncrCaches.AutoIncrCaches = make(map[string]defines.AutoIncrCache)
	proc.SessionInfo.AutoIncrCaches.Mu = &mu

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New().Return(txnOperator, nil).AnyTimes()

	db := mock_frontend.NewMockDatabase(ctrl)
	db.EXPECT().Relations(gomock.Any()).Return(nil, nil).AnyTimes()

	reader := mock_frontend.NewMockReader(ctrl)
	reader.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, attrs []string, b, c interface{}) (*batch.Batch, error) {
		bat := batch.NewWithSize(4)
		bat.Zs = []int64{1}
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		//err := bat.Vecs[0].Append(int64(1), false, testutil.TestUtilMp)
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		err := vector.AppendBytes(bat.Vecs[1], []byte("10_a"), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}
		bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
		err = vector.AppendFixed(bat.Vecs[2], uint64(28), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}
		bat.Vecs[3] = vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixed(bat.Vecs[3], int64(1), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}
		return bat, nil
	}).AnyTimes()

	table := mock_frontend.NewMockRelation(ctrl)
	table.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().TableDefs(gomock.Any()).Return(buildTableDefs(columnNames, columnTypes), nil).AnyTimes()
	table.EXPECT().GetPrimaryKeys(gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().GetHideKeys(gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().Stats(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().NewReader(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]engine.Reader{reader}, nil).AnyTimes()
	table.EXPECT().GetTableID(gomock.Any()).Return(uint64(10)).AnyTimes()
	table.EXPECT().Rows(gomock.Any()).Return(int64(10), nil).AnyTimes()
	table.EXPECT().Size(gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), gomock.Any()).Return(table, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(db, nil).AnyTimes()
	proc.Ctx = context.WithValue(proc.Ctx, defines.EngineKey{}, eng)
	proc.TxnClient = txnClient

	tests := []struct {
		name    string
		vectors []*vector.Vector
		want    string
	}{
		{
			name: "test01",
			vectors: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"db1"}, []uint64{}),
				testutil.MakeVarcharVector([]string{"t1"}, []uint64{}),
			},
			want: "0",
		},
		{
			name: "test01",
			vectors: []*vector.Vector{
				testutil.MakeScalarVarchar("db1", 1),
				testutil.MakeScalarVarchar("t2", 1),
			},
			want: "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := InternalAutoIncrement(tt.vectors, proc)
			if err != nil {
				t.Errorf("InternalAutoIncrement() error = %v", err)
				return
			}
			require.Nil(t, err)
			require.Equal(t, "28", r.String())
		})
	}
}

func Test_newTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProc()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	_, err := newTxn(eng, proc, proc.Ctx)
	require.NotNil(t, err)

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New().Return(txnOperator, nil).AnyTimes()
	proc.TxnClient = txnClient
	_, err = newTxn(eng, proc, proc.Ctx)
	require.Nil(t, err)

	_, err = newTxn(eng, proc, nil)
	require.NotNil(t, err)
}

func Test_rolllbackTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProc()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	err := rolllbackTxn(eng, txnOperator, proc.Ctx)
	require.Nil(t, err)

	err = rolllbackTxn(eng, nil, proc.Ctx)
	require.Nil(t, err)

	err = rolllbackTxn(eng, txnOperator, nil)
	require.NotNil(t, err)
}

func Test_commitTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProc()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	err := commitTxn(eng, txnOperator, proc.Ctx)
	require.Nil(t, err)

	err = commitTxn(eng, nil, proc.Ctx)
	require.Nil(t, err)

	err = commitTxn(eng, txnOperator, nil)
	require.NotNil(t, err)
}

func Test_getTableAutoIncrCol(t *testing.T) {
	type args struct {
		engineDefs []engine.TableDef
		tableName  string
	}
	tests := []struct {
		name  string
		args  args
		want  bool
		want1 *plan.ColDef
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getTableAutoIncrCol(tt.args.engineDefs, tt.args.tableName)
			if got != tt.want {
				t.Errorf("getTableAutoIncrCol() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("getTableAutoIncrCol() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func makePlan2Type(typ types.Type) plan.Type {
	return plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Size:  typ.Size,
		Scale: typ.Scale,
	}
}

// Define an anonymous function to construct the table structure
func buildTableDefs(columnNames []string, columnTypes []plan.Type) []engine.TableDef {
	exeCols := make([]engine.TableDef, len(columnTypes))
	for i, colTyp := range columnTypes {
		exeCols[i] = &engine.AttributeDef{
			Attr: engine.Attribute{
				Name: columnNames[i],
				Alg:  compress.None,
				Type: types.Type{
					Oid:   types.T(colTyp.GetId()),
					Width: colTyp.GetWidth(),
					Scale: colTyp.GetScale(),
					Size:  colTyp.GetSize(),
				},
				Default:       nil,
				OnUpdate:      nil,
				Primary:       i == 0,
				Comment:       "comment message",
				ClusterBy:     false,
				AutoIncrement: i == 0,
			},
		}
	}
	return exeCols
}

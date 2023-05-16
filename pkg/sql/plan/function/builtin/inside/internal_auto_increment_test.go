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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestInternalAutoIncrement(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, ts rpc.TxnSender) {
		getProc := func(tableID uint64) *process.Process {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			op, err := tc.New(ctx, timestamp.Timestamp{})
			require.NoError(t, err)

			columnNames := []string{"a", "b"}
			plan2Type1 := makePlan2Type(types.T_int32.ToType())
			plan2Type1.AutoIncr = true
			plan2Type2 := makePlan2Type(types.T_varchar.ToType())
			plan2Type2.Width = 15
			columnTypes := []plan.Type{plan2Type1, plan2Type2}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			proc := testutil.NewProc()

			db := mock_frontend.NewMockDatabase(ctrl)
			db.EXPECT().Relations(gomock.Any()).Return(nil, nil).AnyTimes()

			table := mock_frontend.NewMockRelation(ctrl)
			table.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			table.EXPECT().TableDefs(gomock.Any()).Return(buildTableDefs(columnNames, columnTypes), nil).AnyTimes()
			table.EXPECT().GetPrimaryKeys(gomock.Any()).Return(nil, nil).AnyTimes()
			table.EXPECT().GetHideKeys(gomock.Any()).Return(nil, nil).AnyTimes()
			table.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
			table.EXPECT().NewReader(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]engine.Reader{nil}, nil).AnyTimes()
			table.EXPECT().GetTableID(gomock.Any()).Return(tableID).AnyTimes()
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
			proc.TxnClient = tc
			proc.TxnOperator = op
			return proc
		}

		tests := []struct {
			name    string
			tableID uint64
			vectors []*vector.Vector
			want    string
		}{
			{
				name:    "test01",
				tableID: 10,
				vectors: []*vector.Vector{
					testutil.MakeVarcharVector([]string{"db1"}, []uint64{}),
					testutil.MakeVarcharVector([]string{"t1"}, []uint64{}),
				},
				want: "0",
			},
			{
				name:    "test01",
				tableID: 11,
				vectors: []*vector.Vector{
					testutil.MakeScalarVarchar("db1", 1),
					testutil.MakeScalarVarchar("t2", 1),
				},
				want: "0",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				proc := getProc(tt.tableID)
				require.NoError(t,
					incrservice.GetAutoIncrementService().Create(
						proc.Ctx,
						tt.tableID,
						[]incrservice.AutoColumn{
							{
								TableID:  tt.tableID,
								ColName:  "a",
								ColIndex: 0,
								Offset:   0,
								Step:     1,
							},
						},
						proc.TxnOperator))

				r, err := InternalAutoIncrement(tt.vectors, proc)
				if err != nil {
					t.Errorf("InternalAutoIncrement() error = %v", err)
					return
				}
				require.Nil(t, err)
				require.Equal(t, "1", r.String())
			})
		}
	})
}

func makePlan2Type(typ types.Type) plan.Type {
	return plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}

// Define an anonymous function to construct the table structure
func buildTableDefs(columnNames []string, columnTypes []plan.Type) []engine.TableDef {
	exeCols := make([]engine.TableDef, len(columnTypes))
	for i, colTyp := range columnTypes {
		exeCols[i] = &engine.AttributeDef{
			Attr: engine.Attribute{
				Name:          columnNames[i],
				Alg:           compress.None,
				Type:          types.New(types.T(colTyp.GetId()), colTyp.GetWidth(), colTyp.GetScale()),
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

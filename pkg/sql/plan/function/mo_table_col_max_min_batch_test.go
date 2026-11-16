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

package function

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type colExtremaRelData struct {
	engine.RelData
	count int
}

func (d colExtremaRelData) DataCnt() int { return d.count }

func TestMoTableColMaxMinReusesBatchArguments(t *testing.T) {
	for _, fn := range []struct {
		name       string
		call       func([]*vector.Vector, vector.FunctionResultWrapper, *process.Process, int, *FunctionSelectList) error
		valueIndex int
	}{
		{"max", MoTableColMax, 1},
		{"min", MoTableColMin, 0},
	} {
		for _, constant := range []bool{true, false} {
			name := fn.name + "/dynamic"
			if constant {
				name = fn.name + "/constant"
			}
			t.Run(name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				eng := mock_frontend.NewMockEngine(ctrl)
				db := mock_frontend.NewMockDatabase(ctrl)
				rel := mock_frontend.NewMockRelation(ctrl)
				txn := mock_frontend.NewMockTxnOperator(ctrl)
				proc := testutil.NewProcess(t)
				defer proc.Free()
				proc.Ctx = context.WithValue(defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
				proc.Base.TxnOperator = txn

				calls := 2
				if constant {
					calls = 1
				}
				eng.EXPECT().Database(gomock.Any(), "app", txn).Return(db, nil).Times(calls)
				db.EXPECT().IsSubscription(gomock.Any()).Return(false).Times(calls)
				db.EXPECT().Relation(gomock.Any(), "table", nil).Return(rel, nil).Times(calls)
				rel.EXPECT().GetTableName().Return("table").Times(calls)
				rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).Times(calls)
				rel.EXPECT().TableColumns(gomock.Any()).Return([]*engine.Attribute{{Name: "a"}, {Name: "b"}}, nil).Times(calls)
				rel.EXPECT().Ranges(gomock.Any(), engine.DefaultRangesParam).Return(colExtremaRelData{count: 2}, nil).Times(calls)
				rel.EXPECT().MaxAndMinValues(gomock.Any()).Return([][2]any{{int64(1), int64(9)}, {int64(2), int64(8)}}, nil, nil).Times(calls)

				length := 4
				var dbv, tablev, colv *vector.Vector
				if constant {
					var err error
					dbv, err = vector.NewConstBytes(types.T_varchar.ToType(), []byte("app"), length, proc.Mp())
					require.NoError(t, err)
					tablev, err = vector.NewConstBytes(types.T_varchar.ToType(), []byte("table"), length, proc.Mp())
					require.NoError(t, err)
					colv, err = vector.NewConstBytes(types.T_varchar.ToType(), []byte("a"), length, proc.Mp())
					require.NoError(t, err)
				} else {
					length = 6
					dbv = vector.NewVec(types.T_varchar.ToType())
					tablev = vector.NewVec(types.T_varchar.ToType())
					colv = vector.NewVec(types.T_varchar.ToType())
					require.NoError(t, vector.AppendBytesList(dbv, [][]byte{[]byte("app"), []byte("app"), []byte("app"), []byte("app"), []byte("app"), []byte("app")}, nil, proc.Mp()))
					require.NoError(t, vector.AppendBytesList(tablev, [][]byte{[]byte("table"), []byte("table"), []byte("table"), []byte("table"), []byte("table"), []byte("table")}, nil, proc.Mp()))
					require.NoError(t, vector.AppendBytesList(colv, [][]byte{[]byte("a"), []byte("a"), []byte("b"), []byte("a"), nil, []byte("b")}, []bool{false, false, false, false, true, false}, proc.Mp()))
				}
				defer dbv.Free(proc.Mp())
				defer tablev.Free(proc.Mp())
				defer colv.Free(proc.Mp())
				result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
				defer result.Free()
				require.NoError(t, result.PreExtendAndReset(length))
				require.NoError(t, fn.call([]*vector.Vector{dbv, tablev, colv}, result, proc, length, nil))
				out := result.GetResultVector()
				require.Equal(t, length, out.Length())
				wantA, wantB := "1", "2"
				if fn.valueIndex == 1 {
					wantA, wantB = "9", "8"
				}
				if constant {
					for i := 0; i < length; i++ {
						require.False(t, out.IsNull(uint64(i)))
						require.Equal(t, wantA, out.GetStringAt(i))
					}
				} else {
					for i, want := range []string{wantA, wantA, wantB, wantA, "", wantB} {
						if i == 4 {
							require.True(t, out.IsNull(uint64(i)))
						} else {
							require.False(t, out.IsNull(uint64(i)))
							require.Equal(t, want, out.GetStringAt(i))
						}
					}
				}
			})
		}
	}
}

func TestMoTableColMaxMinReusesEmptyBatchResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Ctx = context.WithValue(defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
	proc.Base.TxnOperator = txn

	eng.EXPECT().Database(gomock.Any(), "app", txn).Return(db, nil).Times(1)
	db.EXPECT().IsSubscription(gomock.Any()).Return(false).Times(1)
	db.EXPECT().Relation(gomock.Any(), "table", nil).Return(rel, nil).Times(1)
	rel.EXPECT().GetTableName().Return("table").Times(1)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).Times(1)
	rel.EXPECT().TableColumns(gomock.Any()).Return([]*engine.Attribute{{Name: "a"}}, nil).Times(1)
	rel.EXPECT().Ranges(gomock.Any(), engine.DefaultRangesParam).Return(colExtremaRelData{count: 0}, nil).Times(1)

	const length = 4
	inputs := make([]*vector.Vector, 0, 3)
	for _, value := range []string{"app", "table", "a"} {
		v, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(value), length, proc.Mp())
		require.NoError(t, err)
		defer v.Free(proc.Mp())
		inputs = append(inputs, v)
	}
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(length))
	require.NoError(t, MoTableColMax(inputs, result, proc, length, nil))
	out := result.GetResultVector()
	require.Equal(t, length, out.Length())
	for i := 0; i < length; i++ {
		require.True(t, out.IsNull(uint64(i)))
	}
}

func TestMoTableColMaxMinSubscriptionBatchResolution(t *testing.T) {
	for _, missing := range []bool{false, true} {
		name := "repeated-allowed"
		if missing {
			name = "missing-after-cache-hit"
		}
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			eng := mock_frontend.NewMockEngine(ctrl)
			subscriber := mock_frontend.NewMockDatabase(ctrl)
			publisher := mock_frontend.NewMockDatabase(ctrl)
			rel := mock_frontend.NewMockRelation(ctrl)
			txn := mock_frontend.NewMockTxnOperator(ctrl)
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.Ctx = context.WithValue(defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
			proc.Base.TxnOperator = txn
			helper := &fallbackSubscriptionSQL{}
			proc.Base.SessionInfo.SqlHelper = helper

			calls := 1
			if missing {
				calls = 2
			}
			eng.EXPECT().Database(gomock.Any(), "sub", txn).DoAndReturn(func(ctx context.Context, _ string, _ client.TxnOperator) (engine.Database, error) {
				account, err := defines.GetAccountId(ctx)
				require.NoError(t, err)
				require.Equal(t, uint32(1), account)
				return subscriber, nil
			}).Times(calls)
			subscriber.EXPECT().IsSubscription(gomock.Any()).Return(true).Times(calls)
			eng.EXPECT().Database(gomock.Any(), "origin", txn).DoAndReturn(func(ctx context.Context, _ string, _ client.TxnOperator) (engine.Database, error) {
				account, err := defines.GetAccountId(ctx)
				require.NoError(t, err)
				require.Equal(t, uint32(2), account)
				return publisher, nil
			}).Times(calls)
			publisher.EXPECT().Relation(gomock.Any(), "table", nil).Return(rel, nil).Times(1)
			rel.EXPECT().GetTableName().Return("table").Times(1)
			rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).Times(1)
			rel.EXPECT().TableColumns(gomock.Any()).Return([]*engine.Attribute{{Name: "a"}}, nil).Times(1)
			rel.EXPECT().Ranges(gomock.Any(), engine.DefaultRangesParam).Return(colExtremaRelData{count: 2}, nil).Times(1)
			rel.EXPECT().MaxAndMinValues(gomock.Any()).Return([][2]any{{int64(1), int64(9)}}, nil, nil).Times(1)
			sentinel := errors.New("publisher relation missing")
			if missing {
				publisher.EXPECT().Relation(gomock.Any(), "denied", nil).Return(nil, sentinel).Times(1)
			}

			const length = 3
			dbv, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("sub"), length, proc.Mp())
			require.NoError(t, err)
			defer dbv.Free(proc.Mp())
			tablev := vector.NewVec(types.T_varchar.ToType())
			defer tablev.Free(proc.Mp())
			names := [][]byte{[]byte("table"), []byte("table"), []byte("table")}
			if missing {
				names[2] = []byte("denied")
			}
			require.NoError(t, vector.AppendBytesList(tablev, names, nil, proc.Mp()))
			colv, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("a"), length, proc.Mp())
			require.NoError(t, err)
			defer colv.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(length))
			err = MoTableColMax([]*vector.Vector{dbv, tablev, colv}, result, proc, length, nil)
			if missing {
				require.ErrorIs(t, err, sentinel)
			} else {
				require.NoError(t, err)
				out := result.GetResultVector()
				require.Equal(t, length, out.Length())
				for i := 0; i < length; i++ {
					require.Equal(t, "9", out.GetStringAt(i))
				}
			}
			require.Equal(t, calls, helper.meta)
		})
	}
}

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestMoTableStatsSQLDispatch(t *testing.T) {
	for _, size := range []bool{false, true} {
		name := "rows"
		if size {
			name = "size"
		}
		for _, mode := range []string{"new", "fallback", "error", "cardinality", "session"} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				eng := mock_frontend.NewMockEngine(ctrl)
				db := mock_frontend.NewMockDatabase(ctrl)
				rel := mock_frontend.NewMockRelation(ctrl)
				txn := mock_frontend.NewMockTxnOperator(ctrl)
				proc := testutil.NewProcess(t)
				defer proc.Free()
				proc.Ctx = context.WithValue(defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
				proc.Base.TxnOperator = txn
				proc.SetResolveVariableFunc(func(variable string, _, _ bool) (any, error) {
					if mode == "session" && variable == MoTableRowSizeUseOldImplVarName {
						return "yes", nil
					}
					return "no", nil
				})
				metadataCalls := 2
				if mode == "error" {
					metadataCalls = 0
				}
				eng.EXPECT().Database(gomock.Any(), "app", txn).Return(db, nil).Times(metadataCalls)
				db.EXPECT().IsSubscription(gomock.Any()).Return(false).Times(metadataCalls)
				db.EXPECT().Relation(gomock.Any(), "table", nil).Return(rel, nil).Times(metadataCalls)
				rel.EXPECT().GetDBID(gomock.Any()).Return(uint64(10)).AnyTimes()
				rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(20)).AnyTimes()
				rel.EXPECT().GetTableName().Return("table").Times(metadataCalls)
				defCalls := metadataCalls
				if size && (mode == "fallback" || mode == "session") {
					defCalls += 2 // old size path also reads the base's index list
				}
				rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).Times(defCalls)
				if mode == "fallback" || mode == "session" {
					if size {
						rel.EXPECT().Size(gomock.Any(), AllColumns).Return(uint64(37), nil).Times(2)
					} else {
						rel.EXPECT().Rows(gomock.Any()).Return(uint64(37), nil).Times(2)
					}
				}
				sentinel := errors.New("statistics unavailable")
				count := 0
				callback := GetMoTableSizeRowsFuncType(func(_ context.Context, _ engine.Engine, resolve MoTableStatsResolver, force, reset bool) ([]uint64, error, bool) {
					count++
					require.False(t, force)
					require.False(t, reset)
					if mode == "new" || mode == "cardinality" {
						accs, dbs, tbls, err := resolve()
						require.NoError(t, err)
						require.Equal(t, []uint64{1, 1}, accs)
						require.Equal(t, []uint64{10, 10}, dbs)
						require.Equal(t, []uint64{20, 20}, tbls)
					}
					switch mode {
					case "fallback":
						return nil, nil, false
					case "error":
						return nil, sentinel, false
					case "cardinality":
						return []uint64{73}, nil, true
					case "session":
						t.Fatal("session old mode must bypass statistics")
						return nil, nil, false
					default:
						return []uint64{73, 74}, nil, true
					}
				})
				pointer := &GetMoTableRowsFunc
				call := MoTableRows
				if size {
					pointer = &GetMoTableSizeFunc
					call = MoTableSize
				}
				previous := pointer.Swap(&callback)
				defer pointer.Store(previous)
				// Table, NULL, tenant-filtered system table, table. Both implementations
				// must retain this order and append each input exactly once.
				dbv := vector.NewVec(types.T_varchar.ToType())
				defer dbv.Free(proc.Mp())
				tblv := vector.NewVec(types.T_varchar.ToType())
				defer tblv.Free(proc.Mp())
				require.NoError(t, vector.AppendBytesList(dbv, [][]byte{[]byte("app"), nil, []byte(catalog.MO_CATALOG), []byte("app")}, []bool{false, true, false, false}, proc.Mp()))
				require.NoError(t, vector.AppendBytesList(tblv, [][]byte{[]byte("table"), nil, []byte(catalog.MO_TABLES), []byte("table")}, []bool{false, true, false, false}, proc.Mp()))
				result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
				defer result.Free()
				require.NoError(t, result.PreExtendAndReset(4))
				copy(vector.MustFixedColWithTypeCheck[int64](result.GetResultVector()), []int64{111, 222, 333, 444})
				err := call([]*vector.Vector{dbv, tblv}, result, proc, 4, nil)
				if mode == "error" {
					require.ErrorIs(t, err, sentinel)
					return
				}
				if mode == "cardinality" {
					require.ErrorContains(t, err, "cardinality")
					return
				}
				require.NoError(t, err)
				expected := []int64{37, 0, 0, 37}
				if mode == "new" {
					expected = []int64{73, 0, 0, 74}
				}
				require.Equal(t, 4, result.GetResultVector().Length())
				values := vector.MustFixedColWithTypeCheck[int64](result.GetResultVector())
				require.Equal(t, []int64{expected[0], expected[2], expected[3]}, []int64{values[0], values[2], values[3]})
				require.True(t, result.GetResultVector().IsNull(1))
				require.False(t, result.GetResultVector().IsNull(2))
				if mode == "session" {
					require.Zero(t, count)
				} else {
					require.Equal(t, 1, count)
				}
			})
		}
	}
}

// A published relation can carry index kind even after a legacy direct rename
// removed its reserved prefix. Check its publisher-side identity before the
// statistics executor consumes the catalog IDs.
func TestMoTableStatsSubscriptionIndexKindAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, kind, account, user, role string
		flags                           uint64
		allow                           bool
	}{
		{"renamed index denied", catalog.SystemIndexRel, "tenant", "alice", "accountadmin", 0, false},
		{"renamed flagged child", catalog.SystemOrdinaryRel, "tenant", "alice", "accountadmin", features.IndexTable, false},
		{"legacy plugin child", "unknown_plugin_index", "tenant", "alice", "accountadmin", 0, false},
		{"ordinary indexed base", catalog.SystemOrdinaryRel, "tenant", "alice", "accountadmin", 0, true},
		{"system diagnostic", catalog.SystemIndexRel, "sys", "root", "moadmin", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			proc.Base.SessionInfo.Account = tc.account
			proc.Base.SessionInfo.User = tc.user
			proc.Base.SessionInfo.Role = tc.role
			proc.Base.SessionInfo.SqlHelper = &fallbackSubscriptionSQL{}
			eng.EXPECT().Database(gomock.Any(), "app", txn).Return(subscriber, nil).Times(1)
			subscriber.EXPECT().IsSubscription(gomock.Any()).Return(true).Times(1)
			eng.EXPECT().Database(gomock.Any(), "origin", txn).DoAndReturn(func(ctx context.Context, _ string, _ client.TxnOperator) (engine.Database, error) {
				account, err := defines.GetAccountId(ctx)
				require.NoError(t, err)
				require.Equal(t, uint32(2), account)
				return publisher, nil
			}).Times(1)
			publisher.EXPECT().Relation(gomock.Any(), "renamed", nil).Return(rel, nil).Times(1)
			rel.EXPECT().GetDBID(gomock.Any()).Return(uint64(10)).Times(1)
			rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(20)).Times(1)
			rel.EXPECT().GetTableName().Return("renamed").Times(1)
			rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
				TableType: tc.kind, FeatureFlag: tc.flags,
				Indexes: []*plan.IndexDef{{IndexTableName: catalog.SecondaryIndexTableNamePrefix + "generated"}},
			}).Times(1)
			callback := GetMoTableSizeRowsFuncType(func(_ context.Context, _ engine.Engine, resolve MoTableStatsResolver, _, _ bool) ([]uint64, error, bool) {
				_, _, _, err := resolve()
				if err != nil {
					return nil, err, true
				}
				return []uint64{37}, nil, true
			})
			previous := GetMoTableRowsFunc.Swap(&callback)
			defer GetMoTableRowsFunc.Store(previous)
			dbv, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("app"), 1, proc.Mp())
			require.NoError(t, err)
			defer dbv.Free(proc.Mp())
			tblv, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("renamed"), 1, proc.Mp())
			require.NoError(t, err)
			defer tblv.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(1))
			err = MoTableRows([]*vector.Vector{dbv, tblv}, result, proc, 1, nil)
			if !tc.allow {
				require.ErrorContains(t, err, "privilege")
				return
			}
			require.NoError(t, err)
			require.Equal(t, int64(37), vector.MustFixedColWithTypeCheck[int64](result.GetResultVector())[0])
		})
	}
}

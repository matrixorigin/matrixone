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
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"testing"
)

type fallbackSubscriptionSQL struct{ meta, queries int }

func (s *fallbackSubscriptionSQL) GetCompilerContext() any { return nil }
func (s *fallbackSubscriptionSQL) ExecSql(string) ([][]interface{}, error) {
	panic("unexpected contextless SQL")
}
func (s *fallbackSubscriptionSQL) GetSubscriptionMeta(string) (*plan.SubscriptionMeta, error) {
	s.meta++
	return &plan.SubscriptionMeta{AccountId: 2, AccountName: "publisher", DbName: "origin", Tables: "*"}, nil
}
func (s *fallbackSubscriptionSQL) ExecSqlWithCtx(context.Context, string) ([][]interface{}, error) {
	s.queries++
	return [][]interface{}{{uint64(10), uint64(20)}}, nil
}

// Invoke real public scalar entries and real old-function fallback. Only
// Engine metadata and catalog SQL are dependency counters, not replacements
// for the function under review. Unhandled is the owner's old-mode contract.
func TestMoTableStatsFallbackResolvesMetadataOnce(t *testing.T) {
	for _, size := range []bool{false, true} {
		for _, sub := range []bool{false, true} {
			for _, session := range []bool{false, true} {
				name := "rows/ordinary/owner"
				if size {
					name = "size/ordinary/owner"
				}
				if sub {
					name += "/subscription"
				}
				if session {
					name += "/session-control"
				}
				t.Run(name, func(t *testing.T) {
					ctrl := gomock.NewController(t)
					eng := mock_frontend.NewMockEngine(ctrl)
					db := mock_frontend.NewMockDatabase(ctrl)
					origin := mock_frontend.NewMockDatabase(ctrl)
					rel := mock_frontend.NewMockRelation(ctrl)
					txn := mock_frontend.NewMockTxnOperator(ctrl)
					proc := testutil.NewProcess(t)
					defer proc.Free()
					proc.Ctx = context.WithValue(defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
					proc.Base.TxnOperator = txn
					proc.SetResolveVariableFunc(func(variable string, _, _ bool) (any, error) {
						if session && variable == MoTableRowSizeUseOldImplVarName {
							return "yes", nil
						}
						return "no", nil
					})
					counter := &fallbackSubscriptionSQL{}
					proc.Base.SessionInfo.SqlHelper = counter
					databases, relations := 0, 0
					eng.EXPECT().Database(gomock.Any(), "app", txn).DoAndReturn(func(context.Context, string, any) (engine.Database, error) { databases++; return db, nil }).AnyTimes()
					db.EXPECT().IsSubscription(gomock.Any()).Return(sub).AnyTimes()
					if sub {
						eng.EXPECT().Database(gomock.Any(), "origin", txn).Return(origin, nil).AnyTimes()
						origin.EXPECT().Relation(gomock.Any(), "table", nil).DoAndReturn(func(context.Context, string, any) (engine.Relation, error) { relations++; return rel, nil }).AnyTimes()
					} else {
						db.EXPECT().Relation(gomock.Any(), "table", nil).DoAndReturn(func(context.Context, string, any) (engine.Relation, error) { relations++; return rel, nil }).AnyTimes()
						rel.EXPECT().GetDBID(gomock.Any()).Return(uint64(10)).AnyTimes()
						rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(20)).AnyTimes()
					}
					if size {
						rel.EXPECT().Size(gomock.Any(), AllColumns).Return(uint64(37), nil).Times(1)
						rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).Times(1)
					} else {
						rel.EXPECT().Rows(gomock.Any()).Return(uint64(37), nil).Times(1)
					}
					callback := GetMoTableSizeRowsFuncType(func(context.Context, engine.Engine, MoTableStatsResolver, bool, bool) ([]uint64, error, bool) {
						require.False(t, session)
						return nil, nil, false
					})
					pointer, call := &GetMoTableRowsFunc, MoTableRows
					if size {
						pointer, call = &GetMoTableSizeFunc, MoTableSize
					}
					previous := pointer.Swap(&callback)
					defer pointer.Store(previous)
					dbv, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("app"), 1, proc.Mp())
					require.NoError(t, err)
					defer dbv.Free(proc.Mp())
					tv, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("table"), 1, proc.Mp())
					require.NoError(t, err)
					defer tv.Free(proc.Mp())
					result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
					defer result.Free()
					require.NoError(t, result.PreExtendAndReset(1))
					require.NoError(t, call([]*vector.Vector{dbv, tv}, result, proc, 1, nil))
					require.Equal(t, int64(37), vector.MustFixedColWithTypeCheck[int64](result.GetResultVector())[0])
					t.Logf("database=%d relation=%d subscription-meta=%d catalog-SQL=%d", databases, relations, counter.meta, counter.queries)
					require.Equal(t, 1, databases, "owner old mode must resolve the original database only once")
					require.Equal(t, 1, relations)
					if sub {
						require.Equal(t, 1, counter.meta)
						require.Equal(t, 1, counter.queries)
					}
				})
			}
		}
	}
}

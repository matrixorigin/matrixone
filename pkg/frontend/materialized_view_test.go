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

package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/mv"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewReadPrivileges(t *testing.T) {
	def := catalog.MVDefinition{FormatVersion: 1, Query: "select * from src", Sources: []catalog.MVSource{{Database: "db", Table: "src"}, {Database: "db", Table: "dimension"}}}
	data, err := json.Marshal(def)
	require.NoError(t, err)
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT, Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "stored"}, TableDef: &plan.TableDef{TableType: catalog.SystemMaterializedViewRel, Props: []*plan.PropertyDef{{Key: catalog.MaterializedViewProperty, Value: string(data)}}}}}}}}
	tips, err := extractPrivilegeTipsFromPlan(p)
	require.NoError(t, err)
	require.Len(t, tips, 3)
	for i, name := range []string{"src", "dimension", "stored"} {
		require.Equal(t, name, tips[i].tableName)
		require.Equal(t, PrivilegeTypeSelect, tips[i].typ)
		require.Equal(t, "db", tips[i].databaseName)
	}
	p.GetQuery().Nodes[0].TableDef.Props[0].Value = "invalid"
	_, err = extractPrivilegeTipsFromPlan(p)
	require.Error(t, err)
}

func TestMaterializedViewGlobalCatalogOwnership(t *testing.T) {
	for _, ddl := range catalog.MaterializedViewCatalogDDLs {
		require.True(t, isMaterializedViewCatalogDDL(ddl), "tenant initialization must not recreate a global MV catalog")
	}
}

func TestMaterializedViewTransactionClassification(t *testing.T) {
	for _, statement := range []tree.Statement{&tree.CreateMaterializedView{}, &tree.RefreshMaterializedView{}, &tree.DropMaterializedView{}, &tree.AlterMaterializedView{}} {
		allowed, err := statementCanBeExecutedInUncommittedTransaction(context.Background(), nil, statement)
		require.NoError(t, err)
		require.False(t, allowed)
	}
	allowed, err := statementCanBeExecutedInUncommittedTransaction(context.Background(), nil, &tree.ShowMaterializedViews{})
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestMaterializedViewDefinerAccountStatus(t *testing.T) {
	for _, status := range []string{"open", "suspend", "restricted", "unknown", "missing"} {
		t.Run(status, func(t *testing.T) {
			ses := newTestSession(t, gomock.NewController(t))
			t.Cleanup(ses.Close)
			ses.SetAccountId(7)
			ses.SetTenantInfo(getDefaultAccount())
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			userReads := 0
			sql := executor.NewMemExecutor(func(q string) (executor.Result, error) {
				if strings.HasPrefix(q, "select status from mo_catalog.mo_account") {
					require.Contains(t, q, "account_id=7")
					if status == "missing" {
						return executor.Result{}, nil
					}
					r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
					r.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendBytesRows(r, 0, [][]byte{[]byte(status)}))
					return r.GetResult(), nil
				}
				userReads++
				r := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()}, mp)
				r.NewBatchWithRowCount(1)
				for i, v := range []string{"owner", userStatusUnlock, "accountadmin"} {
					require.NoError(t, executor.AppendBytesRows(r, i, [][]byte{[]byte(v)}))
				}
				return r.GetResult(), nil
			})
			err := validateMaterializedViewDefiner(t.Context(), ses, mv.NewService(sql), &catalog.MVDefinition{OwnerUser: 2, OwnerRole: 3, OwnerName: "owner"})
			if status == "open" {
				require.NoError(t, err)
				require.Equal(t, 1, userReads)
			} else {
				require.ErrorContains(t, err, "account is unavailable")
				require.Zero(t, userReads)
			}
		})
	}
}

func TestMaterializedViewAccountRetirement(t *testing.T) {
	for _, count := range []int64{0, 3, 4} {
		for _, fail := range []int{-1, 0, 2, 5} {
			t.Run(fmt.Sprintf("catalog=%d/failure=%d", count, fail), func(t *testing.T) {
				bh := mock_frontend.NewMockBackgroundExec(gomock.NewController(t))
				ctx := t.Context()
				bh.EXPECT().ClearExecResultSet()
				bh.EXPECT().Exec(ctx, mv.CatalogReadinessSQL).Return(nil)
				bh.EXPECT().GetExecResultSet().Return([]interface{}{newMrsForCheckTenant([][]interface{}{{count}})})
				statements := accountMaterializedViewRetirementSQL(7)
				injected := errors.New("retirement failed")
				if count == 4 && (fail < 0 || fail > 2) {
					bh.EXPECT().ClearExecResultSet()
					bh.EXPECT().Exec(ctx, "select mv_id,database_name,name from mo_catalog.mo_materialized_views where account_id=7 order by mv_id limit 129").Return(nil)
					bh.EXPECT().GetExecResultSet().Return([]interface{}{newMrsForCheckTenant(nil)})
				}
				if count == 4 {
					for i, q := range statements {
						require.Contains(t, q, "7")
						if i == fail {
							bh.EXPECT().Exec(ctx, q).Return(injected)
							break
						}
						bh.EXPECT().Exec(ctx, q).Return(nil)
					}
				}
				err := retireAccountMaterializedViews(ctx, bh, 7)
				if count == 3 {
					require.ErrorContains(t, err, "incomplete")
				} else if count == 4 && fail >= 0 {
					require.ErrorIs(t, err, injected)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

type materializedViewRetirementBackground struct{ BackgroundExec }

func (materializedViewRetirementBackground) materializedViewRetirementTxnID() []byte {
	return []byte("account-drop")
}

func TestMaterializedViewAccountRetirementCapability(t *testing.T) {
	ctx := t.Context()
	mock := mock_frontend.NewMockBackgroundExec(gomock.NewController(t))
	bh := materializedViewRetirementBackground{mock}
	count := newMrsForCheckTenant([][]interface{}{{int64(4)}})
	objects := newMrsForCheckTenant([][]interface{}{{uint64(99), "db`name", "mv`name"}})
	mock.EXPECT().ClearExecResultSet().Times(2)
	calls := []*gomock.Call{mock.EXPECT().Exec(ctx, mv.CatalogReadinessSQL).Return(nil), mock.EXPECT().GetExecResultSet().Return([]interface{}{count})}
	statements := accountMaterializedViewRetirementSQL(7)
	for _, q := range statements[:3] {
		calls = append(calls, mock.EXPECT().Exec(ctx, q).Return(nil))
	}
	calls = append(calls, mock.EXPECT().Exec(ctx, "select mv_id,database_name,name from mo_catalog.mo_materialized_views where account_id=7 order by mv_id limit 129").Return(nil), mock.EXPECT().GetExecResultSet().Return([]interface{}{objects}))
	calls = append(calls, mock.EXPECT().Exec(gomock.Any(), "drop table `db``name`.`mv``name`").DoAndReturn(func(c context.Context, _ string) error {
		require.True(t, defines.CanWriteMaterializedView(c, 7, 99, []byte("account-drop")))
		require.False(t, defines.CanWriteMaterializedView(c, 8, 99, []byte("account-drop")))
		require.False(t, defines.CanWriteMaterializedView(c, 7, 100, []byte("account-drop")))
		require.False(t, defines.CanWriteMaterializedView(c, 7, 99, []byte("another-txn")))
		return nil
	}))
	for _, q := range statements[3:] {
		calls = append(calls, mock.EXPECT().Exec(ctx, q).Return(nil))
	}
	gomock.InOrder(calls...)
	require.NoError(t, retireAccountMaterializedViews(ctx, bh, 7))
}

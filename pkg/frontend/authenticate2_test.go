// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	plan3 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func Test_verifyAccountCanOperateClusterTable(t *testing.T) {
	type arg struct {
		acc  *TenantInfo
		db   string
		op   clusterTableOperationType
		want bool
	}

	sys := &TenantInfo{
		Tenant: sysAccountName,
	}

	nonSys := &TenantInfo{
		Tenant: "abc",
	}

	var args []arg

	for db := range bannedCatalogDatabases {
		for i := clusterTableNone; i <= clusterTableDrop; i++ {
			args = append(args, arg{
				acc:  sys,
				db:   db,
				op:   i,
				want: db == moCatalog,
			})
			args = append(args, arg{
				acc:  sys,
				db:   "abc",
				op:   i,
				want: false,
			})
			args = append(args, arg{
				acc:  nonSys,
				db:   db,
				op:   i,
				want: db == moCatalog && (i == clusterTableNone || i == clusterTableSelect),
			})
			args = append(args, arg{
				acc:  nonSys,
				db:   "abc",
				op:   i,
				want: false,
			})
		}
	}

	for _, a := range args {
		ret := verifyAccountCanOperateClusterTable(a.acc, a.db, a.op)
		assert.True(t, ret == a.want)
	}
}

func Test_verifyLightPrivilege(t *testing.T) {
	catalog.SetupDefines("")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	sys := &TenantInfo{
		Tenant: sysAccountName,
	}

	nonSys := &TenantInfo{
		Tenant: "abc",
	}

	ses.SetFromRealUser(true)
	ses.SetTenantInfo(sys)

	var ret bool

	ret = verifyLightPrivilege(ses, moCatalog, true,
		false, clusterTableNone)
	assert.False(t, ret)

	ret = verifyLightPrivilege(ses, moCatalog, true,
		true, clusterTableCreate)
	assert.True(t, ret)

	ret = verifyLightPrivilege(ses, "abc", true,
		true, clusterTableCreate)
	assert.False(t, ret)

	ret = verifyLightPrivilege(ses, "abc", true,
		false, clusterTableCreate)
	assert.True(t, ret)

	ret = verifyLightPrivilege(ses, "abc", false,
		false, clusterTableCreate)
	assert.True(t, ret)

	ses.SetTenantInfo(nonSys)

	ret = verifyLightPrivilege(ses, moCatalog, true,
		false, clusterTableNone)
	assert.False(t, ret)

	ret = verifyLightPrivilege(ses, moCatalog, true,
		true, clusterTableCreate)
	assert.False(t, ret)

	ret = verifyLightPrivilege(ses, moCatalog, true,
		true, clusterTableSelect)
	assert.True(t, ret)

	ret = verifyLightPrivilege(ses, moCatalog, true,
		true, clusterTableNone)
	assert.True(t, ret)

	ret = verifyLightPrivilege(ses, "abc", true,
		true, clusterTableCreate)
	assert.False(t, ret)

	ret = verifyLightPrivilege(ses, "abc", true,
		false, clusterTableCreate)
	assert.True(t, ret)

	ret = verifyLightPrivilege(ses, "abc", false,
		false, clusterTableCreate)
	assert.True(t, ret)
}

func Test_moctrl(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	sys := &TenantInfo{
		Tenant: sysAccountName,
	}

	sys2 := &TenantInfo{
		Tenant:      sysAccountName,
		DefaultRole: moAdminRoleName,
	}

	nonSys := &TenantInfo{
		Tenant: "abc",
	}

	nonSys2 := &TenantInfo{
		Tenant:      "abc",
		DefaultRole: moAdminRoleName,
	}
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(sys)

	var ret bool

	ret = verifyAccountCanExecMoCtrl(sys)
	assert.False(t, ret)

	ret = verifyAccountCanExecMoCtrl(sys2)
	assert.True(t, ret)

	ret = verifyAccountCanExecMoCtrl(nonSys)
	assert.False(t, ret)

	ret = verifyAccountCanExecMoCtrl(nonSys2)
	assert.False(t, ret)
}

func Test_hasMoCtrl(t *testing.T) {
	var ret bool
	ret = hasMoCtrl(nil)
	assert.False(t, ret)

	ret = hasMoCtrl(&plan2.Plan{})
	assert.False(t, ret)

	ret = hasMoCtrl(&plan2.Plan{
		Plan: &plan2.Plan_Query{
			Query: &plan2.Query{
				StmtType: plan3.Query_SELECT,
				Nodes: []*plan3.Node{
					{
						NodeType: plan3.Node_PROJECT,
						ProjectList: []*plan3.Expr{
							{
								Expr: &plan3.Expr_F{
									F: &plan3.Function{
										Func: &plan3.ObjectRef{
											ObjName: "mo_ctl",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	})
	assert.True(t, ret)

	// mo_ctl outside the SELECT projection must also be detected, otherwise a WHERE/JOIN placement
	// bypasses the sys-admin gate.
	moCtl := func() *plan3.Expr {
		return &plan3.Expr{Expr: &plan3.Expr_F{F: &plan3.Function{Func: &plan3.ObjectRef{ObjName: "mo_ctl"}}}}
	}
	planWith := func(node *plan3.Node) *plan2.Plan {
		return &plan2.Plan{Plan: &plan2.Plan_Query{Query: &plan2.Query{
			StmtType: plan3.Query_SELECT, Nodes: []*plan3.Node{node},
		}}}
	}
	// WHERE filter (the reported bypass)
	assert.True(t, hasMoCtrl(planWith(&plan3.Node{NodeType: plan3.Node_FILTER, FilterList: []*plan3.Expr{moCtl()}})))
	// JOIN condition
	assert.True(t, hasMoCtrl(planWith(&plan3.Node{NodeType: plan3.Node_JOIN, OnList: []*plan3.Expr{moCtl()}})))
	// a DELETE (or any query stmt type) carrying it in a filter
	assert.True(t, hasMoCtrl(&plan2.Plan{Plan: &plan2.Plan_Query{Query: &plan2.Query{
		StmtType: plan3.Query_DELETE,
		Nodes:    []*plan3.Node{{NodeType: plan3.Node_TABLE_SCAN, FilterList: []*plan3.Expr{moCtl()}}},
	}}}))
	// a plan with no mo_ctl anywhere is not flagged
	assert.False(t, hasMoCtrl(planWith(&plan3.Node{NodeType: plan3.Node_FILTER, FilterList: []*plan3.Expr{
		{Expr: &plan3.Expr_F{F: &plan3.Function{Func: &plan3.ObjectRef{ObjName: "="}}}},
	}})))

	// A prepared SET stores its bound value in the DCL plan (GetQuery() is nil) and EXECUTE evaluates
	// it directly, so mo_ctl in a SET-variable Value/Reserved must be gated here too -- otherwise
	// `PREPARE s FROM 'set @v = mo_ctl(...)'; EXECUTE s` bypasses the sys-admin check.
	dclSet := func(item *plan3.SetVariablesItem) *plan2.Plan {
		return &plan2.Plan{Plan: &plan3.Plan_Dcl{Dcl: &plan3.DataControl{
			Control: &plan3.DataControl_SetVariables{SetVariables: &plan3.SetVariables{
				Items: []*plan3.SetVariablesItem{item},
			}},
		}}}
	}
	assert.True(t, hasMoCtrl(dclSet(&plan3.SetVariablesItem{Name: "v", Value: moCtl()})))
	assert.True(t, hasMoCtrl(dclSet(&plan3.SetVariablesItem{Name: "v", Reserved: moCtl()})))
	// mo_ctl nested inside a larger SET value expression is still caught
	assert.True(t, hasMoCtrl(dclSet(&plan3.SetVariablesItem{Name: "v", Value: &plan3.Expr{
		Expr: &plan3.Expr_F{F: &plan3.Function{Func: &plan3.ObjectRef{ObjName: "+"}, Args: []*plan3.Expr{moCtl()}}},
	}})))
	// an ordinary prepared SET is not flagged
	assert.False(t, hasMoCtrl(dclSet(&plan3.SetVariablesItem{Name: "v", Value: &plan3.Expr{
		Expr: &plan3.Expr_F{F: &plan3.Function{Func: &plan3.ObjectRef{ObjName: "+"}}},
	}})))
	// a DCL plan carrying no set variables is safe
	assert.False(t, hasMoCtrl(&plan2.Plan{Plan: &plan3.Plan_Dcl{Dcl: &plan3.DataControl{}}}))
}

func newTestExecCtx(ctx context.Context, ctrl *gomock.Controller) *ExecCtx {
	ret := &ExecCtx{
		reqCtx: ctx,
	}
	return ret
}

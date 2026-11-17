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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type blockFilterInsertContext struct {
	plan2.CompilerContext
	proc *process.Process
}

func (ctx *blockFilterInsertContext) GetProcess() *process.Process { return ctx.proc }

func (ctx *blockFilterInsertContext) Stats(*plan.ObjectRef, *plan.Snapshot) (*statsinfo.StatsInfo, error) {
	return nil, nil
}

func (ctx *blockFilterInsertContext) Resolve(db, table string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	obj, def, err := ctx.CompilerContext.Resolve(db, table, snapshot)
	if err != nil {
		return obj, def, err
	}
	// 通用 mock 的 map 遍历 ID 可能命中系统表保留值，使用确定的普通表 ID。
	copyObj := *obj
	def = plan2.DeepCopyTableDef(def, true)
	def.TblId = 42001
	if table == "nation2" {
		def.TblId = 42002
	}
	copyObj.Obj = int64(def.TblId)
	return &copyObj, def, nil
}

func TestInternalInsertFinalBlockFilters(t *testing.T) {
	c := NewMockCompile(t)
	ctx := &blockFilterInsertContext{CompilerContext: plan2.NewMockCompilerContext(true), proc: c.proc}
	stmt, err := mysql.ParseOne(c.proc.Ctx, "insert into nation2 select * from nation where n_nationkey in (1,2,3,4) and n_nationkey not in (2,3)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	// 内部 INSERT 与 CTAS populateCreatedTable 不经过 BaseOptimizer。
	pn, err := plan2.BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	require.Equal(t, plan.Query_INSERT, pn.GetQuery().StmtType)
	t.Cleanup(func() {
		for _, exe := range c.filterExprExes {
			exe.Free()
		}
	})
	scans := 0
	for _, node := range pn.GetQuery().Nodes {
		if node.NodeType != plan.Node_TABLE_SCAN || node.TableDef.Name != "nation" {
			continue
		}
		scans++
		filters := plan2.DeepCopyExprList(node.BlockFilterList)
		require.Len(t, filters, 3, "反例必须实际绕过 optimizer 的最终去重")
		for _, filter := range filters {
			_, err = plan2.ReplaceFoldExpr(c.proc, filter, &c.filterExprExes)
			require.NoError(t, err)
		}
		s := &Scope{Proc: c.proc, DataSource: &Source{BlockFilterList: filters}}
		for range 2 {
			block, err := s.handleRuntimeFilters(c, nil)
			require.NoError(t, err)
			require.Len(t, block, 2)
			domains := make([][]int32, 0, len(block))
			for _, filter := range block {
				vec := decodeBlockFilterVector(t, c.proc, filter)
				domains = append(domains, append([]int32(nil), vector.MustFixedColWithTypeCheck[int32](vec)...))
			}
			require.Equal(t, [][]int32{{1, 2, 3, 4}, {1, 4}}, domains)
			require.Len(t, s.DataSource.BlockFilterList, 3, "不能改动长期缓存的过滤列表及 Fold 生命周期")
		}
	}
	require.Equal(t, 1, scans)
}

func decodeBlockFilterVector(t *testing.T, proc *process.Process, filter *plan.Expr) *vector.Vector {
	rhs := filter.GetF().Args[1]
	var data []byte
	if vec := rhs.GetVec(); vec != nil {
		data = vec.Data
	} else {
		require.NotNil(t, rhs.GetFold())
		require.False(t, rhs.GetFold().IsConst)
		data = rhs.GetFold().Data
	}
	vec := vector.NewVec(types.T_any.ToType())
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	require.NoError(t, vec.UnmarshalBinary(data))
	return vec
}

func TestReaderBlockFilterFoldReuse(t *testing.T) {
	c := NewMockCompile(t)
	t.Cleanup(func() {
		for _, exe := range c.filterExprExes {
			exe.Free()
		}
	})
	col := plan2.GetColExpr(plan.Type{Id: int32(types.T_int32)}, 1, 0)
	filters := make([]*plan.Expr, 0, 2)
	for pos := range int32(2) {
		param := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: pos}}}
		cast, err := plan2.ForceCastExpr(c.proc.Ctx, param, col.Typ)
		require.NoError(t, err)
		filter, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "in", []*plan.Expr{col, {
			Typ: col.Typ, Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{plan2.MakePlan2Int32ConstExprWithType(1), plan2.MakePlan2Int32ConstExprWithType(2)}}},
		}})
		require.NoError(t, err)
		require.Equal(t, "in", filter.GetF().Func.ObjName)
		// 直接构造 Fold 的列表消费契约，避免 binder 将带函数的列表改写成 OR。
		filter.GetF().Args[1] = &plan.Expr{Typ: col.Typ, Expr: &plan.Expr_List{List: &plan.ExprList{
			List: []*plan.Expr{plan2.MakePlan2Int32ConstExprWithType(1), cast},
		}}}
		_, err = plan2.ReplaceFoldExpr(c.proc, filter, &c.filterExprExes)
		require.NoError(t, err)
		filters = append(filters, filter)
	}
	require.Len(t, c.filterExprExes, 2)
	s := &Scope{Proc: c.proc, DataSource: &Source{BlockFilterList: filters}}
	empty := vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { empty.Free(c.proc.Mp()) })
	c.proc.SetPrepareParams(empty)
	_, err := s.handleRuntimeFilters(c, nil)
	require.Error(t, err, "Fold 求值失败必须向上传递，而不是被去重掩盖")
	for _, tc := range []struct {
		second string
		null   bool
	}{
		{"4", false},
		{"9", false},
		{"", true},
		{"4", false},
	} {
		params := vector.NewVec(types.T_varchar.ToType())
		t.Cleanup(func() { params.Free(c.proc.Mp()) })
		require.NoError(t, vector.AppendBytes(params, []byte("4"), false, c.proc.Mp()))
		require.NoError(t, vector.AppendBytes(params, []byte(tc.second), tc.null, c.proc.Mp()))
		c.proc.SetPrepareParams(params)
		for _, exe := range c.filterExprExes {
			exe.ResetForNextQuery()
		}
		block, err := s.handleRuntimeFilters(c, nil)
		require.NoError(t, err)
		require.Len(t, block, 2, "不同 Fold 参数不能因为本次值相同而永久合并")
		require.Len(t, s.DataSource.BlockFilterList, 2)
		require.Len(t, c.filterExprExes, 2, "重用不能积累额外 executor")
		first := decodeBlockFilterVector(t, c.proc, block[0])
		second := decodeBlockFilterVector(t, c.proc, block[1])
		require.Equal(t, []int32{1, 4}, vector.MustFixedColWithTypeCheck[int32](first))
		if tc.null {
			require.True(t, second.IsNull(1))
		} else if tc.second == "9" {
			require.Equal(t, []int32{1, 9}, vector.MustFixedColWithTypeCheck[int32](second))
		} else {
			require.Equal(t, []int32{1, 4}, vector.MustFixedColWithTypeCheck[int32](second))
		}
	}
}

func TestRuntimeFilterSpecAssociation(t *testing.T) {
	for _, nonPK := range []bool{false, true} {
		name := "primary-key"
		if nonPK {
			name = "non-primary-key"
		}
		t.Run(name, func(t *testing.T) {
			c := NewMockCompile(t)
			c.proc.SetMessageBoard(message.NewMessageBoard())
			col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "k", ColPos: 0}}}
			specs := []*plan.RuntimeFilterSpec{{Tag: 1, Expr: col, NotOnPk: !nonPK}, {Tag: 2, Expr: col, NotOnPk: nonPK}}
			scan := table_scan.NewArgument()
			t.Cleanup(scan.Release)
			s := &Scope{Proc: c.proc, DataSource: &Source{RuntimeFilterSpecs: specs}, RootOp: scan}
			vec := vector.NewVec(types.T_int32.ToType())
			t.Cleanup(func() { vec.Free(c.proc.Mp()) })
			require.NoError(t, vector.AppendFixed(vec, int32(7), false, c.proc.Mp()))
			data, err := vec.MarshalBinary()
			require.NoError(t, err)
			message.SendMessage(message.RuntimeFilterMessage{Tag: 1, Typ: message.RuntimeFilter_PASS}, c.proc.GetMessageBoard())
			message.SendMessage(message.RuntimeFilterMessage{Tag: 2, Typ: message.RuntimeFilter_IN, Card: 1, Data: data}, c.proc.GetMessageBoard())
			filters, drop, err := s.waitForRuntimeFilters(c)
			require.NoError(t, err)
			require.False(t, drop)
			block, err := s.handleRuntimeFilters(c, filters)
			require.NoError(t, err)
			require.Len(t, block, 1)
			if nonPK {
				require.Nil(t, s.DataSource.FilterExpr, "PASS must not shift the remaining IN expression onto a PK spec")
				require.Len(t, scan.RuntimeFilterExprs, 1)
			} else {
				require.NotNil(t, s.DataSource.FilterExpr)
				require.Empty(t, scan.RuntimeFilterExprs)
			}
		})
	}
}

func TestRuntimeFilterTerminalMessageContract(t *testing.T) {
	for _, tc := range []struct {
		name      string
		types     []int32
		wantDrop  bool
		wantError bool
	}{
		{name: "pass", types: []int32{message.RuntimeFilter_PASS}},
		{name: "parallel-shuffle-pass", types: []int32{message.RuntimeFilter_PASS, message.RuntimeFilter_PASS}},
		{name: "drop", types: []int32{message.RuntimeFilter_DROP}, wantDrop: true},
		{name: "duplicate-in", types: []int32{message.RuntimeFilter_IN, message.RuntimeFilter_IN}, wantError: true},
		{name: "conflicting-terminal", types: []int32{message.RuntimeFilter_PASS, message.RuntimeFilter_DROP}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewMockCompile(t)
			c.proc.SetMessageBoard(message.NewMessageBoard())
			s := &Scope{Proc: c.proc, DataSource: &Source{RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{{Tag: 1}}}}
			for _, typ := range tc.types {
				message.SendMessage(message.RuntimeFilterMessage{Tag: 1, Typ: typ}, c.proc.GetMessageBoard())
			}
			filters, drop, err := s.waitForRuntimeFilters(c)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantDrop, drop)
			}
			require.Empty(t, filters)
		})
	}
}

func TestRuntimeFilterCancellationIsError(t *testing.T) {
	for _, name := range []string{"waiting", "already-queued"} {
		t.Run(name, func(t *testing.T) {
			c := &Compile{proc: testutil.NewProcess(t)}
			c.proc.SetMessageBoard(message.NewMessageBoard())
			if name == "already-queued" {
				message.SendMessage(message.RuntimeFilterMessage{Tag: 1, Typ: message.RuntimeFilter_PASS}, c.proc.GetMessageBoard())
			}
			ctx, cancel := context.WithCancel(c.proc.Ctx)
			cancel()
			c.proc.Ctx = ctx
			s := &Scope{Proc: c.proc, DataSource: &Source{RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{{Tag: 1}}}}
			_, _, err := s.waitForRuntimeFilters(c)
			require.ErrorIs(t, err, context.Canceled, "canceled filtering must not become a successful query with no output")
		})
	}
}

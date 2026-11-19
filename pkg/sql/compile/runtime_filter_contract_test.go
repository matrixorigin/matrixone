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
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
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

// 这些表达式测试只需要 process/mpool；不安装自增服务或创建文件服务。
// 借用的 runtime 服务不归 fixture 所有，只有本地资源由 Cleanup 逆序释放。
func newBlockFilterCompile(t testing.TB) *Compile {
	t.Helper()
	mp, err := mpool.NewMPool(t.Name(), 0, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.AllocatedBytes())
	})
	ctx, cancel := context.WithCancel(defines.AttachAccountId(context.Background(), catalog.System_Account))
	t.Cleanup(cancel)
	proc := process.NewTopProcess(ctx, mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	t.Cleanup(proc.Free)
	c := &Compile{proc: proc}
	t.Cleanup(func() {
		for _, exe := range c.filterExprExes {
			exe.Free()
		}
	})
	return c
}

func TestBlockFilterFixtureOwnership(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	before, existed := rt.GetGlobalVariables(moruntime.AutoIncrementService)
	var proc *process.Process
	var name string
	t.Run("owned process", func(t *testing.T) {
		name = t.Name()
		c := newBlockFilterCompile(t)
		proc = c.proc
		current, exists := rt.GetGlobalVariables(moruntime.AutoIncrementService)
		require.Equal(t, existed, exists, "纯表达式 fixture 不应安装新服务")
		if before == nil {
			require.Nil(t, current)
		} else {
			require.Same(t, before, current, "不能替换 runtime 已有服务")
		}
		require.Nil(t, proc.GetFileService(), "纯表达式 fixture 不需要文件服务")
		require.NotEqual(t, "[]", mpool.ReportMemUsage(name))
		buf, err := proc.Mp().Alloc(128, true)
		require.NoError(t, err)
		t.Cleanup(func() { proc.Mp().Free(buf) })
		require.Positive(t, proc.Mp().CurrNB())
	})
	require.ErrorIs(t, proc.Ctx.Err(), context.Canceled)
	require.Zero(t, proc.Mp().CurrNB())
	require.Zero(t, proc.Mp().AllocatedBytes())
	require.Equal(t, "[]", mpool.ReportMemUsage(name), "拥有的池必须注销，而非仅释放向量")
}

func TestInternalInsertFinalBlockFilters(t *testing.T) {
	c := newBlockFilterCompile(t)
	ctx := &blockFilterInsertContext{CompilerContext: plan2.NewMockCompilerContext(true), proc: c.proc}
	stmt, err := mysql.ParseOne(c.proc.Ctx, "insert into nation2 select * from nation where n_nationkey in (1,2,3,4) and n_nationkey not in (2,3)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	// 内部 INSERT 与 CTAS populateCreatedTable 不经过 BaseOptimizer。
	pn, err := plan2.BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	require.Equal(t, plan.Query_INSERT, pn.GetQuery().StmtType)
	scans := 0
	for _, node := range pn.GetQuery().Nodes {
		if node.NodeType != plan.Node_TABLE_SCAN || node.TableDef.Name != "nation" {
			continue
		}
		scans++
		require.Len(t, node.BlockFilterList, 3, "反例必须实际绕过 optimizer 的最终去重")
		s := &Scope{Proc: c.proc, DataSource: &Source{}}
		require.NoError(t, c.prepareScanBlockFilters(s.DataSource, node.BlockFilterList))
		view := s.DataSource.readerBlockFilters
		require.Len(t, view, 2)
		for range 2 {
			require.NoError(t, c.prepareScanBlockFilters(s.DataSource, node.BlockFilterList))
			require.Same(t, &view[0], &s.DataSource.readerBlockFilters[0], "复用同一代静态 reader 视图")
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
	c := newBlockFilterCompile(t)
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
		filters = append(filters, filter)
	}
	s := &Scope{Proc: c.proc, DataSource: &Source{}}
	require.NoError(t, c.prepareScanBlockFilters(s.DataSource, filters))
	require.Len(t, c.filterExprExes, 2)
	require.Nil(t, s.DataSource.readerBlockFilters, "动态 Fold 不得按当前值缓存去重")
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

func TestReaderDeduplicatesOnlyStaticFilters(t *testing.T) {
	c := newBlockFilterCompile(t)
	col := plan2.GetColExpr(plan.Type{Id: int32(types.T_int32)}, 1, 0)
	v := vector.NewVec(types.T_int32.ToType())
	defer v.Free(c.proc.Mp())
	require.NoError(t, vector.AppendFixedList(v, []int32{1, 4}, nil, c.proc.Mp()))
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	runtimeExpr := plan2.MakeInExpr(c.proc.Ctx, col, 2, data, false)
	static := plan2.DeepCopyExpr(runtimeExpr)
	other := plan2.MakeInExpr(c.proc.Ctx, plan2.GetColExpr(col.Typ, 1, 1), 2, data, false)
	source := []*plan.Expr{static, other, plan2.DeepCopyExpr(other)}
	s := &Scope{Proc: c.proc, DataSource: &Source{}}
	require.NoError(t, c.prepareScanBlockFilters(s.DataSource, source))
	compiled := s.DataSource.BlockFilterList
	runtime := []runtimeFilterExpr{{expr: runtimeExpr}, {expr: runtimeExpr}}
	for range 2 {
		s.DataSource.FilterExpr = nil
		block, err := s.handleRuntimeFilters(c, runtime)
		require.NoError(t, err)
		require.Len(t, block, 4, "只消除静态区内部重复，runtime 与跨区表达式保持原样")
		for i, original := range []*plan.Expr{runtimeExpr, runtimeExpr, compiled[0], compiled[1]} {
			require.Same(t, original, block[i])
		}
		require.Len(t, s.DataSource.BlockFilterList, 3)
		require.NotNil(t, compiled[2], "独立输出切片不能压缩缓存的 Source")
	}
	// 更换源列表时旧的两项视图不能泄漏到新一代（包括空列表）。
	for _, next := range [][]*plan.Expr{{static}, nil} {
		require.NoError(t, c.prepareScanBlockFilters(s.DataSource, next))
		block, err := s.handleRuntimeFilters(c, nil)
		require.NoError(t, err)
		require.Len(t, block, len(next))
		require.Nil(t, s.DataSource.readerBlockFilters)
	}
}

func blockFilterTestScan(t testing.TB, c *Compile, predicate string, ordinary bool) *plan.Node {
	ctx := &blockFilterInsertContext{CompilerContext: plan2.NewMockCompilerContext(true), proc: c.proc}
	sql := "select * from nation where " + predicate
	if !ordinary {
		sql = "insert into nation2 " + sql
	}
	stmt, err := mysql.ParseOne(c.proc.Ctx, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	var qry *plan.Query
	if ordinary {
		qry, err = plan2.NewBaseOptimizer(ctx).Optimize(stmt, false)
	} else {
		var pn *plan.Plan
		pn, err = plan2.BuildPlan(ctx, stmt, false)
		if err == nil {
			qry = pn.GetQuery()
		}
	}
	require.NoError(t, err)
	for _, node := range qry.Nodes {
		if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.Name == "nation" {
			return node
		}
	}
	t.Fatal("没有生成目标扫描")
	return nil
}

func TestReaderStringInLogicalIdentity(t *testing.T) {
	c := newBlockFilterCompile(t)
	a, b := strings.Repeat("a", 30), strings.Repeat("b", 30)
	node := blockFilterTestScan(t, c, "n_name in ('"+a+"','"+b+"') and n_name in ('"+b+"','"+a+"')", false)
	s := &Scope{Proc: c.proc, DataSource: &Source{}}
	require.NoError(t, c.prepareScanBlockFilters(s.DataSource, node.BlockFilterList))
	filters := s.DataSource.BlockFilterList
	require.Len(t, filters, 4, "真实 BuildPlan 保留两种顺序各自的 List/Vec")
	first, second := filters[0].GetF().Args[1].GetVec(), filters[1].GetF().Args[1].GetVec()
	require.NotNil(t, first)
	require.NotNil(t, second)
	require.False(t, bytes.Equal(first.Data, second.Data), "必须实际覆盖不同 area 布局")
	firstData, secondData := bytes.Clone(first.Data), bytes.Clone(second.Data)
	for range 2 {
		block, err := s.handleRuntimeFilters(c, nil)
		require.NoError(t, err)
		require.Len(t, block, 1, "同一字符串域不能因物理偏移不同而重复交给 reader")
		require.Same(t, filters[0], block[0])
		v := decodeBlockFilterVector(t, c.proc, block[0])
		require.Equal(t, []string{a, b}, []string{string(v.GetBytesAt(0)), string(v.GetBytesAt(1))})
		require.Len(t, s.DataSource.BlockFilterList, 4)
		require.Equal(t, firstData, first.Data)
		require.Equal(t, secondData, second.Data)
	}
}

func TestReaderNullableInLogicalDomains(t *testing.T) {
	for _, ordinary := range []bool{false, true} {
		for _, tc := range []struct {
			name, predicate string
			want            int
		}{
			{"numeric permutation", "n_regionkey in (1,2,null) and n_regionkey in (2,1,null)", 1},
			{"string multiplicity", "n_name in ('a','b',null) and n_name in ('b','a','a',null,null)", 1},
			{"distinct numeric domains", "n_regionkey in (1,2,null) and n_regionkey in (2,3,null)", 2},
		} {
			mode := "internal insert/"
			if ordinary {
				mode = "ordinary/"
			}
			t.Run(mode+tc.name, func(t *testing.T) {
				c := newBlockFilterCompile(t)
				node := blockFilterTestScan(t, c, tc.predicate, ordinary)
				s := &Scope{Proc: c.proc, DataSource: &Source{}}
				require.NoError(t, c.prepareScanBlockFilters(s.DataSource, node.BlockFilterList))
				filters := s.DataSource.BlockFilterList
				before := plan2.DeepCopyExprList(filters)
				for range 2 {
					block, err := s.handleRuntimeFilters(c, nil)
					require.NoError(t, err)
					require.Len(t, block, tc.want, "只执行一次相同逻辑域，保留不同域")
					require.Same(t, filters[0], block[0])
					require.Equal(t, before, s.DataSource.BlockFilterList, "不能改变缓存、NULL对齐或原始payload")
					for _, f := range block {
						v := decodeBlockFilterVector(t, c.proc, f)
						require.True(t, v.GetNulls().Any(), "NULL存在性不能从执行向量中删除")
					}
				}
			})
		}
	}
}

// 普通optimizer已经完成折叠；测量Scope不能再次解码/散列不可能重复的静态过滤。
func BenchmarkReaderDistinctStaticFilters(b *testing.B) {
	for _, tc := range []struct{ name, predicate string }{
		{"numeric plus comparison", "n_regionkey in (1,2) and n_nationkey > 0"},
		{"different string columns", "n_name in ('a','b') and n_comment in ('a','b')"},
		{"same column numeric rewrite", "n_nationkey in (1,2,3,4) and n_nationkey not in (2,3)"},
		{"same column nullable strings", "n_name in ('a','b',null) and n_name in ('b','c',null)"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c := newBlockFilterCompile(b)
			node := blockFilterTestScan(b, c, tc.predicate, true)
			s := &Scope{Proc: c.proc, DataSource: &Source{}}
			require.NoError(b, c.prepareScanBlockFilters(s.DataSource, node.BlockFilterList))
			filters := s.DataSource.BlockFilterList
			require.Len(b, filters, 2)
			require.Nil(b, s.DataSource.readerBlockFilters)
			b.ReportAllocs()
			for b.Loop() {
				block, err := s.handleRuntimeFilters(c, nil)
				if err != nil || len(block) != 2 || block[0] != filters[0] || block[1] != filters[1] {
					b.Fatalf("distinct static filters changed: %v", err)
				}
			}
		})
	}
}

func TestFoldedStringInPayloadBudget(t *testing.T) {
	c := newBlockFilterCompile(t)
	a, b := "'"+strings.Repeat("a", 30)+"'", "'"+strings.Repeat("b", 30)+"'"
	// 用跨越4KiB的最小量级，100001项仅在显式benchmark中出现。
	node := blockFilterTestScan(t, c, "n_name in ("+strings.Repeat(a+",", 137)+b+")", false)
	require.Len(t, node.FilterList, 1)
	vec := node.FilterList[0].GetF().Args[1].GetVec()
	require.NotNil(t, vec)
	require.Equal(t, int32(2), vec.Len)
	require.Greater(t, len(vec.Data), 4096, "折叠后 area 仍保留未引用字节")
	require.Empty(t, node.BlockFilterList, "逻辑项数小不能绕过物理预算")
}

func BenchmarkReaderFoldedStringPayload(b *testing.B) {
	c := newBlockFilterCompile(b)
	// 138项的真实SQL回归已证明可达；成本测量直接复用常量折叠的向量操作，
	// 不把100001项SQL的解析/绑定成本混进reader benchmark。
	largeVec := vector.NewVec(types.T_varchar.ToType())
	defer largeVec.Free(c.proc.Mp())
	a := []byte(strings.Repeat("a", 30))
	for range 100_000 {
		require.NoError(b, vector.AppendBytes(largeVec, a, false, c.proc.Mp()))
	}
	require.NoError(b, vector.AppendBytes(largeVec, []byte(strings.Repeat("z", 30)), false, c.proc.Mp()))
	largeVec.InplaceSortAndCompact()
	payload, err := largeVec.MarshalBinary()
	require.NoError(b, err)
	require.Equal(b, 2, largeVec.Length())
	require.Greater(b, len(payload), 3_000_000)
	large := plan2.MakeInExpr(c.proc.Ctx, plan2.GetColExpr(plan.Type{Id: int32(types.T_varchar)}, 1, 1), 2, payload, false)
	col := plan2.GetColExpr(plan.Type{Id: int32(types.T_int32)}, 1, 2)
	v := vector.NewVec(types.T_int32.ToType())
	defer v.Free(c.proc.Mp())
	require.NoError(b, vector.AppendFixedList(v, []int32{1, 4}, nil, c.proc.Mp()))
	data, err := v.MarshalBinary()
	require.NoError(b, err)
	small := plan2.MakeInExpr(c.proc.Ctx, col, 2, data, false)
	// 模拟仍可经原统计/hint路径送入的过滤，去重本身也必须守住物理预算。
	s := &Scope{Proc: c.proc, DataSource: &Source{BlockFilterList: []*plan.Expr{large, small}}}
	b.ReportAllocs()
	for b.Loop() {
		block, err := s.handleRuntimeFilters(c, nil)
		if err != nil || len(block) != 2 || block[0] != large || block[1] != small {
			b.Fatalf("static filters changed: %v", err)
		}
	}
}

// 向量构造不计时；百万键是现有 JOIN runtime filter 的支持范围。
func BenchmarkReaderLargeRuntimeFilter(b *testing.B) {
	c := newBlockFilterCompile(b)
	proc := c.proc
	col := plan2.GetColExpr(plan.Type{Id: int32(types.T_int32)}, 1, 0)
	v := vector.NewVec(types.T_int32.ToType())
	defer v.Free(proc.Mp())
	values := make([]int32, 1_000_000)
	for i := range values {
		values[i] = int32(i)
	}
	require.NoError(b, vector.AppendFixedList(v, values, nil, proc.Mp()))
	data, err := v.MarshalBinary()
	require.NoError(b, err)
	runtime := []runtimeFilterExpr{{expr: plan2.MakeInExpr(proc.Ctx, col, int32(len(values)), data, false)}}
	v2 := vector.NewVec(types.T_int32.ToType())
	defer v2.Free(proc.Mp())
	require.NoError(b, vector.AppendFixedList(v2, []int32{1, 4}, nil, proc.Mp()))
	data2, err := v2.MarshalBinary()
	require.NoError(b, err)
	static := plan2.MakeInExpr(proc.Ctx, plan2.GetColExpr(col.Typ, 1, 1), 2, data2, false)
	s := &Scope{Proc: proc, DataSource: &Source{BlockFilterList: []*plan.Expr{static}}}
	b.ReportAllocs()
	for b.Loop() {
		s.DataSource.FilterExpr = nil
		block, err := s.handleRuntimeFilters(c, runtime)
		if err != nil || len(block) != 2 || block[0] != runtime[0].expr || block[1] != static {
			b.Fatalf("runtime/static filters changed: %v", err)
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

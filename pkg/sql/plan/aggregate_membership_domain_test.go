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

package plan

import (
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func aggregateMembershipTestBuilder(t *testing.T) (*QueryBuilder, int32) {
	t.Helper()
	ctx := &fixedStatsCompilerContext{&statsCacheCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)), statsCache: NewStatsCache(),
	}}
	b := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	k := plan.Type{Id: int32(types.T_int32), NotNullable: true}
	v := plan.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2}
	text := plan.Type{Id: int32(types.T_varchar), Width: 55}
	bind := func(name string, args ...*plan.Expr) *plan.Expr {
		expr, err := BindFuncExprImplByPlanExpr(b.GetContext(), name, args)
		require.NoError(t, err)
		return expr
	}
	scans := make([]*plan.Node, 0, 3)
	for i, cols := range [][]*plan.ColDef{
		{{Name: "part", Typ: k}, {Name: "supp", Typ: k}},
		{{Name: "part", Typ: k}, {Name: "name", Typ: text}},
		{{Name: "part", Typ: k}, {Name: "supp", Typ: k}, {Name: "v", Typ: v}},
	} {
		tag := b.genNewBindTag()
		def := &plan.TableDef{TblId: uint64(1000 + i), Cols: cols}
		id := b.appendNode(&plan.Node{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{tag},
			ObjRef: &plan.ObjectRef{Obj: int64(def.TblId)}, TableDef: def,
			Stats: &plan.Stats{Outcnt: 100000, TableCnt: 100000, Selectivity: 1}}, nil)
		b.tag2Table[tag] = def
		ctx.statsCache.Set(def.TblId, &statsinfo.StatsInfo{TableCnt: 100000,
			NdvMap: map[string]float64{"part": 5000, "supp": 2000}})
		scans = append(scans, b.qry.Nodes[id])
	}
	left, domain, fact := scans[0], scans[1], scans[2]
	domain.FilterList = []*plan.Expr{bind("like", GetColExpr(text, domain.BindingTags[0], 1), makePlan2StringConstExprWithType("lime%"))}
	domain.Stats = &plan.Stats{Outcnt: 100, TableCnt: 10000, Selectivity: .01}
	semi := b.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_SEMI, Children: []int32{left.NodeId, domain.NodeId},
		OnList: []*plan.Expr{bind("=", GetColExpr(k, left.BindingTags[0], 0), GetColExpr(k, domain.BindingTags[0], 0))}}, nil)
	sum := bind("sum", GetColExpr(v, fact.BindingTags[0], 2))
	groupTag, sumTag := b.genNewBindTag(), b.genNewBindTag()
	agg := b.appendNode(&plan.Node{NodeType: plan.Node_AGG, Children: []int32{fact.NodeId}, BindingTags: []int32{groupTag, sumTag},
		GroupBy: []*plan.Expr{GetColExpr(k, fact.BindingTags[0], 0), GetColExpr(k, fact.BindingTags[0], 1)},
		AggList: []*plan.Expr{sum}, Stats: &plan.Stats{Outcnt: 50000}}, nil)
	join := b.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_LEFT, Children: []int32{semi, agg},
		OnList: []*plan.Expr{
			bind("=", GetColExpr(k, left.BindingTags[0], 0), GetColExpr(k, groupTag, 0)),
			bind("=", GetColExpr(k, left.BindingTags[0], 1), GetColExpr(k, groupTag, 1)),
		}}, nil)
	root := b.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{join}, BindingTags: []int32{b.genNewBindTag()},
		ProjectList: []*plan.Expr{GetColExpr(k, left.BindingTags[0], 0), GetColExpr(sum.Typ, sumTag, 0)}}, nil)
	return b, root
}

func TestAggregateMembershipRestrictsOnlyFactInput(t *testing.T) {
	b, root := aggregateMembershipTestBuilder(t)
	original := proto.Clone(b.qry.Nodes[1]).(*plan.Node)
	oldOn := DeepCopyExprList(b.qry.Nodes[5].OnList)
	require.True(t, b.restrictAggregateMembership(root))
	agg := b.qry.Nodes[4]
	input := b.qry.Nodes[agg.Children[0]]
	require.Equal(t, plan.Node_JOIN, input.NodeType)
	require.Equal(t, plan.Node_SEMI, input.JoinType)
	require.Equal(t, int32(2), input.Children[0])
	copy := b.qry.Nodes[input.Children[1]]
	require.NotEqual(t, original.BindingTags[0], copy.BindingTags[0])
	require.True(t, proto.Equal(original.ObjRef, copy.ObjRef))
	require.True(t, proto.Equal(original.ScanSnapshot, copy.ScanSnapshot))
	require.True(t, proto.Equal(original, b.qry.Nodes[1]), "the original mandatory membership is retained unchanged")
	require.Equal(t, original.BindingTags[0], b.qry.Nodes[3].OnList[0].GetF().Args[1].GetCol().RelPos)
	require.Equal(t, copy.BindingTags[0], copy.FilterList[0].GetF().Args[0].GetCol().RelPos)
	require.Equal(t, oldOn, b.qry.Nodes[5].OnList, "retain both correlation keys and original scalar consumer")
	before := proto.Clone(b.qry)
	require.False(t, b.restrictAggregateMembership(root))
	require.True(t, proto.Equal(before, b.qry), "an already restricted aggregate is not wrapped again")
}

func TestAggregateMembershipRejectsUnsafeOrUnprofitableDomains(t *testing.T) {
	for _, name := range []string{"negative", "optional", "mixed-type", "no-filter", "barrier", "unknown-ndv", "expensive-domain", "unselective", "distinct", "unsafe-sum", "shared", "disabled"} {
		t.Run(name, func(t *testing.T) {
			b, root := aggregateMembershipTestBuilder(t)
			domain, semi, agg := b.qry.Nodes[1], b.qry.Nodes[3], b.qry.Nodes[4]
			switch name {
			case "negative":
				semi.JoinType = plan.Node_ANTI
			case "optional":
				semi.JoinType = plan.Node_LEFT
			case "mixed-type":
				semi.OnList[0].GetF().Args[1].Typ.Id = int32(types.T_int64)
			case "no-filter":
				domain.FilterList = nil
			case "barrier":
				domain.FilterIsBarrier = true
			case "unknown-ndv":
				b.compCtx.GetStatsCache().Delete(domain.TableDef.TblId)
			case "expensive-domain":
				domain.Stats.TableCnt = 1e12
			case "unselective":
				domain.Stats.Outcnt, domain.Stats.Selectivity = 100000, 1
			case "distinct":
				fn := agg.AggList[0].GetF()
				fn.Func.Obj = int64(uint64(fn.Func.Obj) | function.Distinct)
			case "unsafe-sum":
				agg.AggList[0].Typ.Width = 10
			case "shared":
				b.qry.Steps = []int32{root, domain.NodeId}
			case "disabled":
				b.optimizerHints = &OptimizerHints{aggPushDown: 1}
			}
			before := proto.Clone(b.qry)
			require.False(t, b.restrictAggregateMembership(root))
			require.True(t, proto.Equal(before, b.qry), "rejection must retain the valid original graph")
		})
	}
}

func TestOrdinaryScanPreservesIntegerRangeFilterOrder(t *testing.T) {
	ctx := &fixedStatsCompilerContext{&statsCacheCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)), statsCache: NewStatsCache(),
	}}
	part := ctx.tables["part"]
	ctx.statsCache.Set(part.TblId, &statsinfo.StatsInfo{
		TableName: "part", TableCnt: 3, BlockNumber: 1,
		NdvMap:      map[string]float64{"p_partkey": 3},
		MinValMap:   map[string]float64{"p_partkey": 1},
		MaxValMap:   map[string]float64{"p_partkey": 20},
		DataTypeMap: map[string]uint64{"p_partkey": uint64(types.T_int32)},
	})
	for _, tc := range []struct{ name, filter string }{
		{"range-first", "p_partkey > 1 and regexp_like('a', p_name)"},
		{"regexp-first", "regexp_like('a', p_name) and p_partkey > 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(ctx.GetContext(), "select p_partkey from part where "+tc.filter, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			p, err := BuildPlan(ctx, stmt, false, nil)
			require.NoError(t, err)
			var scan *plan.Node
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == plan.Node_TABLE_SCAN {
					require.Nil(t, scan)
					scan = node
				}
			}
			require.NotNil(t, scan)
			require.Len(t, scan.FilterList, 2)
			require.Equal(t, ">", scan.FilterList[0].GetF().Func.ObjName)
			require.Equal(t, "regexp_like", scan.FilterList[1].GetF().Func.ObjName)
		})
	}
}

func TestAggregateMembershipSQLReachability(t *testing.T) {
	sql := `select s_name, s_address from supplier, nation
		where s_suppkey in (
			select ps_suppkey from partsupp
			where ps_partkey in (select p_partkey from part where p_name like 'forest%')
			and ps_availqty > (
				select 0.5 * sum(l_quantity) from lineitem
				where l_partkey = ps_partkey and l_suppkey = ps_suppkey
				and l_shipdate >= date '1994-01-01'
				and l_shipdate < date '1994-01-01' + interval '1' year
			)
		)
		and s_nationkey = n_nationkey and n_name = 'CANADA'
		order by s_name`
	for _, tc := range []struct {
		name           string
		predicate      string
		partRows       float64
		partKeyNDV     float64
		keyType        types.T
		metadataStats  bool
		missingPartNDV bool
		nullableName   bool
		partNameNulls  uint64
		restricted     bool
	}{
		{name: "selective-prefix", predicate: "p_name like 'forest%'", restricted: true},
		{name: "single-percent", predicate: "p_name like '%'"},
		{name: "small-domain-single-percent", predicate: "p_name like '%'", partRows: 1000, restricted: true},
		{name: "metadata-full-domain", predicate: "p_name like '%'", metadataStats: true},
		{name: "metadata-near-full-domain", predicate: "p_name like '%' and p_partkey between 1 and 19998", metadataStats: true},
		{name: "metadata-near-full-domain-underestimated-primary-key", predicate: "p_name like '%' and p_partkey between 1 and 19998",
			partKeyNDV: 1000, metadataStats: true},
		{name: "metadata-bigint-full-domain", predicate: "p_name like '%'", keyType: types.T_int64, metadataStats: true},
		{name: "metadata-unsigned-near-full-domain", predicate: "p_name like '%' and p_partkey between 1 and 19998",
			keyType: types.T_uint64, metadataStats: true},
		{name: "metadata-inclusive-lower-bound", predicate: "p_name like '%' and p_partkey >= 1", metadataStats: true},
		{name: "metadata-inclusive-upper-bound", predicate: "p_name like '%' and p_partkey <= 20000", metadataStats: true},
		{name: "metadata-strict-outside-bound", predicate: "p_name like '%' and p_partkey > 0", metadataStats: true},
		{name: "metadata-strict-edge-bound", predicate: "p_name like '%' and p_partkey < 20000", metadataStats: true},
		{name: "metadata-nested-range-disjunction", predicate: "p_name like '%' and (p_partkey >= 1 or p_name = 'cherry')", metadataStats: true},
		{name: "metadata-selective-single-range", predicate: "p_name like '%' and p_partkey <= 1000", metadataStats: true, restricted: true},
		{name: "metadata-selective-range", predicate: "p_name like '%' and p_partkey between 1 and 1000", metadataStats: true, restricted: true},
		{name: "metadata-range-without-wildcard", predicate: "p_partkey between 1 and 19998", metadataStats: true, restricted: true},
		{name: "metadata-small-primary-key-domain", predicate: "p_name like '%'", partRows: 1000, metadataStats: true, missingPartNDV: true, restricted: true},
		{name: "metadata-small-primary-key-domain-with-range", predicate: "p_name like '%' and p_partkey between 1 and 1000",
			partRows: 1000, metadataStats: true, missingPartNDV: true, restricted: true},
		{name: "nullable-name-high-null-count", predicate: "p_name like '%'", nullableName: true, partNameNulls: 16000, restricted: true},
		{name: "nullable-name-no-nulls", predicate: "p_name like '%'", nullableName: true},
		{name: "multiple-percent", predicate: "p_name like '%%'"},
		{name: "no-filter", predicate: "true"},
		{name: "negated-wildcard", predicate: "not (p_name like '%')", restricted: true},
		{name: "not-like-wildcard", predicate: "p_name not like '%'", restricted: true},
		{name: "double-negated-wildcard", predicate: "not (not (p_name like '%'))"},
		{name: "wildcard-is-true", predicate: "(p_name like '%') is true"},
		{name: "wildcard-dominates-or", predicate: "p_partkey = 1 or p_name like '%'"},
		{name: "selective-or", predicate: "(p_partkey = 1 and p_name like '%') or p_partkey = 2", restricted: true},
		{name: "selective-and-is-true", predicate: "(p_name like '%' and p_name like 'forest%') is true", restricted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &fixedStatsCompilerContext{&statsCacheCompilerContext{
				MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)), statsCache: NewStatsCache(),
			}}
			partRows := tc.partRows
			if partRows == 0 {
				partRows = 20000
			}
			for _, name := range []string{"part", "partsupp", "lineitem", "supplier", "nation"} {
				table := ctx.tables[name]
				rows := map[string]float64{"part": partRows, "partsupp": 80000, "lineitem": 6000000, "supplier": 1000, "nation": 25}[name]
				s := &statsinfo.StatsInfo{TableCnt: rows, BlockNumber: 1, TableName: name,
					NdvMap: map[string]float64{}, SizeMap: map[string]uint64{}, DataTypeMap: map[string]uint64{}}
				keyName := map[string]string{"part": "p_partkey", "partsupp": "ps_partkey", "lineitem": "l_partkey"}[name]
				for _, col := range table.Cols {
					if name == "part" && tc.nullableName && col.Name == "p_name" {
						col.Typ.NotNullable = false
						s.NullCntMap = map[string]uint64{"p_name": tc.partNameNulls}
					}
					if tc.keyType != types.T_any && col.Name == keyName {
						col.Typ.Id = int32(tc.keyType)
					}
					s.NdvMap[col.Name], s.SizeMap[col.Name], s.DataTypeMap[col.Name] = rows, uint64(rows*8), uint64(col.Typ.Id)
				}
				switch name {
				case "part":
					if tc.partKeyNDV > 0 {
						s.NdvMap["p_partkey"] = tc.partKeyNDV
					}
					if tc.metadataStats && !tc.missingPartNDV {
						s.MinValMap = map[string]float64{"p_partkey": 1}
						s.MaxValMap = map[string]float64{"p_partkey": partRows}
					}
					if tc.missingPartNDV {
						// A small table can have a transient row count before column statistics are published.
						delete(s.NdvMap, "p_partkey")
						s.TableName = ""
					}
				case "partsupp":
					s.NdvMap["ps_partkey"], s.NdvMap["ps_suppkey"] = 20000, 1000
				case "lineitem":
					s.NdvMap["l_partkey"], s.NdvMap["l_suppkey"] = 20000, 1000
					if tc.metadataStats {
						// Overlapping objects can overestimate NDV even with complete integer bounds.
						s.NdvMap["l_partkey"] = 60144
						s.MinValMap = map[string]float64{"l_partkey": 1}
						s.MaxValMap = map[string]float64{"l_partkey": 20000}
					}
				}
				ctx.statsCache.Set(table.TblId, s)
			}
			stmt, err := mysql.ParseOne(ctx.GetContext(), strings.Replace(sql, "p_name like 'forest%'", tc.predicate, 1), 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			p, err := BuildPlan(ctx, stmt, false, nil)
			require.NoError(t, err)
			query := p.GetQuery()
			var aggregate *plan.Node
			seen := make(map[int32]bool)
			var visit func(int32)
			visit = func(id int32) {
				if seen[id] {
					return
				}
				seen[id] = true
				n := query.Nodes[id]
				if n.NodeType == plan.Node_AGG {
					for _, expr := range n.AggList {
						if fn := expr.GetF(); fn != nil && fn.Func.ObjName == "sum" {
							require.Nil(t, aggregate, "Q20 has one reachable SUM")
							aggregate = n
						}
					}
				}
				for _, child := range n.Children {
					visit(child)
				}
			}
			for _, root := range query.Steps {
				visit(root)
			}
			require.NotNil(t, aggregate)
			require.Len(t, aggregate.Children, 1)
			input := query.Nodes[aggregate.Children[0]]
			if !tc.restricted {
				require.Equal(t, plan.Node_TABLE_SCAN, input.NodeType, "SUM(l_quantity) input: %s", input)
				require.Equal(t, "lineitem", input.TableDef.Name)
				return
			}
			require.Equal(t, plan.Node_JOIN, input.NodeType)
			require.Len(t, input.Children, 2)
			require.Equal(t, plan.Node_SEMI, input.JoinType)
			factSide, domainSide := 0, 1
			if input.IsRightJoin {
				factSide, domainSide = 1, 0
			}
			fact, domain := query.Nodes[input.Children[factSide]], query.Nodes[input.Children[domainSide]]
			require.Equal(t, plan.Node_TABLE_SCAN, fact.NodeType)
			require.Equal(t, "lineitem", fact.TableDef.Name)
			require.Equal(t, plan.Node_TABLE_SCAN, domain.NodeType)
			require.Equal(t, "part", domain.TableDef.Name)
			require.Len(t, input.OnList, 1)
			fn := input.OnList[0].GetF()
			require.NotNil(t, fn)
			require.True(t, IsEqualFunc(fn.Func.Obj))
			require.Len(t, fn.Args, 2)
			first, second := fn.Args[0].GetCol(), fn.Args[1].GetCol()
			require.NotNil(t, first)
			require.NotNil(t, second)
			require.ElementsMatch(t, []string{"lineitem.l_partkey", "part.p_partkey"}, []string{first.Name, second.Name})
		})
	}
}

func TestAggregateMembershipMatchAllPattern(t *testing.T) {
	b, _ := aggregateMembershipTestBuilder(t)
	domain := b.qry.Nodes[1]
	tag := domain.BindingTags[0]
	literal := makePlan2StringConstExprWithType
	cast := func(id int32, value string, width int32) *plan.Expr {
		source := literal(value)
		target := source.Typ
		target.Width = width
		return &plan.Expr{Typ: target, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(id) << 32},
			Args: []*plan.Expr{source, {Typ: target, Expr: &plan.Expr_T{T: &plan.TargetType{}}}},
		}}}
	}
	withSource := literal("%")
	withSource.GetLit().Src = &plan.Expr{Typ: withSource.Typ, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	null := literal("%")
	null.GetLit().Isnull = true
	for _, tc := range []struct {
		name      string
		pattern   *plan.Expr
		escape    *plan.Expr
		binding   int32
		wantMatch bool
	}{
		{"single-percent", literal("%"), nil, tag, true},
		{"multiple-percent", literal("%%%"), nil, tag, true},
		{"literal-cast", cast(function.CAST, "%", 8), nil, tag, true},
		{"literal-strict-cast", cast(function.CAST_STRICT, "%%", 8), nil, tag, true},
		{"truncating-cast", cast(function.CAST, "%%", 1), nil, tag, false},
		{"non-ASCII-cast", cast(function.CAST, "％", 8), nil, tag, false},
		{"empty", literal(""), nil, tag, false},
		{"prefix", literal("lime%"), nil, tag, false},
		{"single-character", literal("%_%"), nil, tag, false},
		{"escaped-percent", literal(`\%`), nil, tag, false},
		{"explicit-escape", literal("%"), literal("%"), tag, false},
		{"parameter", withSource.GetLit().Src, nil, tag, false},
		{"execution-literal-source", withSource, nil, tag, false},
		{"NULL", null, nil, tag, false},
		{"foreign-binding", literal("%"), nil, b.qry.Nodes[0].BindingTags[0], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter := DeepCopyExpr(domain.FilterList[0])
			like := filter.GetF()
			like.Args[0].GetCol().RelPos = tc.binding
			like.Args[1] = tc.pattern
			if tc.escape != nil {
				like.Args = append(like.Args, tc.escape)
			}
			require.Equal(t, tc.wantMatch, membershipPredicateMatchesAll(filter, tag))
		})
	}
}

func TestAggregateMembershipPricesIndependentConjuncts(t *testing.T) {
	for _, tc := range []struct {
		name              string
		form              string
		lower, upper      int32
		notNullable       bool
		nullCount         uint64
		unpublished       bool
		snapshot          bool
		wrongStatsType    bool
		secondNullCount   uint64
		wantWildcardRows  float64
		keyNDV            float64
		tableRows         float64
		groups            float64
		removeStats       bool
		differentScanRows bool
		noPrimaryKeyStats bool
		useScanRows       bool
		wantRestriction   bool
	}{
		{name: "only-wildcard-exact-ndv", keyNDV: 5000},
		{name: "only-wildcard-nullable-published", nullCount: 8000, wantWildcardRows: 2000, keyNDV: 4500, wantRestriction: true},
		{name: "only-wildcard-nullable-all-nulls", nullCount: 10000, wantWildcardRows: 1, keyNDV: 4500, wantRestriction: true},
		{name: "only-wildcard-nullable-unpublished", nullCount: 8000, keyNDV: 4500, unpublished: true},
		{name: "only-wildcard-nullable-absent-column-stat", keyNDV: 4500},
		{name: "only-wildcard-nullable-invalid-null-count", nullCount: 10001, keyNDV: 4500},
		{name: "only-wildcard-nullable-inconsistent-type", nullCount: 8000, keyNDV: 4500, wrongStatsType: true},
		{name: "only-wildcard-nullable-snapshot", nullCount: 8000, keyNDV: 4500, snapshot: true},
		{name: "only-wildcard-not-null-ndv-drift", notNullable: true, nullCount: 8000, keyNDV: 4500},
		{name: "repeated-wildcard-same-column", form: "duplicate", nullCount: 8000, wantWildcardRows: 2000, keyNDV: 4500, wantRestriction: true},
		{name: "wildcards-on-two-columns", nullCount: 8000, secondNullCount: 9000, wantWildcardRows: 1000, keyNDV: 4500, wantRestriction: true},
		{name: "nullable-wildcard-caps-residual", form: "and", lower: 1, upper: 5000,
			nullCount: 8000, wantWildcardRows: 2000, keyNDV: 4500, wantRestriction: true},
		{name: "selective-residual-caps-nullable-wildcard", form: "and", lower: 10, upper: 20,
			nullCount: 8000, wantWildcardRows: 2000, keyNDV: 4500, wantRestriction: true},
		{name: "nullable-wildcard-with-missing-primary-key-ndv", form: "and", lower: 10, upper: 20,
			nullCount: 8000, wantWildcardRows: 2000, noPrimaryKeyStats: true, wantRestriction: true},
		{name: "missing-primary-key-ndv-keeps-only-null-bound", form: "and", lower: 10, upper: 20,
			nullCount: 5000, wantWildcardRows: 5000, noPrimaryKeyStats: true},
		{name: "only-wildcard-small-key-domain", notNullable: true, keyNDV: 1000, wantRestriction: true},
		{name: "only-wildcard-expensive-build", keyNDV: 1000, tableRows: 100000},
		{name: "only-wildcard-missing-key-ndv"},
		{name: "only-wildcard-missing-table-stats", keyNDV: 1000, removeStats: true},
		{name: "only-wildcard-inconsistent-stats", keyNDV: 1000, differentScanRows: true},
		{name: "nested-or-only", form: "or", nullCount: 8000, keyNDV: 5000},
		{name: "nested-not-only", form: "not", nullCount: 8000, keyNDV: 5000, useScanRows: true},
		{name: "nested-not-is-true", form: "not-istrue", nullCount: 8000, keyNDV: 5000, useScanRows: true},
		{name: "nested-double-not", form: "double-not", nullCount: 8000, keyNDV: 4500},
		{name: "nested-is-true", form: "istrue", nullCount: 8000, keyNDV: 4500},
		{name: "or-of-two-positive-filters", form: "or-of-and", keyNDV: 4500},
		{name: "strong-range", form: "and", lower: 10, upper: 20, keyNDV: 5000, wantRestriction: true},
		{name: "full-range-ndv-drift", form: "and", lower: 1, upper: 5000, keyNDV: 4500},
		{name: "full-range-small-key-domain", form: "and", lower: 1, upper: 5000, keyNDV: 1000, wantRestriction: true},
		{name: "weak-range", form: "and", lower: 1, upper: 4500, keyNDV: 5000},
		{name: "build-cost-boundary", form: "and", lower: 1, upper: 2000, keyNDV: 5000, groups: 25000},
		{name: "nested-or-with-strong-range", form: "or", lower: 10, upper: 20, keyNDV: 5000, wantRestriction: true},
		{name: "nested-not-with-strong-range", form: "not", lower: 10, upper: 20, keyNDV: 5000, useScanRows: true, wantRestriction: true},
		{name: "mixed-with-missing-key-ndv", form: "and", lower: 10, upper: 20},
		{name: "mixed-with-missing-primary-key-stats", form: "and", lower: 1, upper: 5000, noPrimaryKeyStats: true},
		{name: "mixed-with-missing-stats", form: "and", lower: 10, upper: 20, keyNDV: 5000, removeStats: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, root := aggregateMembershipTestBuilder(t)
			domain, fact, agg := b.qry.Nodes[1], b.qry.Nodes[2], b.qry.Nodes[4]
			// The fixture adds table statistics after constructing each scan.
			fact.Stats = calcScanStats(fact, b)
			agg.Stats.Outcnt = 50000
			tag := domain.BindingTags[0]
			domain.TableDef.Cols[1].Typ.NotNullable = tc.notNullable
			bind := func(name string, args ...*plan.Expr) *plan.Expr {
				expr, err := BindFuncExprImplByPlanExpr(b.GetContext(), name, args)
				require.NoError(t, err)
				return expr
			}
			name := GetColExpr(domain.TableDef.Cols[1].Typ, tag, 1)
			wildcard := bind("like", name, makePlan2StringConstExprWithType("%"))
			filter := wildcard
			switch tc.form {
			case "duplicate":
				filter = bind("and", wildcard, DeepCopyExpr(wildcard))
			case "or":
				key := GetColExpr(domain.TableDef.Cols[0].Typ, tag, 0)
				filter = bind("or", wildcard, bind("=", key, makePlan2Int32ConstExprWithType(42)))
			case "not":
				filter = bind("not", wildcard)
			case "not-istrue":
				filter = bind("not", bind("istrue", wildcard))
			case "double-not":
				filter = bind("not", bind("not", wildcard))
			case "istrue":
				filter = bind("istrue", wildcard)
			case "or-of-and":
				both := bind("and", wildcard, DeepCopyExpr(wildcard))
				key := GetColExpr(domain.TableDef.Cols[0].Typ, tag, 0)
				filter = bind("or", both, bind("=", key, makePlan2Int32ConstExprWithType(42)))
			}
			domain.FilterList = []*plan.Expr{filter}
			if tc.secondNullCount > 0 {
				colPos := int32(len(domain.TableDef.Cols))
				typ := domain.TableDef.Cols[1].Typ
				domain.TableDef.Cols = append(domain.TableDef.Cols, &plan.ColDef{Name: "other_name", Typ: typ})
				other := bind("like", GetColExpr(typ, tag, colPos), makePlan2StringConstExprWithType("%%"))
				domain.FilterList = append(domain.FilterList, other)
			}
			var independent []*plan.Expr
			if tc.upper > 0 {
				key := func() *plan.Expr { return GetColExpr(domain.TableDef.Cols[0].Typ, tag, 0) }
				independent = []*plan.Expr{
					bind(">=", key(), makePlan2Int32ConstExprWithType(tc.lower)),
					bind("<=", key(), makePlan2Int32ConstExprWithType(tc.upper)),
				}
				if tc.form == "and" {
					rangeFilter := bind("and", DeepCopyExpr(independent[0]), DeepCopyExpr(independent[1]))
					domain.FilterList[0] = bind("and", filter, rangeFilter)
				} else {
					domain.FilterList = append(domain.FilterList, DeepCopyExprList(independent)...)
				}
			}

			tableRows := tc.tableRows
			if tableRows == 0 {
				tableRows = 10000
			}
			stats := &statsinfo.StatsInfo{
				TableCnt: tableRows, BlockNumber: 2, TableName: "domain",
				NdvMap: map[string]float64{}, DataTypeMap: map[string]uint64{"part": uint64(types.T_int32)},
				MinValMap: map[string]float64{"part": 1}, MaxValMap: map[string]float64{"part": 5000},
			}
			if tc.keyNDV > 0 {
				stats.NdvMap["part"] = tc.keyNDV
			}
			if tc.noPrimaryKeyStats {
				domain.TableDef.Pkey = &plan.PrimaryKeyDef{PkeyColName: "part", Names: []string{"part"}}
				stats.MinValMap, stats.MaxValMap = nil, nil
			}
			if tc.nullCount > 0 {
				stats.NullCntMap = map[string]uint64{"name": tc.nullCount}
				stats.DataTypeMap["name"] = uint64(types.T_varchar)
				if tc.wrongStatsType {
					stats.DataTypeMap["name"] = uint64(types.T_char)
				}
			}
			if tc.secondNullCount > 0 {
				stats.NullCntMap["other_name"] = tc.secondNullCount
				stats.DataTypeMap["other_name"] = uint64(types.T_varchar)
			}
			if tc.snapshot {
				domain.ScanSnapshot = &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1}}
			}
			if tc.unpublished {
				stats.TableName = ""
			}
			b.compCtx.GetStatsCache().Set(domain.TableDef.TblId, stats)
			domain.Stats = calcScanStats(domain, b)
			require.Equal(t, stats.TableCnt, domain.Stats.TableCnt)
			require.True(t, membershipDomainScanTotal(domain), "each case must reach the cost gate")
			if tc.differentScanRows {
				domain.Stats.TableCnt *= 2
			}
			if tc.groups > 0 {
				agg.Stats.Outcnt = tc.groups
			}
			if tc.removeStats {
				b.compCtx.GetStatsCache().Delete(domain.TableDef.TblId)
			}

			before := proto.Clone(b.qry)
			wantWildcardRows := tc.wantWildcardRows
			if wantWildcardRows == 0 && !tc.useScanRows && !tc.removeStats && !tc.differentScanRows {
				wantWildcardRows = tableRows
			}
			var wantRows float64
			switch {
			case tc.useScanRows:
				wantRows = domain.Stats.Outcnt
			case tc.removeStats || tc.differentScanRows:
				wantRows = 0
			case len(independent) == 0:
				wantRows = wantWildcardRows
			default:
				control := DeepCopyNode(domain)
				control.FilterList = independent
				control.Stats = calcScanStats(control, b)
				if tc.noPrimaryKeyStats {
					require.Less(t, control.Stats.Outcnt, 5000.0, "the heuristic would price the domain below the fact key NDV")
				} else if tc.lower == 1 && tc.upper == 5000 {
					require.Equal(t, 1.0, control.Stats.Selectivity, "the range alone retains the whole table")
				}
				wantRows = min(wantWildcardRows, control.Stats.Outcnt)
			}
			rows, wildcardRows, reestimated := b.aggregateMembershipDomainRows(domain)
			require.InDelta(t, wantRows, rows, 1e-9)
			require.Equal(t, wantWildcardRows, wildcardRows)
			require.Equal(t, !tc.useScanRows && !tc.removeStats && !tc.differentScanRows, reestimated)
			require.Equal(t, tc.wantRestriction, b.restrictAggregateMembership(root))
			if !tc.wantRestriction {
				require.True(t, proto.Equal(before, b.qry), "rejection retains all original expressions and statistics")
				return
			}
			require.True(t, proto.Equal(before.(*plan.Query).Nodes[1], domain), "costing must not modify the original scan")
			input := b.qry.Nodes[agg.Children[0]]
			require.Equal(t, plan.Node_SEMI, input.JoinType)
			require.Len(t, b.qry.Nodes[input.Children[1]].FilterList, len(domain.FilterList), "the copy evaluates the original predicates")
		})
	}
}

func TestAggregateMembershipFactNDVRequiresPublishedRange(t *testing.T) {
	for _, tc := range []struct {
		name        string
		tableName   string
		tableRows   float64
		reestimated bool
		wantNDV     float64
	}{
		{name: "published", tableName: "fact", tableRows: 100000, reestimated: true, wantNDV: 20000},
		{name: "transient", tableRows: 100000, reestimated: true, wantNDV: 60144},
		{name: "inconsistent-row-count", tableName: "fact", tableRows: 100001, reestimated: true, wantNDV: 60144},
		{name: "original-scan-estimate", tableName: "fact", tableRows: 100000, wantNDV: 60144},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := aggregateMembershipTestBuilder(t)
			fact, agg := b.qry.Nodes[2], b.qry.Nodes[4]
			fact.Stats.TableCnt, fact.Stats.Outcnt = 100000, 100000
			b.compCtx.GetStatsCache().Set(fact.TableDef.TblId, &statsinfo.StatsInfo{
				TableName: tc.tableName, TableCnt: tc.tableRows,
				NdvMap:      map[string]float64{"part": 60144},
				MinValMap:   map[string]float64{"part": 1},
				MaxValMap:   map[string]float64{"part": 20000},
				DataTypeMap: map[string]uint64{"part": uint64(types.T_int32)},
			})
			require.Equal(t, tc.wantNDV, b.aggregateMembershipFactNDV(fact, agg.GroupBy[0], tc.reestimated))
		})
	}
}

func TestEstimateSingleIntegerRangeSelectivity(t *testing.T) {
	for _, tc := range []struct {
		name, op    string
		lo, hi      float64
		value       int64
		want        float64
		unavailable bool
	}{
		{name: "inclusive-at-min", op: ">=", lo: 1, hi: 20, value: 1, want: 1},
		{name: "inclusive-at-max", op: "<=", lo: 1, hi: 20, value: 20, want: 1},
		{name: "strict-before-min", op: ">", lo: 1, hi: 20, value: 0, want: 1},
		{name: "strict-after-max", op: "<", lo: 1, hi: 20, value: 21, want: 1},
		{name: "strict-at-min", op: ">", lo: 1, hi: 20, value: 1, want: 0.95},
		{name: "strict-at-max", op: "<", lo: 1, hi: 20, value: 20, want: 0.95},
		{name: "narrow-inclusive", op: "<=", lo: 1, hi: 20, value: 2, want: 0.1},
		{name: "singleton-strict", op: ">", lo: 1, hi: 1, value: 1, want: 1e-8},
		{name: "missing-bounds", op: ">=", lo: 0, hi: 0, unavailable: true},
		{name: "inexact-stats", op: "<", lo: 1 << 53, hi: 1<<53 + 2, value: 1<<53 + 1, unavailable: true},
		{name: "inexact-literal", op: "<", lo: 1, hi: 20, value: 1<<53 + 1, unavailable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sel, ok := estimateSingleIntegerRangeSelectivity(tc.op, tc.lo, tc.hi, types.T_int64, makePlan2Int64ConstExprWithType(tc.value).GetLit())
			require.Equal(t, !tc.unavailable, ok)
			require.InDelta(t, tc.want, sel, 1e-12)
		})
	}
}

func TestAggregateMembershipCostsOnlyPublishedIntegerBounds(t *testing.T) {
	for _, tc := range []struct {
		name, tableName string
		upperBounds     map[string]float64
		nullable        bool
		nullCounts      map[string]uint64
		want            float64
	}{
		{name: "published", tableName: "fact", upperBounds: map[string]float64{"part": 20}, want: 1},
		{name: "transient", upperBounds: map[string]float64{"part": 20}, want: 0.1},
		{name: "missing-upper-bound", tableName: "fact", want: 0.1},
		{name: "nullable", tableName: "fact", upperBounds: map[string]float64{"part": 20}, nullable: true,
			nullCounts: map[string]uint64{"part": 75000}, want: 0.25},
		{name: "nullable-missing-null-count", tableName: "fact", upperBounds: map[string]float64{"part": 20},
			nullable: true, want: 0.1},
		{name: "nullable-invalid-null-count", tableName: "fact", upperBounds: map[string]float64{"part": 20},
			nullable: true, nullCounts: map[string]uint64{"part": 100001}, want: 0.1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := aggregateMembershipTestBuilder(t)
			domain := b.qry.Nodes[1]
			keyType := domain.TableDef.Cols[0].Typ
			keyType.NotNullable = !tc.nullable
			key := GetColExpr(keyType, domain.BindingTags[0], 0)
			expr, err := BindFuncExprImplByPlanExpr(b.GetContext(), ">=", []*plan.Expr{key, makePlan2Int32ConstExprWithType(1)})
			require.NoError(t, err)
			b.compCtx.GetStatsCache().Set(domain.TableDef.TblId, &statsinfo.StatsInfo{
				TableName: tc.tableName, TableCnt: 100000,
				MinValMap:   map[string]float64{"part": 1},
				MaxValMap:   tc.upperBounds,
				NullCntMap:  tc.nullCounts,
				DataTypeMap: map[string]uint64{"part": uint64(types.T_int32)},
			})
			require.Equal(t, 0.1, estimateNonEqualitySelectivity(DeepCopyExpr(expr), ">=", b), "ordinary scan costing")
			expr.Selectivity = 0.1 // The ordinary scan has already priced this predicate.
			domain.FilterList[0].GetF().Args[1] = makePlan2StringConstExprWithType("%")
			domain.FilterList = append(domain.FilterList, expr)
			domain.Stats = &plan.Stats{TableCnt: 100000, Outcnt: 2000}
			before := proto.Clone(domain)
			rows, _, reestimated := b.aggregateMembershipDomainRows(domain)
			require.True(t, reestimated)
			require.InDelta(t, tc.want*domain.Stats.TableCnt, rows, 1e-9)
			require.True(t, proto.Equal(before, domain), "the scan and predicate caches keep their original values")
		})
	}
}

func TestAggregateMembershipRefreshesCachedBooleanRanges(t *testing.T) {
	b, _ := aggregateMembershipTestBuilder(t)
	domain := b.qry.Nodes[1]
	key := func() *plan.Expr { return GetColExpr(domain.TableDef.Cols[0].Typ, domain.BindingTags[0], 0) }
	bind := func(name string, selectivity float64, args ...*plan.Expr) *plan.Expr {
		expr, err := BindFuncExprImplByPlanExpr(b.GetContext(), name, args)
		require.NoError(t, err)
		expr.Selectivity = selectivity
		return expr
	}
	constant := makePlan2Int32ConstExprWithType
	leftRange := bind(">=", 0.1, key(), constant(1))
	equality := bind("=", 0.02, key(), constant(7))
	rightRange := bind("<=", 0.1, key(), constant(20))
	residual := bind("or", 0.15, bind("and", 0.01, leftRange, equality), rightRange)
	domain.FilterList[0].GetF().Args[1] = makePlan2StringConstExprWithType("%")
	domain.FilterList = append(domain.FilterList, residual)
	domain.Stats = &plan.Stats{TableCnt: 100000, Outcnt: 3000}
	stats := &statsinfo.StatsInfo{
		TableName: "domain", TableCnt: 100000,
		MinValMap:   map[string]float64{"part": 1},
		MaxValMap:   map[string]float64{"part": 20},
		DataTypeMap: map[string]uint64{"part": uint64(types.T_int32)},
	}
	b.compCtx.GetStatsCache().Set(domain.TableDef.TblId, stats)
	before := proto.Clone(domain)

	copy := DeepCopyExpr(residual)
	require.True(t, reestimateMembershipIntegerRanges(copy, domain, stats))
	left, right := copy.GetF().Args[0], copy.GetF().Args[1]
	require.Zero(t, copy.Selectivity)
	require.Zero(t, left.Selectivity)
	require.Equal(t, 1.0, left.GetF().Args[0].Selectivity)
	require.Equal(t, 0.02, left.GetF().Args[1].Selectivity)
	require.Equal(t, 1.0, right.Selectivity)

	rows, _, reestimated := b.aggregateMembershipDomainRows(domain)
	require.True(t, reestimated)
	require.Equal(t, domain.Stats.TableCnt, rows)
	require.True(t, proto.Equal(before, domain))
}

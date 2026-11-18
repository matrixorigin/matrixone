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

package plan

import (
	"encoding/binary"
	"slices"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
)

var defaultRules = []Rule{}
var prepareRules = []Rule{}

func init() {
	defaultRules = []Rule{
		rule.NewConstantFold(false),
		// rule.NewPredicatePushdown(),
	}
	prepareRules = []Rule{
		rule.NewConstantFold(true),
	}
}

func NewBaseOptimizer(ctx CompilerContext) *BaseOptimizer {
	return &BaseOptimizer{
		ctx:   ctx,
		rules: defaultRules,
	}
}

func NewPrepareOptimizer(ctx CompilerContext) *BaseOptimizer {
	return &BaseOptimizer{
		ctx:   ctx,
		rules: prepareRules,
	}
}

func (opt *BaseOptimizer) CurrentContext() CompilerContext {
	return opt.ctx
}

func (opt *BaseOptimizer) Optimize(stmt tree.Statement, isPrepareStmt bool) (*Query, error) {
	pn, err := BuildPlan(opt.ctx, stmt, isPrepareStmt)
	if err != nil {
		return nil, err
	}
	qry, ok := pn.Plan.(*plan.Plan_Query)
	if !ok {
		panic(moerr.NewInternalError(opt.ctx.GetContext(), pn.String()))
	}
	opt.qry = qry.Query
	return opt.optimize()
}

func (opt *BaseOptimizer) optimize() (*Query, error) {
	if len(opt.qry.Steps) == 0 {
		return opt.qry, nil
	}
	for _, step := range opt.qry.Steps {
		opt.exploreNode(opt.qry.Nodes[step])
	}

	opt.pruneUsedNodes(opt.qry)

	return opt.qry, nil
}

func (opt *BaseOptimizer) exploreNode(n *Node) {
	for i := range n.Children {
		opt.exploreNode(opt.qry.Nodes[n.Children[i]])
	}
	for _, rule := range opt.rules {
		if rule.Match(n) {
			rule.Apply(n, opt.qry, opt.ctx.GetProcess())
		}
	}
	// 域改写和 List/Vec 折叠完成后再去重，避免 reader 重复执行相同谓词。
	n.BlockFilterList = DeduplicateFinalBlockFilters(n.BlockFilterList)
}

// DeduplicateFinalBlockFilters 仅压缩本次新增准入的小 IN 重复项。
// 调用方若需保留缓存列表及 Fold 槽位，必须传入独立切片。
func DeduplicateFinalBlockFilters(filters []*plan.Expr) []*plan.Expr {
	if len(filters) < 2 {
		return filters
	}
	// optimizer与reader共用入口：先按直列分组，不解码/散列不可能重复的域。
	// 常见的一两个不同列候选无需堆分配；多过滤场景仍为线性预扫描。
	counts := make(map[[2]int32]int)
	possible := false
	for _, filter := range filters {
		if col := finalBlockFilterCandidateColumn(filter); col != nil {
			key := [2]int32{col.RelPos, col.ColPos}
			counts[key]++
			possible = possible || counts[key] > 1
		}
	}
	if !possible {
		return filters
	}
	buckets := make(map[uint64][]*plan.Expr, len(filters))
	unique := filters[:0]
	for _, filter := range filters {
		// 不把此次修复变成通用表达式去重，也不散列原有大静态向量。
		col := finalBlockFilterCandidateColumn(filter)
		if col == nil || counts[[2]int32{col.RelPos, col.ColPos}] < 2 ||
			!isSimpleZoneMapFilter(filter) || !zoneMapExprIsStable(filter) {
			unique = append(unique, filter)
			continue
		}
		identity := finalBlockFilterIdentity(filter)
		hash := exprStructuralHash(identity)
		duplicate := false
		for _, previous := range buckets[hash] {
			if exprStructuralEqual(previous, identity) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			buckets[hash] = append(buckets[hash], identity)
			unique = append(unique, filter)
		}
	}
	clear(filters[len(unique):])
	return unique
}

func finalBlockFilterCandidateColumn(expr *plan.Expr) *plan.ColRef {
	fn := expr.GetF()
	if fn.GetFunc().GetObjName() != "in" || len(fn.Args) != 2 || fn.Args[0] == nil {
		return nil
	}
	col := fn.Args[0].GetCol()
	if col == nil || !smallZoneMapInBounds(types.T(fn.Args[0].Typ.Id), fn.Args[1]) {
		return nil
	}
	return col
}

// 只在通过数量及物理字节预算后构造比较视图，绝不交给 reader。
// Vec 外层 Typ 可为 tuple；身份取实际 type、NULL 存在性与成员集合。
func finalBlockFilterIdentity(expr *plan.Expr) *plan.Expr {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Vec:
		return finalBlockFilterVectorIdentity(e)
	case *plan.Expr_F:
		copyExpr, fn := *expr, *e.F
		fn.Args = make([]*plan.Expr, len(e.F.Args))
		for i, arg := range e.F.Args {
			fn.Args[i] = finalBlockFilterIdentity(arg)
		}
		copyExpr.Expr = &plan.Expr_F{F: &fn}
		return &copyExpr
	case *plan.Expr_List:
		copyExpr := *expr
		items := make([]*plan.Expr, len(e.List.List))
		for i, item := range e.List.List {
			items[i] = finalBlockFilterIdentity(item)
		}
		copyExpr.Expr = &plan.Expr_List{List: &plan.ExprList{List: items}}
		return &copyExpr
	default:
		return expr
	}
}

// 可选去重不能因畸形编码改变执行行为；失败时保留原物理身份。
func finalBlockFilterVectorIdentity(e *plan.Expr_Vec) (identity *plan.Expr) {
	identity = &plan.Expr{Expr: e}
	defer func() {
		if recover() != nil {
			identity = &plan.Expr{Expr: e}
		}
	}()
	if e.Vec == nil || e.Vec.Len < 0 || e.Vec.Len > maxZoneMapInCandidates || len(e.Vec.Data) > maxZoneMapInPayloadBytes {
		return identity
	}
	var v vector.Vector
	data := e.Vec.Data[:len(e.Vec.Data):len(e.Vec.Data)]
	if err := v.UnmarshalBinary(data); err != nil || v.Length() != int(e.Vec.Len) ||
		!smallZoneMapInBounds(v.GetType().Oid, identity) {
		return identity
	}
	values := make([]string, v.Length())
	for i := range values {
		if v.IsNull(uint64(i)) {
			values[i] = "\x00"
		} else {
			values[i] = "\x01" + string(v.GetRawBytesAt(i))
		}
	}
	sort.Strings(values)
	values = slices.Compact(values)
	// 0xff不是向量class，私有集合编码不能与保留的原始payload混淆。
	data = append([]byte{0xff}, types.EncodeType(v.GetType())...)
	for _, value := range values {
		data = binary.LittleEndian.AppendUint32(data, uint32(len(value)))
		data = append(data, value...)
	}
	return &plan.Expr{Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: int32(len(values)), Data: data}}}
}

func (opt *BaseOptimizer) pruneUsedNodes(qry *plan.Query) {
	newSteps := make([]int32, 0, len(qry.Steps))
	newNodes := make([]*plan.Node, 0, len(qry.Nodes))

	for _, step := range qry.Steps {
		newSteps = append(newSteps, opt.compactPlanTree(qry, step, &newNodes))
	}

	qry.Steps = newSteps
	qry.Nodes = newNodes
}

func (opt *BaseOptimizer) compactPlanTree(qry *plan.Query, nodeID int32, nodes *[]*plan.Node) int32 {
	node := qry.Nodes[nodeID]

	for i, childID := range node.Children {
		node.Children[i] = opt.compactPlanTree(qry, childID, nodes)
	}

	node.NodeId = int32(len(*nodes))
	*nodes = append(*nodes, node)

	return node.NodeId
}

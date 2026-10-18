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
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// Select the general path before any destructive pull-up. A previously bound
// dependent child makes its enclosing relation dependent even when all of its
// own references are local. Ordinary one-level plans keep their existing path.
func (builder *QueryBuilder) subqueryNeedsDependent(root int32) bool {
	q := &plan.Query{Nodes: builder.qry.Nodes, Steps: builder.qry.Steps}
	// WalkQueryNodes walks the global directory; this selection is restricted
	// to the candidate relation and its source dependencies.
	pending := []int32{root}
	seen := make(map[int32]bool)
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if id < 0 || int(id) >= len(q.Nodes) || q.Nodes[id] == nil {
			return true // The binding validator will report the malformed input.
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		n := q.Nodes[id]
		if builder.dependentRelations[id] {
			return true
		}
		needed := false
		// An outer grouping key is constant only within its invocation. It
		// cannot be pulled above the grouping operation into the caller.
		for _, key := range n.GroupBy {
			_ = plan.VisitExprTree(key, func(e *plan.Expr) error { needed = needed || e.GetCorr() != nil; return nil })
		}
		_ = plan.VisitExpressionsInOwner(n, func(e *plan.Expr) error {
			return plan.VisitExprTree(e, func(e *plan.Expr) error {
				needed = needed || e.GetDependent() != nil || e.GetCorr() != nil && e.GetCorr().Depth > 1
				return nil
			})
		})
		if needed {
			return true
		}
		pending = append(pending, n.Children...)
		for _, step := range n.SourceStep {
			if step < 0 || int(step) >= len(q.Steps) {
				return true
			}
			pending = append(pending, q.Steps[step])
		}
	}
	return false
}

// Remap each region without moving expressions across demand stages. Ordinary
// set rewrites have already run only in regions selected by their own proof.
func (builder *QueryBuilder) createDependentQuery() (*Query, error) {
	if err := builder.finalizeCorrelationBindings(); err != nil {
		return nil, err
	}
	q := builder.qry
	// Normalize side ownership before physical column remapping.
	if err := plan.WalkQueryNodes(q, func(_ int32, n *plan.Node) error {
		if n.NodeType == plan.Node_JOIN && n.JoinType == plan.Node_RIGHT {
			n.JoinType = plan.Node_LEFT
			n.Children[0], n.Children[1] = n.Children[1], n.Children[0]
		}
		return nil
	}); err != nil {
		return nil, err
	}
	colRefBool := make(map[[2]int32]bool)
	sinkColRef := make(map[[2]int32]int)
	for i, root := range q.Steps {
		builder.rewriteDistinctToAGG(root)
		for j := range q.Nodes[root].ProjectList {
			colRefBool[[2]int32{int32(i), int32(j)}] = true
		}
	}
	for i, root := range q.Steps {
		builder.markSinkProject(root, int32(i), colRefBool)
	}
	for i := len(q.Steps) - 1; i >= 0; i-- {
		root := q.Nodes[q.Steps[i]]
		refs := make(map[[2]int32]int)
		if len(root.BindingTags) > 0 {
			for j := range root.ProjectList {
				refs[[2]int32{root.BindingTags[0], int32(j)}] = 1
			}
		}
		if _, err := builder.remapAllColRefs(root.NodeId, int32(i), refs, colRefBool, sinkColRef); err != nil {
			return nil, err
		}
	}
	builder.sinkColRef = sinkColRef
	for id := range builder.positionalSinkScans {
		n := q.Nodes[id]
		if n.NodeType != plan.Node_SINK_SCAN || len(n.SourceStep) == 0 {
			continue
		}
		for _, e := range n.ProjectList {
			if col := e.GetCol(); col != nil {
				if pos, ok := sinkColRef[[2]int32{n.SourceStep[0], col.ColPos}]; ok {
					col.ColPos = int32(pos)
				}
			}
		}
	}
	if err := builder.lockTableIfLockNoRowsAtTheEndForDelAndUpdate(); err != nil {
		return nil, err
	}
	if err := builder.deferDependentProjectionsAfterSort(); err != nil {
		return nil, err
	}
	if err := builder.assignCorrelationStages(); err != nil {
		return nil, err
	}
	// Remapping resolves aggregate/window output nullability and physical
	// widths. Freeze call and comparison contracts from those actual outputs.
	if _, err := refreshPreparedPlanProjectionTypes(builder.GetContext(),
		&Plan{Plan: &plan.Plan_Query{Query: q}}, nil, nil,
		NewResetParamRefRule(builder.GetContext(), nil)); err != nil {
		return nil, err
	}
	if err := q.ValidateCorrelation(); err != nil {
		return nil, moerr.NewInternalErrorf(builder.GetContext(), "invalid dependent plan: %v", err)
	}
	return q, nil
}

// Program roots and stage owners are node references, just like Children and
// Steps. Compact all of them under one map, preserving shared node identity.
func compactCorrelationPlan(q *plan.Query) error {
	ids := make(map[int32]int32)
	var nodes []*plan.Node
	if err := plan.WalkQueryNodes(q, func(id int32, n *plan.Node) error {
		ids[id] = int32(len(nodes))
		nodes = append(nodes, n)
		return nil
	}); err != nil {
		return err
	}
	for _, n := range nodes {
		n.NodeId = ids[n.NodeId]
		for i, child := range n.Children {
			n.Children[i] = ids[child]
		}
	}
	for i, root := range q.Steps {
		q.Steps[i] = ids[root]
	}
	for _, p := range q.Correlation.Programs {
		p.RootNode = ids[p.RootNode]
	}
	for _, s := range q.Correlation.Stages {
		s.OwnerNode = ids[s.OwnerNode]
	}
	q.Nodes = nodes
	return q.ValidateCorrelation()
}

// bindDependentSubquery closes one lexical boundary. A reference to the direct
// parent becomes a caller column; a reference beyond it remains correlated in
// the caller and is forwarded when that caller's boundary is closed. Binding
// tags identify semantic owners, so shadowed names cannot alias slots.
func (builder *QueryBuilder) bindDependentSubquery(sub *plan.SubqueryRef) (*plan.Expr, error) {
	if sub == nil || sub.NodeId < 0 || int(sub.NodeId) >= len(builder.qry.Nodes) {
		return nil, moerr.NewInternalError(builder.GetContext(), "invalid dependent subquery root")
	}
	ctx := builder.ctxByNode[sub.NodeId]
	if ctx == nil || len(ctx.results) == 0 {
		return nil, moerr.NewInternalError(builder.GetContext(), "dependent subquery has no result binding")
	}
	spec := builder.qry.Correlation
	if spec == nil {
		spec = &plan.CorrelationSpec{Version: plan.CorrelationVersion}
	}
	if uint64(len(spec.Programs)) >= uint64(^uint32(0)) {
		return nil, moerr.NewOOM(builder.GetContext())
	}
	programID := uint32(len(spec.Programs) + 1)
	program := &plan.CorrelationProgram{RootNode: sub.NodeId}
	for _, result := range ctx.results {
		program.OutputTypes = append(program.OutputTypes, *DeepCopyType(&result.Typ))
	}
	call := &plan.DependentExpr{ProgramId: programID, Reducer: sub.Typ}
	result := &plan.Expr{Expr: &plan.Expr_Dependent{Dependent: call}, Typ: plan.Type{Id: int32(types.T_bool)}}
	if sub.Typ == plan.SubqueryRef_SCALAR && sub.Child == nil {
		if len(program.OutputTypes) != 1 {
			return nil, moerr.NewInternalError(builder.GetContext(), "unconsumed row subquery")
		}
		result.Typ = *DeepCopyType(&program.OutputTypes[0])
		result.Typ.NotNullable = false
	}
	if sub.Typ == plan.SubqueryRef_EXISTS || sub.Typ == plan.SubqueryRef_NOT_EXISTS {
		result.Typ.NotNullable = true
	}
	if sub.Child != nil {
		if list := sub.Child.GetList(); list != nil {
			call.Lhs = DeepCopyExprList(list.List)
		} else {
			call.Lhs = []*plan.Expr{DeepCopyExpr(sub.Child)}
		}
		lhs := make([]*plan.Expr, len(call.Lhs))
		rhs := make([]*plan.Expr, len(program.OutputTypes))
		for i, e := range call.Lhs {
			lhs[i] = GetColExpr(e.Typ, 0, int32(i))
		}
		for i, typ := range program.OutputTypes {
			rhs[i] = GetColExpr(typ, 1, int32(i))
		}
		var left *plan.Expr
		if len(lhs) == 1 {
			left = lhs[0]
		} else {
			left = &plan.Expr{Typ: plan.Type{Id: int32(types.T_tuple)}, Expr: &plan.Expr_List{List: &plan.ExprList{List: lhs}}}
		}
		op := sub.Op
		if sub.Typ == plan.SubqueryRef_IN || sub.Typ == plan.SubqueryRef_NOT_IN {
			op = "="
		}
		var err error
		call.Comparison, err = builder.generateRowComparisonWithProjects(op, left, rhs, false)
		if err != nil {
			return nil, err
		}
	}
	// Prepare substitutions without changing the source graph. An error cannot
	// leave half the nodes parameterized under an unpublished program id.
	type replacement struct {
		target  *plan.Expr
		ordinal uint32
	}
	var replacements []replacement
	slots := make(map[[3]int32]uint32)
	pending := []int32{sub.NodeId}
	visited := make(map[int32]bool)
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if id < 0 || int(id) >= len(builder.qry.Nodes) || builder.qry.Nodes[id] == nil {
			return nil, moerr.NewInternalError(builder.GetContext(), "invalid dependent relation")
		}
		if visited[id] {
			continue
		}
		visited[id] = true
		node := builder.qry.Nodes[id]
		pending = append(pending, node.Children...)
		for _, step := range node.SourceStep {
			if step < 0 || int(step) >= len(builder.qry.Steps) {
				return nil, moerr.NewInternalError(builder.GetContext(), "invalid dependent source step")
			}
			pending = append(pending, builder.qry.Steps[step])
		}
		if err := plan.VisitExpressionsInOwner(node, func(expr *plan.Expr) error {
			return plan.VisitExprTree(expr, func(e *plan.Expr) error {
				corr := e.GetCorr()
				if corr == nil {
					return nil
				}
				if corr.Depth < 1 {
					return moerr.NewInternalError(builder.GetContext(), "invalid correlation depth")
				}
				depth := dependentCaptureDepth(ctx, corr)
				key := [3]int32{corr.RelPos, corr.ColPos, depth}
				ordinal, ok := slots[key]
				if !ok {
					if uint64(len(call.Inputs)) >= uint64(^uint32(0)) {
						return moerr.NewOOM(builder.GetContext())
					}
					ordinal = uint32(len(call.Inputs))
					slots[key] = ordinal
					input := DeepCopyExpr(e)
					if depth == 1 {
						input.Expr = &plan.Expr_Col{Col: &plan.ColRef{RelPos: corr.RelPos, ColPos: corr.ColPos}}
					} else {
						input.GetCorr().Depth = depth - 1
					}
					call.Inputs = append(call.Inputs, input)
					program.InputTypes = append(program.InputTypes, *DeepCopyType(&e.Typ))
				} else if !proto.Equal(&e.Typ, &program.InputTypes[ordinal]) {
					return moerr.NewInternalError(builder.GetContext(), "inconsistent correlation slot type")
				}
				replacements = append(replacements, replacement{e, ordinal})
				return nil
			})
		}); err != nil {
			return nil, err
		}
	}
	for _, r := range replacements {
		r.target.Expr = &plan.Expr_Slot{Slot: &plan.CorrelationSlot{ProgramId: programID, Ordinal: r.ordinal}}
	}
	spec.Programs = append(spec.Programs, program)
	builder.qry.Correlation = spec
	return result, nil
}

// A CTE body resolves names at its declaration, whereas its containing program
// closes the boundary at the use site. Recover the capture distance from the
// unique binding tag in the program's ancestors rather than forwarding the
// declaration-relative depth. Structural contexts without binders do not add a
// lexical boundary, matching baseBindColRef. The original depth remains useful
// for transformed plans whose binding contexts are no longer retained.
func dependentCaptureDepth(ctx *BindContext, corr *plan.CorrColRef) int32 {
	depth := int32(0)
	for ancestor := ctx.parent; ancestor != nil; ancestor = ancestor.parent {
		if ancestor.binder == nil {
			continue
		}
		depth++
		if ancestor.bindingByTag[corr.RelPos] != nil ||
			(corr.RelPos > 0 && (corr.RelPos == ancestor.groupTag || corr.RelPos == ancestor.aggregateTag ||
				corr.RelPos == ancestor.projectTag || corr.RelPos == ancestor.resultTag)) {
			return depth
		}
	}
	return corr.Depth
}

// finalizeCorrelationBindings freezes step ownership after all query blocks
// have been bound. Program output steps are explicit even when the binder did
// not need a SINK for them. The main output remains the pre-append output step.
func (builder *QueryBuilder) finalizeCorrelationBindings() error {
	q := builder.qry
	spec := q.Correlation
	if spec == nil {
		return nil
	}
	spec.MainOutputStep = int32(len(q.Steps) - 1)
	stepByRoot := make(map[int32]int)
	for i, root := range q.Steps {
		stepByRoot[root] = i
	}
	for _, p := range spec.Programs {
		if _, ok := stepByRoot[p.RootNode]; !ok {
			stepByRoot[p.RootNode] = len(q.Steps)
			q.Steps = append(q.Steps, p.RootNode)
		}
	}
	return builder.refreshCorrelationBindings()
}

// Late DML graph rewrites keep the main output identity but can add or remove
// materialized steps and expression owners. Derive their directory from the
// final graph rather than extending a stale step-owner slice.
func (builder *QueryBuilder) refreshCorrelationBindings() error {
	q := builder.qry
	spec := q.Correlation
	if spec == nil {
		return nil
	}
	stepByRoot := make(map[int32]int, len(q.Steps))
	for i, root := range q.Steps {
		stepByRoot[root] = i
	}
	spec.StepOwners = make([]uint32, len(q.Steps))
	nodeOwner := make(map[int32]uint32)
	for i, p := range spec.Programs {
		owner := uint32(i + 1)
		pending := []int32{p.RootNode}
		for len(pending) > 0 {
			id := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			if previous, ok := nodeOwner[id]; ok {
				if previous != owner {
					return moerr.NewInternalError(builder.GetContext(), "dependent programs share mutable relation")
				}
				continue
			}
			if id < 0 || int(id) >= len(q.Nodes) || q.Nodes[id] == nil {
				return moerr.NewInternalError(builder.GetContext(), "invalid dependent node")
			}
			nodeOwner[id] = owner
			if step, ok := stepByRoot[id]; ok {
				spec.StepOwners[step] = owner
			}
			pending = append(pending, q.Nodes[id].Children...)
			for _, step := range q.Nodes[id].SourceStep {
				if step < 0 || int(step) >= len(q.Steps) {
					return moerr.NewInternalError(builder.GetContext(), "invalid dependent step")
				}
				pending = append(pending, q.Steps[step])
			}
		}
	}
	return builder.assignCorrelationStages()
}

// A legacy DML source was remapped before its writer nodes were built. Keep its
// directory slot until the final writer can replace it, so appended program
// steps and every SourceStep retain their indexes.
func (builder *QueryBuilder) replaceLegacyDMLSourceStep(sourceStep int32) {
	q := builder.qry
	if q.Correlation == nil {
		return
	}
	last := len(q.Steps) - 1
	q.Steps[sourceStep] = q.Steps[last]
	q.Steps = q.Steps[:last]
	q.Correlation.MainOutputStep = sourceStep
}

func (builder *QueryBuilder) assignCorrelationStages() error {
	q := builder.qry
	spec := q.Correlation
	spec.Stages = nil
	occurrence := uint32(0)
	type stageBinding struct {
		id      uint32
		kind    plan.CorrelationStage_Kind
		image   uint32
		aliases map[*plan.DependentExpr]uint32
	}
	return plan.WalkQueryNodes(q, func(id int32, node *plan.Node) error {
		// Stage identity belongs to an expression's evaluation owner, not to
		// binder aliases shared by nodes (for example PARTITION and WINDOW).
		// Detach before marking roles so mutations cannot overwrite another owner.
		hasDependent := false
		if err := plan.VisitExpressionsInOwner(node, func(e *plan.Expr) error {
			return plan.VisitExprTree(e, func(e *plan.Expr) error {
				hasDependent = hasDependent || e.GetDependent() != nil
				return nil
			})
		}); err != nil {
			return err
		}
		if hasDependent {
			*node = *DeepCopyNode(node)
		}
		ordinary := &stageBinding{kind: plan.CorrelationStage_INPUT}
		roles := make(map[*plan.DependentExpr]*stageBinding)
		mark := func(expr *plan.Expr, stage *stageBinding) {
			_ = plan.VisitExprTree(expr, func(e *plan.Expr) error {
				if d := e.GetDependent(); d != nil {
					roles[d] = stage
				}
				return nil
			})
		}
		markList := func(exprs []*plan.Expr, stage *stageBinding) {
			for _, e := range exprs {
				mark(e, stage)
			}
		}
		if node.NodeType == plan.Node_JOIN {
			markList(node.OnList, &stageBinding{kind: plan.CorrelationStage_JOIN_PAIR})
		}
		if node.NodeType == plan.Node_AGG {
			group := &stageBinding{kind: plan.CorrelationStage_GROUP}
			markList(node.ProjectList, group)
			markList(node.FilterList, group)
		}
		setup := &stageBinding{kind: plan.CorrelationStage_WINDOW_SETUP}
		if node.NodeType == plan.Node_WINDOW || node.NodeType == plan.Node_TIME_WINDOW || node.NodeType == plan.Node_FILL {
			row := &stageBinding{kind: plan.CorrelationStage_WINDOW_ROW}
			markList(node.ProjectList, row)
			markList(node.FilterList, row)
			for _, expr := range node.WinSpecList {
				if w := expr.GetW(); w != nil && w.Frame != nil {
					if w.Frame.Start != nil {
						mark(w.Frame.Start.Val, setup)
					}
					if w.Frame.End != nil {
						mark(w.Frame.End.Val, setup)
					}
				}
			}
			mark(node.GapFillStart, setup)
			mark(node.GapFillEnd, setup)
		}
		if node.NodeType == plan.Node_FILL && node.FillType == plan.Node_VALUE {
			markList(node.FillVal, setup)
		}
		if builder.correlationRouteNodes[id] {
			markList(node.ProjectList, &stageBinding{kind: plan.CorrelationStage_ROUTE})
		}
		if ctx := node.DedupJoinCtx; ctx != nil {
			for i, expr := range ctx.UpdateColExprList {
				mark(expr, &stageBinding{kind: plan.CorrelationStage_ASSIGNMENT, image: uint32(i + 1)})
			}
		}
		return plan.VisitExpressionsInOwner(node, func(e *plan.Expr) error {
			return plan.VisitExprTree(e, func(e *plan.Expr) error {
				d := e.GetDependent()
				if d == nil {
					return nil
				}
				stage := roles[d]
				if stage == nil {
					stage = ordinary
				}
				if stage.id == 0 {
					spec.Stages = append(spec.Stages, &plan.CorrelationStage{OwnerNode: id, Kind: stage.kind, ImageVersion: stage.image})
					stage.id = uint32(len(spec.Stages))
					stage.aliases = make(map[*plan.DependentExpr]uint32)
				}
				if prior, ok := stage.aliases[d]; ok {
					d.OccurrenceId, d.StageId = prior, stage.id
					return nil
				}
				if occurrence == ^uint32(0) {
					return moerr.NewOOM(builder.GetContext())
				}
				occurrence++
				d.OccurrenceId, d.StageId = occurrence, stage.id
				stage.aliases[d] = occurrence
				return nil
			})
		})
	})
}

// A non-lateral derived relation may capture a genuine ancestor. Its relational
// shape does not decide lexical visibility; ownership does. The immediate FROM
// siblings remain unavailable, and removing this transparent query boundary
// forwards exactly one lexical level.
func (builder *QueryBuilder) normalizeDependentDerivedTable(root int32, ctx *BindContext) error {
	refs := make(map[*plan.CorrColRef]struct{})
	pending := []int32{root}
	seen := make(map[int32]bool)
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if seen[id] {
			continue
		}
		seen[id] = true
		n := builder.qry.Nodes[id]
		if err := plan.VisitExpressionsInOwner(n, func(e *plan.Expr) error {
			return plan.VisitExprTree(e, func(e *plan.Expr) error {
				corr := e.GetCorr()
				if corr == nil {
					return nil
				}
				if _, same := ctx.bindingByTag[corr.RelPos]; same {
					return moerr.NewNYI(builder.GetContext(), "correlated subquery in FROM clause")
				}
				found := false
				for ancestor := ctx.parent; ancestor != nil; ancestor = ancestor.parent {
					if _, ok := ancestor.bindingByTag[corr.RelPos]; ok {
						found = true
						break
					}
				}
				if !found || corr.Depth < 1 {
					return moerr.NewNYI(builder.GetContext(), "correlated subquery in FROM clause")
				}
				refs[corr] = struct{}{}
				return nil
			})
		}); err != nil {
			return err
		}
		pending = append(pending, n.Children...)
		for _, step := range n.SourceStep {
			pending = append(pending, builder.qry.Steps[step])
		}
	}
	if len(refs) == 0 {
		return moerr.NewInternalError(builder.GetContext(), "derived correlation has no capture")
	}
	for corr := range refs {
		if ctx.binder != nil && corr.Depth > 1 {
			corr.Depth--
		}
	}
	ctx.isCorrelated = true
	if builder.dependentRelations == nil {
		builder.dependentRelations = make(map[int32]bool)
	}
	builder.dependentRelations[root] = true
	return nil
}

func (builder *QueryBuilder) bindDependentJoinCondition(e *plan.Expr) (*plan.Expr, error) {
	if e == nil {
		return nil, nil
	}
	if sub := e.GetSub(); sub != nil {
		if sub.Child != nil {
			var err error
			sub.Child, err = builder.bindDependentJoinCondition(sub.Child)
			if err != nil {
				return nil, err
			}
		}
		return builder.bindDependentSubquery(sub)
	}
	if f := e.GetF(); f != nil {
		for i, arg := range f.Args {
			next, err := builder.bindDependentJoinCondition(arg)
			if err != nil {
				return nil, err
			}
			f.Args[i] = next
		}
	}
	if list := e.GetList(); list != nil {
		for i, arg := range list.List {
			next, err := builder.bindDependentJoinCondition(arg)
			if err != nil {
				return nil, err
			}
			list.List[i] = next
		}
	}
	return e, nil
}

// Prove the legacy null-rejected scalar aggregate before its pull-up mutates
// any predicate. General parameter execution is reserved for the complement.
func (builder *QueryBuilder) legacyDeepScalarAggregateSafe(sub *plan.SubqueryRef, nullRejected bool) bool {
	ctx := builder.ctxByNode[sub.NodeId]
	if sub.Typ != plan.SubqueryRef_SCALAR || !nullRejected || ctx == nil ||
		!ctx.hasSingleRow || len(ctx.groups) != 0 || len(ctx.aggregates) == 0 ||
		!scalarAggregateResultReturnsNullOnEmpty(ctx) ||
		!builder.scalarAggregatePlanSupportsDeepCorrelation(sub.NodeId, ctx.aggregateTag) {
		return false
	}
	var predicates []*plan.Expr
	aggregates := 0
	id := sub.NodeId
	for range builder.qry.Nodes {
		if id < 0 || int(id) >= len(builder.qry.Nodes) || builder.dependentRelations[id] {
			return false
		}
		n := builder.qry.Nodes[id]
		if n == nil || n.Limit != nil || n.Offset != nil || n.RankOption != nil || len(n.SourceStep) != 0 {
			return false
		}
		allowed := make(map[*plan.CorrColRef]bool)
		if n.NodeType == plan.Node_FILTER {
			for _, predicate := range n.FilterList {
				if !hasCorrCol(predicate) {
					continue
				}
				inner := exprHasColRef(predicate) || containsVolatileFunction(predicate)
				valid := true
				_ = plan.VisitExprTree(predicate, func(e *plan.Expr) error {
					if corr := e.GetCorr(); corr != nil {
						valid = valid && corr.Depth > 0 && (corr.Depth <= 2 || !inner)
						allowed[corr] = true
					}
					return nil
				})
				if !valid {
					return false
				}
				copy := DeepCopyExpr(predicate)
				_ = plan.VisitExprTree(copy, func(e *plan.Expr) error {
					if corr := e.GetCorr(); corr != nil {
						corr.Depth = 1
					}
					return nil
				})
				predicates = append(predicates, copy)
			}
		}
		valid := true
		_ = plan.VisitExpressionsInOwner(n, func(root *plan.Expr) error {
			return plan.VisitExprTree(root, func(e *plan.Expr) error {
				valid = valid && e.GetSub() == nil && e.GetDependent() == nil && (e.GetCorr() == nil || allowed[e.GetCorr()])
				return nil
			})
		})
		if !valid {
			return false
		}
		switch n.NodeType {
		case plan.Node_AGG:
			aggregates++
			if aggregates > 1 || len(n.GroupBy) != 0 {
				return false
			}
		case plan.Node_PROJECT, plan.Node_FILTER:
		case plan.Node_TABLE_SCAN, plan.Node_VALUE_SCAN:
			_, equality := correlatedPaginationPartitionKeys(splitPlanConjunctions(predicates))
			return aggregates == 1 && equality
		default:
			return false
		}
		if len(n.Children) != 1 {
			return false
		}
		id = n.Children[0]
	}
	return false
}

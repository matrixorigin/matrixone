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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// cteEvaluation separates source publication from values that cannot be replayed.
type cteEvaluation struct {
	needsPublication bool
	valueUnstable    bool
	// Binding rewrites may still change these dependencies.
	mutableDependency bool
}

func (e *cteEvaluation) merge(other cteEvaluation) {
	e.needsPublication = e.needsPublication || other.needsPublication
	e.valueUnstable = e.valueUnstable || other.valueUnstable
	e.mutableDependency = e.mutableDependency || other.mutableDependency
}

// boundCTE is a statement-local publication descriptor, never a cache of rows.
// Runtime generations and budgets remain owned by Compile or dependent.region.
type boundCTE struct {
	ref                        *CTERef
	evaluation                 cteEvaluation
	hasSingleRow               bool
	sourceStep                 int32
	output                     cteOccurrence
	provenance                 []OutputColumnProvenance
	orderTypes, canonicalTypes []*planpb.Type
	semanticKeys               []string
	database                   string
	snapshot                   *Snapshot
	views                      []string
}

func (builder *QueryBuilder) appendCTEProducer(root int32, ctx *BindContext, tag int32) int32 {
	sink := appendSinkNodeWithTag(builder, ctx, root, tag)
	builder.qry.Nodes[sink].ExtraOptions = materialized.CTESinkOption
	if builder.cteSinkNodes == nil {
		builder.cteSinkNodes = make(map[int32]struct{})
	}
	builder.cteSinkNodes[sink] = struct{}{}
	return builder.appendStep(sink)
}

func (builder *QueryBuilder) publishBoundCTE(useCtx *BindContext, ref *CTERef, root int32, ctx *BindContext, recursive bool, evaluation cteEvaluation) *boundCTE {
	node := builder.qry.Nodes[root]
	headings := append([]string(nil), ctx.headings...)
	if recursive {
		for i, name := range ref.ast.Name.Cols {
			headings[i] = string(name)
		}
	}
	tag := ctx.rootTag()
	output := cteOccurrence{
		headings: headings, headingProvenance: cloneHeadingProvenances(ctx.headingProvenance),
		types: make([]planpb.Type, len(node.ProjectList)), isCorrelated: ctx.isCorrelated}
	// Recursive outward scans already publish the final, common column contract.
	if recursive {
		tag = node.BindingTags[0]
	}
	for i, expr := range node.ProjectList {
		output.types[i] = expr.Typ
	}
	bound := &boundCTE{ref: ref, evaluation: evaluation, output: output, database: ctx.cteDatabase, hasSingleRow: !recursive && ctx.hasSingleRow,
		provenance: make([]OutputColumnProvenance, len(headings)),
		orderTypes: make([]*planpb.Type, len(headings)), canonicalTypes: make([]*planpb.Type, len(headings)),
		semanticKeys: append([]string(nil), ctx.projectSemanticKeys...), snapshot: useCtx.snapshot}
	if ref.snapshot != nil {
		bound.snapshot = ref.snapshot
	}
	for i := range headings {
		bound.provenance[i] = ctx.outputColumnProvenanceForProject(int32(i))
		bound.orderTypes[i] = ctx.mysqlSpecialOrderTypeForProject(int32(i))
		bound.canonicalTypes[i] = ctx.mysqlSpecialCanonicalTypeForProject(int32(i))
	}
	bound.sourceStep = builder.appendCTEProducer(root, ctx, tag)
	if builder.boundCTEs == nil {
		builder.boundCTEs = make(map[*CTERef]map[*BindContext]*boundCTE)
	}
	if builder.semanticCTESinks == nil {
		builder.semanticCTESinks = make(map[int32]*boundCTE)
	}
	publications := builder.boundCTEs[ref]
	if publications == nil {
		publications = make(map[*BindContext]*boundCTE)
		builder.boundCTEs[ref] = publications
	}
	// Stable graphs may be rebound in another dependent program; their mutable
	// nodes and substituted correlation slots belong to that query block.
	// Unstable values retain declaration-wide identity (the nil owner).
	owner := useCtx.queryBlockOwner
	if evaluation.valueUnstable {
		owner = nil
	}
	publications[owner] = bound
	builder.semanticCTESinks[builder.qry.Steps[bound.sourceStep]] = bound
	return bound
}

func (builder *QueryBuilder) bindCTEReader(useCtx *BindContext, ref *CTERef, bound *boundCTE) (int32, error) {
	if builder.isForUpdate {
		return 0, moerr.NewNotSupported(builder.GetContext(), "SELECT ... FOR UPDATE on a materialized CTE")
	}
	snapshot := useCtx.snapshot
	if ref.snapshot != nil {
		snapshot = ref.snapshot
	}
	if !proto.Equal(snapshot, bound.snapshot) {
		return 0, moerr.NewNYI(builder.GetContext(), "shared CTE references require the same snapshot")
	}
	ctx := NewBindContext(builder, ref.declarationCtx)
	ctx.cteName, ctx.cteDatabase = string(ref.ast.Name.Alias), bound.database
	ctx.snapshot = snapshot
	ctx.isCorrelated = bound.output.isCorrelated
	ctx.projectTag = builder.genNewBindTag()
	ctx.headings = append([]string(nil), bound.output.headings...)
	ctx.headingProvenance = cloneHeadingProvenances(bound.output.headingProvenance)
	ctx.projectSemanticKeys = append([]string(nil), bound.semanticKeys...)
	occurrence := bound.output
	occurrence.ctx, occurrence.rootTag = ctx, builder.genNewBindTag()
	// Transport includes private native identities; only headings are SQL-visible.
	occurrence.headings = append([]string(nil), occurrence.headings...)
	for len(occurrence.headings) < len(occurrence.types) {
		occurrence.headings = append(occurrence.headings, "")
	}
	scan := builder.appendSharedCTEScan(ref, occurrence, bound.sourceStep, false)
	projects := make([]*planpb.Expr, len(ctx.headings))
	for i := range projects {
		projects[i] = GetColExpr(occurrence.types[i], occurrence.rootTag, int32(i))
		ctx.setMySQLSpecialOrderType(int32(i), DeepCopyType(bound.orderTypes[i]))
		ctx.setMySQLSpecialCanonicalType(int32(i), bound.canonicalTypes[i])
		if ctx.outputColumnProvenance == nil {
			ctx.outputColumnProvenance = make(map[int32]OutputColumnProvenance)
		}
		ctx.outputColumnProvenance[int32(i)] = bound.provenance[i]
	}
	ctx.projects, ctx.results = projects, projects
	root := builder.appendNode(&planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{scan},
		BindingTags: []int32{ctx.projectTag}, ProjectList: projects}, ctx)
	if builder.localCTERoots == nil {
		builder.localCTERoots = make(map[int32]bool)
	}
	builder.localCTERoots[root] = true
	if bound.hasSingleRow {
		useCtx.hasSingleRow = true
	}
	useCtx.recordViews(bound.views)
	return root, nil
}

// cteIdentityProducer resolves only the visible boundary created by bindCTEReader.
func (builder *QueryBuilder) cteIdentityProducer(root int32) int32 {
	node := builder.qry.Nodes[root]
	if node.NodeType != planpb.Node_PROJECT || len(node.Children) != 1 {
		return root
	}
	scan := builder.qry.Nodes[node.Children[0]]
	if scan.NodeType != planpb.Node_SINK_SCAN || len(scan.SourceStep) != 1 {
		return root
	}
	sinkID := builder.qry.Steps[scan.SourceStep[0]]
	if builder.semanticCTESinks[sinkID] == nil {
		return root
	}
	return builder.qry.Nodes[sinkID].Children[0]
}

// Walk executable dependencies once. Recursion and existing publication require
// a source boundary, but only unstable values forbid cross-owner recomputation.
func (builder *QueryBuilder) classifyCTEEvaluation(id int32, seen map[int32]bool) cteEvaluation {
	if builder.cteRecursiveBindingDepth == 0 && builder.stableCTERoots[id] {
		return cteEvaluation{}
	}
	if seen[id] {
		return cteEvaluation{}
	}
	seen[id] = true
	if bound := builder.semanticCTESinks[id]; bound != nil {
		return bound.evaluation
	}
	node := builder.qry.Nodes[id]
	var evaluation cteEvaluation
	evaluation.mutableDependency = len(node.SourceStep) > 0
	switch node.NodeType {
	case planpb.Node_RECURSIVE_CTE:
		evaluation.needsPublication = true
		evaluation.mutableDependency = true
	case planpb.Node_SAMPLE, planpb.Node_EXTERNAL_FUNCTION:
		evaluation.valueUnstable = true
	case planpb.Node_FUNCTION_SCAN:
		evaluation.valueUnstable = !tableFunctionHasStableIntrinsicValues(node)
	}
	// Catalog defaults/generated expressions are metadata, not scan evaluations.
	executable := *node
	executable.TableDef = nil
	_ = planpb.VisitExpressionsInOwner(&executable, func(expr *planpb.Expr) error {
		return planpb.VisitExprTree(expr, func(expr *planpb.Expr) error {
			if fn := expr.GetF(); fn != nil {
				unstable, known := function.GetFunctionValueUnstableByID(fn.Func.Obj)
				evaluation.valueUnstable = evaluation.valueUnstable || !known || unstable
			}
			if expr.GetCorr() != nil || expr.GetSlot() != nil {
				evaluation.mutableDependency = true
			}
			if sub := expr.GetSub(); sub != nil {
				evaluation.mutableDependency = true
				evaluation.merge(builder.classifyCTEEvaluation(sub.NodeId, seen))
			}
			if call := expr.GetDependent(); call != nil && builder.qry.Correlation != nil {
				evaluation.mutableDependency = true
				evaluation.merge(builder.classifyCTEEvaluation(builder.qry.Correlation.Programs[call.ProgramId-1].RootNode, seen))
			}
			return nil
		})
	})
	for _, child := range node.Children {
		evaluation.merge(builder.classifyCTEEvaluation(child, seen))
	}
	for _, step := range node.SourceStep {
		evaluation.merge(builder.classifyCTEEvaluation(builder.qry.Steps[step], seen))
	}
	evaluation.needsPublication = evaluation.needsPublication || evaluation.valueUnstable
	return evaluation
}

// A dependent invocation owns its own sources. It cannot capture a statement's
// unstable materialized value or carry that value through fresh recursive rounds.
// Validate before dependent binding substitutes any correlated expression.
func (builder *QueryBuilder) validateCTEInvocation(root int32) error {
	if len(builder.semanticCTESinks) == 0 {
		return nil
	}
	owner := builder.ctxByNode[root].queryBlockOwner
	type visitKey struct {
		node  int32
		round *CTERef
	}
	seen := make(map[visitKey]bool)
	var visit func(int32, *CTERef) error
	visit = func(id int32, round *CTERef) error {
		key := visitKey{id, round}
		if seen[key] {
			return nil
		}
		seen[key] = true
		node := builder.qry.Nodes[id]
		if bound := builder.semanticCTESinks[id]; bound != nil && bound.evaluation.valueUnstable {
			ref := bound.ref
			local := false
			for ctx := ref.declarationCtx; ctx != nil; ctx = ctx.parent {
				if ctx.queryBlockOwner == owner {
					local = true
					break
				}
			}
			if !local {
				return moerr.NewNYI(builder.GetContext(), "shared CTE capture across dependent invocations")
			}
			if round != nil {
				var declaredRound *CTERef
				for ctx := ref.declarationCtx; ctx != nil; ctx = ctx.parent {
					if ctx.cteState.cteBindType == CteBindTypeRecurStmt {
						declaredRound = ctx.cteState.cte
						break
					}
				}
				if declaredRound != round {
					return moerr.NewNYI(builder.GetContext(), "shared CTE capture across recursive rounds")
				}
			}
		}
		for _, child := range node.Children {
			if err := visit(child, round); err != nil {
				return err
			}
		}
		if node.NodeType == planpb.Node_RECURSIVE_SCAN {
			return nil
		}
		for i, step := range node.SourceStep {
			sourceRound := round
			if node.NodeType == planpb.Node_RECURSIVE_CTE && i > 0 {
				sourceRound = builder.recursiveCTEOwners[id]
			}
			if err := visit(builder.qry.Steps[step], sourceRound); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root, nil)
}

// finalizeCTEIdentity is shared by SELECT and direct DML/DDL builders. SELECT
// finalizes before its optional sharing rewrites; createQuery covers the other
// entry points once all statement readers and step owners have been bound.
func (builder *QueryBuilder) finalizeCTEIdentity(extraRoots ...int32) {
	if builder.cteIdentityFinalized {
		return
	}
	builder.cteIdentityFinalized = true
	builder.stableCTERoots = nil
	if len(builder.semanticCTESinks) == 0 || builder.qry.Correlation != nil {
		return
	}
	roots := append([]int32(nil), builder.qry.Steps...)
	roots = append(roots, extraRoots...)
	builder.inlineSingleReaderCTEs(roots...)
}

// inlineSingleReaderCTEs removes a physical publication boundary only after the
// complete statement has bound. Semantic identity and capture checks have already
// run. Recursive/dependent regions retain their existing publication owners.
func (builder *QueryBuilder) inlineSingleReaderCTEs(roots ...int32) {
	if len(builder.semanticCTESinks) == 0 || builder.qry.Correlation != nil {
		return
	}
	seen := make(map[int32]bool)
	parents := make(map[int32]int)
	readers := make(map[int32]int32)
	safe := true
	var visit func(int32)
	visit = func(id int32) {
		if seen[id] || !safe {
			return
		}
		seen[id] = true
		node := builder.qry.Nodes[id]
		if node.NodeType == planpb.Node_RECURSIVE_CTE || node.NodeType == planpb.Node_RECURSIVE_SCAN || node.RecursiveSink || node.RecursiveCte {
			safe = false
			return
		}
		executable := *node
		executable.TableDef = nil
		_ = planpb.VisitExpressionsInOwner(&executable, func(expr *planpb.Expr) error {
			return planpb.VisitExprTree(expr, func(expr *planpb.Expr) error {
				if expr.GetSub() != nil || expr.GetDependent() != nil || expr.GetCorr() != nil || expr.GetSlot() != nil {
					safe = false
				}
				return nil
			})
		})
		for _, child := range node.Children {
			parents[child]++
			if parents[child] > 1 {
				safe = false // A shared subtree can execute its scan more than once.
				return
			}
			visit(child)
		}
		for _, step := range node.SourceStep {
			if node.NodeType == planpb.Node_SINK_SCAN && len(node.SourceStep) == 1 {
				readers[step] = id
			}
			visit(builder.qry.Steps[step])
		}
	}
	for _, root := range roots {
		visit(root)
	}
	if !safe {
		return
	}
	// Count all references, including abandoned nodes: an unaccounted reference
	// conservatively retains publication rather than leaving a stale step index.
	references := make(map[int32]int)
	for _, node := range builder.qry.Nodes {
		for _, step := range node.SourceStep {
			references[step]++
		}
	}
	removed := make(map[int32]bool)
	for step, reader := range readers {
		sinkID := builder.qry.Steps[step]
		bound := builder.semanticCTESinks[sinkID]
		if bound == nil || references[step] != 1 || bound.ref.isRecursive {
			continue
		}
		sink, scan := builder.qry.Nodes[sinkID], builder.qry.Nodes[reader]
		if len(sink.Children) != 1 || len(scan.BindingTags) != 1 || len(scan.ProjectList) != len(sink.ProjectList) ||
			len(scan.FilterList) != 0 || len(scan.OrderBy) != 0 {
			continue
		}
		matching := true
		for i := range scan.ProjectList {
			matching = matching && proto.Equal(&scan.ProjectList[i].Typ, &sink.ProjectList[i].Typ)
		}
		if !matching {
			continue
		}
		*scan = planpb.Node{NodeType: planpb.Node_PROJECT, NodeId: scan.NodeId,
			Children: append([]int32(nil), sink.Children...), BindingTags: scan.BindingTags,
			ProjectList: DeepCopyExprList(sink.ProjectList), Limit: scan.Limit, Offset: scan.Offset}
		delete(builder.positionalSinkScans, reader)
		delete(builder.cteSinkNodes, sinkID)
		delete(builder.semanticCTESinks, sinkID)
		removed[step] = true
	}
	if len(removed) == 0 {
		return
	}
	stepMap := make([]int32, len(builder.qry.Steps))
	for i := range stepMap {
		stepMap[i] = -1
	}
	steps := builder.qry.Steps[:0]
	for old, id := range builder.qry.Steps {
		if !removed[int32(old)] {
			stepMap[old] = int32(len(steps))
			steps = append(steps, id)
		}
	}
	builder.qry.Steps = steps
	builder.remapCTEStatementSteps(stepMap)
	for _, node := range builder.qry.Nodes {
		for i, step := range node.SourceStep {
			node.SourceStep[i] = stepMap[step]
		}
	}
	for ref, publications := range builder.boundCTEs {
		for owner, bound := range publications {
			if removed[bound.sourceStep] {
				delete(publications, owner)
			} else {
				bound.sourceStep = stepMap[bound.sourceStep]
			}
		}
		if len(publications) == 0 {
			delete(builder.boundCTEs, ref)
		}
	}
}

// These statement owners consume step indexes after createQuery, when CTE
// producer compaction has already changed the step catalog.
func (builder *QueryBuilder) remapCTEStatementSteps(stepMap []int32) {
	remap := func(step int32) int32 {
		if step < 0 {
			return step
		}
		return stepMap[step]
	}
	remapMap := func(old map[string]int32) map[string]int32 {
		if old == nil {
			return nil
		}
		// Maintenance records can share this map. Copy so each stored old index
		// is remapped exactly once, regardless of aliases between owners.
		updated := make(map[string]int32, len(old))
		for name, step := range old {
			updated[name] = remap(step)
		}
		return updated
	}
	if builder.qry.HasReturning {
		builder.qry.ReturningStep = remap(builder.qry.ReturningStep)
	}
	builder.returningSourceStep = remap(builder.returningSourceStep)
	builder.irregularMaintSourceStep = remap(builder.irregularMaintSourceStep)
	builder.irregularMaintDeleteStep = remap(builder.irregularMaintDeleteStep)
	builder.irregularMaintInsertOnlySourceStep = remap(builder.irregularMaintInsertOnlySourceStep)
	builder.irregularMaintValueChangedSourceSteps = remapMap(builder.irregularMaintValueChangedSourceSteps)
	for i := range builder.irregularUpdateMaints {
		owner := &builder.irregularUpdateMaints[i]
		owner.sourceStep = remap(owner.sourceStep)
		owner.deleteStep = remap(owner.deleteStep)
		owner.insertOnlySourceStep = remap(owner.insertOnlySourceStep)
		owner.valueChangedSourceSteps = remapMap(owner.valueChangedSourceSteps)
	}
}

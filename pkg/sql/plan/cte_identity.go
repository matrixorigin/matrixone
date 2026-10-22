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

// boundCTE is a statement-local publication descriptor, never a cache of rows.
// Runtime generations and budgets remain owned by Compile or dependent.region.
type boundCTE struct {
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

func (builder *QueryBuilder) publishBoundCTE(useCtx *BindContext, ref *CTERef, root int32, ctx *BindContext, recursive bool) *boundCTE {
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
	bound := &boundCTE{output: output, database: ctx.cteDatabase, hasSingleRow: !recursive && ctx.hasSingleRow,
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
		builder.boundCTEs = make(map[*CTERef]*boundCTE)
	}
	if builder.semanticCTESinks == nil {
		builder.semanticCTESinks = make(map[int32]*CTERef)
	}
	builder.boundCTEs[ref] = bound
	builder.semanticCTESinks[builder.qry.Steps[bound.sourceStep]] = ref
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

// Value instability is distinct from deterministic row-scoped diagnostics and
// statement-stable functions. Follow dependencies, not the whole node directory.
func (builder *QueryBuilder) cteNeedsIdentity(id int32, seen map[int32]bool) bool {
	if seen[id] {
		return false
	}
	seen[id] = true
	node := builder.qry.Nodes[id]
	if builder.semanticCTESinks[id] != nil {
		return true
	}
	switch node.NodeType {
	case planpb.Node_SAMPLE, planpb.Node_RECURSIVE_CTE, planpb.Node_EXTERNAL_FUNCTION:
		return true
	case planpb.Node_FUNCTION_SCAN:
		if !statementStableFunctionScan(node) {
			return true
		}
	}
	needed := false
	// Catalog defaults/generated expressions are metadata, not scan evaluations.
	executable := *node
	executable.TableDef = nil
	_ = planpb.VisitExpressionsInOwner(&executable, func(expr *planpb.Expr) error {
		return planpb.VisitExprTree(expr, func(expr *planpb.Expr) error {
			if fn := expr.GetF(); fn != nil {
				overload, ok := function.GetFunctionByIdWithoutError(fn.Func.Obj)
				// Existing ENUM/SET display adapters preserve a source value;
				// their legacy no-fold flag does not make that value unstable.
				_, transparent := transparentOutputSourceExpr(expr)
				needed = needed || !ok || overload.IsVolatile() && !transparent
			}
			if sub := expr.GetSub(); sub != nil {
				needed = needed || builder.cteNeedsIdentity(sub.NodeId, seen)
			}
			if call := expr.GetDependent(); call != nil && builder.qry.Correlation != nil {
				needed = needed || builder.cteNeedsIdentity(builder.qry.Correlation.Programs[call.ProgramId-1].RootNode, seen)
			}
			return nil
		})
	})
	if needed {
		return true
	}
	for _, child := range node.Children {
		if builder.cteNeedsIdentity(child, seen) {
			return true
		}
	}
	for _, step := range node.SourceStep {
		if builder.cteNeedsIdentity(builder.qry.Steps[step], seen) {
			return true
		}
	}
	return false
}

// A dependent invocation owns its own sources. It cannot capture a statement's
// materialized value or carry that value through fresh recursive-round regions.
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
		if ref := builder.semanticCTESinks[id]; ref != nil {
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

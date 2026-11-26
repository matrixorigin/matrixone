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

// Package dependent adapts relational demand to the VM continuation driver.
// Pull tasks borrow cursor state; region owners retire every opened cursor.
package dependent

import (
	"context"
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"

	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Source lends each batch until the next Read or Close. Open is deferred until
// a real demand; metadata binding, authorization and template construction are
// performed before the Factory is published.
type Source interface {
	Read(context.Context) (*batch.Batch, error)
	Close() error
}
type SourceFactory func(*process.Process, *mpool.MPool, *mpool.AllocationAccount) (Source, error)
type SourceCompiler func(*plan.Node) (SourceFactory, error)

type nodeTemplate struct {
	countFoundRows   bool
	valuesWarningRow uint64
	readerIDs        []int
	node             *plan.Node
	expressions      *colexec.DependentExpressionProgram
	source           SourceFactory
	output           []types.Type
	children         []int32
	kernel           KernelFactory
	inputExtra       int
	bounds           *colexec.DependentExpressionProgram
	limitIndex       int
	offsetIndex      int
}
type regionTemplate struct {
	owner         uint32
	sourceReaders map[int32]int
	root          int32
	index         map[int32]int
	nodes         []*nodeTemplate
}
type Template struct {
	comparisons map[uint32]*colexec.DependentExpressionProgram
	mainRoots   []int32
	query       *plan.Query
	regions     []regionTemplate
	// scalarShareEligible is IMMUTABLE per-occurrence metadata built once at
	// Compile: only a guarded MERGE clause-payload call (StageId mark) whose
	// capture-free SCALAR program subtree passes the conservative audit -
	// no nested dependent calls, no volatile/real-time overloads, same-type
	// casts only - may be memoized per Execution.
	scalarShareEligible map[uint32]bool
}

// Compile binds all scalar instructions and source factories once. Instancing
// does not visit, optimize, bind or compile a SQL/relational plan.
func Compile(ctx context.Context, query *plan.Query, compileSource SourceCompiler, kernelCompilers ...KernelCompiler) (*Template, error) {
	if err := query.ValidateCorrelation(); err != nil {
		return nil, err
	}
	if query.Correlation == nil {
		return nil, moerr.NewInternalError(ctx, "missing dependent descriptor")
	}
	// A program can be shared by plan producers. Specialize only when every
	// call observes existence; a value consumer keeps the original template.
	uses := make([]uint8, len(query.Correlation.Programs)+1)
	comparisons := make(map[uint32]*colexec.DependentExpressionProgram)
	scalarShareEligible := make(map[uint32]bool)
	guardedOccurrences := icebergMergeGuardedPayloadOccurrences(query)
	if err := plan.WalkQueryNodes(query, func(_ int32, n *plan.Node) error {
		return plan.VisitExpressionsInOwner(n, func(e *plan.Expr) error {
			return plan.VisitExprTree(e, func(e *plan.Expr) error {
				if call := e.GetDependent(); call != nil {
					if call.Comparison != nil && comparisons[call.OccurrenceId] == nil {
						bound, err := colexec.CompileDependentExpressions(ctx, []*plan.Expr{call.Comparison})
						if err != nil {
							return err
						}
						comparisons[call.OccurrenceId] = bound
					}
					if call.Reducer == plan.SubqueryRef_EXISTS || call.Reducer == plan.SubqueryRef_NOT_EXISTS {
						uses[call.ProgramId] |= 1
					} else {
						uses[call.ProgramId] |= 2
					}
					if captureFreeScalarCall(call) && guardedOccurrences[call.OccurrenceId] &&
						auditShareEligibleProgram(query, call.ProgramId) {
						scalarShareEligible[call.OccurrenceId] = true
					}
				}
				return nil
			})
		})
	}); err != nil {
		return nil, err
	}
	t := &Template{
		scalarShareEligible: scalarShareEligible, comparisons: comparisons, query: query, regions: make([]regionTemplate, len(query.Correlation.Programs)+1)}
	if query.StmtType != plan.Query_SELECT {
		for step, root := range query.Steps {
			if query.Correlation.StepOwners[step] == 0 && int32(step) != query.MainOutputStep() && query.Nodes[root].NodeType != plan.Node_SINK {
				t.mainRoots = append(t.mainRoots, root)
			}
		}
	}
	t.mainRoots = append(t.mainRoots, query.Steps[query.MainOutputStep()])
	for i := range t.regions {
		r := &t.regions[i]
		if i == 0 {
			r.root = query.Steps[query.MainOutputStep()]
		} else {
			r.root = query.Correlation.Programs[i-1].RootNode
		}
		var existenceNodes map[int32]*plan.Node
		if uses[i] == 1 {
			existenceNodes = existenceProjectionNodes(query, r.root)
		}
		r.owner = uint32(i)
		r.sourceReaders = make(map[int32]int)
		r.index = make(map[int32]int)
		pending := []int32{r.root}
		if i == 0 {
			pending = append(pending, t.mainRoots...)
		}
		virtualID := int32(-1)
		for len(pending) > 0 {
			id := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			if _, exists := r.index[id]; exists {
				continue
			}
			n := query.Nodes[id]
			if n.NodeType == plan.Node_EXTERNAL_SCAN {
				// Source metadata binding may prune pushed filters. Cached plans
				// and other occurrences retain their original bound expressions.
				n = proto.Clone(n).(*plan.Node)
			}
			if copy := existenceNodes[id]; copy != nil {
				n = copy
			}
			if n.NodeType == plan.Node_LOCK_OP && len(n.ProjectList) == 0 && len(n.Children) == 1 {
				// Legacy DML locks forward the native input without an explicit
				// projection. Preserve that payload in the continuation template,
				// without publishing a specialization into the cached query.
				input := query.Nodes[n.Children[0]]
				n = proto.Clone(n).(*plan.Node)
				for j, expr := range input.ProjectList {
					n.ProjectList = append(n.ProjectList, &plan.Expr{Typ: expr.Typ,
						Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: int32(j)}}})
				}
			}
			nt := &nodeTemplate{node: n, valuesWarningRow: n.ValuesWarningRow(), children: append([]int32(nil), n.Children...)}
			nt.limitIndex, nt.offsetIndex = -1, -1
			var bounds []*plan.Expr
			if n.Limit != nil && n.NodeType != plan.Node_PARTITION {
				nt.limitIndex = len(bounds)
				bounds = append(bounds, n.Limit)
			}
			if n.Offset != nil {
				nt.offsetIndex = len(bounds)
				bounds = append(bounds, n.Offset)
			}
			if len(bounds) > 0 {
				var err error
				nt.bounds, err = colexec.CompileDependentExpressions(ctx, bounds)
				if err != nil {
					return nil, err
				}
			}
			switch n.NodeType {
			case plan.Node_VALUE_SCAN, plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN:
				if compileSource == nil {
					return nil, moerr.NewInternalError(ctx, "missing dependent source compiler")
				}
				var err error
				nt.source, err = compileSource(n)
				if err != nil {
					return nil, err
				}
			case plan.Node_SINK_SCAN, plan.Node_RECURSIVE_SCAN:
				for _, step := range n.SourceStep {
					nt.readerIDs = append(nt.readerIDs, r.sourceReaders[step])
					r.sourceReaders[step]++
				}
			case plan.Node_RECURSIVE_CTE:
			case plan.Node_PROJECT, plan.Node_FILTER, plan.Node_ASSERT, plan.Node_SINK:
				if len(n.Children) != 1 {
					return nil, moerr.NewInternalError(ctx, "dependent unary input")
				}
			default:
				if len(kernelCompilers) == 0 {
					return nil, moerr.NewNYIf(ctx, "dependent relational adapter %s", n.NodeType)
				}
				children := make([]*plan.Node, len(n.Children))
				for j, id := range n.Children {
					children[j] = query.Nodes[id]
				}
				var inputs [][]*plan.Expr
				var err error
				nt.kernel, inputs, err = kernelCompilers[0](n, children)
				if err != nil {
					return nil, err
				}
				if nt.kernel == nil || len(inputs) != len(children) {
					return nil, moerr.NewInternalError(ctx, "invalid dependent native template")
				}
				for j, exprs := range inputs {
					if exprs == nil {
						continue
					}
					projection := &plan.Node{NodeType: plan.Node_PROJECT, NodeId: n.NodeId, Children: []int32{n.Children[j]}, ProjectList: exprs}
					input := &nodeTemplate{node: projection, children: projection.Children}
					if err = input.compileExpressions(ctx, exprs); err != nil {
						return nil, err
					}
					r.index[virtualID] = len(r.nodes)
					r.nodes = append(r.nodes, input)
					nt.children[j] = virtualID
					if j == 0 {
						nt.inputExtra = len(exprs) - len(children[j].ProjectList)
					}
					virtualID--
				}
			}
			roots := append(append([]*plan.Expr(nil), n.FilterList...), n.ProjectList...)
			if n.NodeType == plan.Node_JOIN || n.NodeType == plan.Node_APPLY {
				for j, e := range n.FilterList {
					roots[j] = proto.Clone(e).(*plan.Expr)
					if err := visitInputColumns(roots[j], func(col *plan.ColRef) error {
						for i, projected := range n.ProjectList {
							ref := projected.GetCol()
							if ref != nil && col.RelPos == ref.RelPos && col.ColPos == ref.ColPos {
								col.RelPos, col.ColPos = 0, int32(i)
								return nil
							}
						}
						return moerr.NewInternalError(ctx, "dependent join filter missing column")
					}); err != nil {
						return nil, err
					}
				}
				for j, e := range n.ProjectList {
					roots[len(n.FilterList)+j] = &plan.Expr{Typ: e.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: int32(j)}}}
				}
			}
			if n.NodeType == plan.Node_AGG || n.NodeType == plan.Node_WINDOW || n.NodeType == plan.Node_SAMPLE || n.NodeType == plan.Node_TIME_WINDOW || n.NodeType == plan.Node_FILL {
				for j, e := range roots {
					roots[j] = proto.Clone(e).(*plan.Expr)
					_ = visitInputColumns(roots[j], func(col *plan.ColRef) error {
						{
							if n.NodeType == plan.Node_WINDOW && int(col.ColPos) >= len(query.Nodes[n.Children[0]].ProjectList) {
								col.ColPos += int32(nt.inputExtra)
							}
							if col.RelPos < 0 {
								col.RelPos = 0
							}
						}
						return nil
					})
				}
			}
			if err := nt.compileExpressions(ctx, roots); err != nil {
				return nil, err
			}
			r.index[id] = len(r.nodes)
			r.nodes = append(r.nodes, nt)
			pending = append(pending, n.Children...)
			for _, step := range n.SourceStep {
				pending = append(pending, query.Steps[step])
			}
		}
	}
	return t, nil
}

func (n *nodeTemplate) compileExpressions(ctx context.Context, roots []*plan.Expr) error {
	var err error
	// rowTask evaluates these projections/filters over one materialized node
	// output, including native-kernel and virtual input projections. JOIN's
	// two-input condition and reducer comparisons use their own namespaces.
	n.expressions, err = colexec.CompileDependentSingleInputExpressions(ctx, roots)
	if err != nil {
		return err
	}
	for _, e := range n.node.ProjectList {
		typ, err := types.TypeFromPlan(e.Typ)
		if err != nil {
			return err
		}
		n.output = append(n.output, typ)
	}
	return nil
}

type Execution struct {
	mainStep     int
	affectedRows uint64
	template     *Template
	proc         *process.Process
	allocation   *mpool.MPool
	main         *region
	account      *mpool.AllocationAccount
	// scalarShares memoizes the finalized result of an eligible capture-free
	// scalar occurrence's FIRST winning row so later winning rows of the SAME
	// occurrence inside THIS Execution reuse it instead of opening and
	// re-evaluating the inner relation. The owner is this per-statement
	// Execution - never the reusable Template - so no value crosses a
	// statement, snapshot or execution boundary. Eligibility requires the
	// precise guarded-MERGE-payload StageId mark, the plan's own capture-free
	// provenance and the template's immutable per-occurrence audit; a fresh
	// Execution starts empty and close releases every donated batch and its
	// carried budget exactly once.
	scalarShares map[uint32]*batch.Batch
	// scalarShareCharged is the memo's carried accounting on the Execution's
	// own resource budget: each donated batch's cost was transferred off the
	// donating reducer (which stops owning it) and is released once here.
	scalarShareCharged uint64
	scalarShareBudget  *process.ExecutionResourceGeneration
}

// sharedScalarInvocation serves a memoized capture-free scalar result without
// opening the inner relation: stepping is immediately done, the result is the
// Execution-owned batch, and Close is a no-op because the cache's lifetime is
// the Execution's own close.
type sharedScalarInvocation struct {
	result *batch.Batch
}

func (s *sharedScalarInvocation) Step(context.Context) (vm.ContinuationStep, error) {
	return vm.ContinuationStep{Done: true}, nil
}

func (s *sharedScalarInvocation) Accept(vm.Continuation) error {
	return nil
}

func (s *sharedScalarInvocation) Result() *batch.Batch { return s.result }

func (s *sharedScalarInvocation) Close(_ error) error { return nil }

// CountFoundRowsAt designates the main statement's pagination before the
// template is published. Nested program limits never own session row counts.
func (t *Template) CountFoundRowsAt(nodeID int32) error {
	index, ok := t.regions[0].index[nodeID]
	if !ok {
		return moerr.NewInternalErrorNoCtx("dependent FOUND_ROWS owner is outside the main region")
	}
	t.regions[0].nodes[index].countFoundRows = true
	return nil
}

func (t *Template) NewExecution(proc *process.Process, allocation *mpool.MPool, accounts ...*mpool.AllocationAccount) (*Execution, error) {
	if proc == nil || allocation == nil {
		return nil, moerr.NewInternalErrorNoCtx("dependent execution requires owner")
	}
	// The main region also owns a private board and cancellation generation.
	// Reusing the statement process's board would reuse terminal join-map
	// identities on the next prepared execution.
	owner, err := proc.NewDependentProcess(0, nil, len(t.query.Nodes))
	if err != nil {
		return nil, err
	}
	e := &Execution{template: t, proc: owner, allocation: allocation, scalarShares: make(map[uint32]*batch.Batch)}
	if len(accounts) > 0 {
		e.account = accounts[0]
	}
	r, err := e.newRegion(owner, 0)
	if err != nil {
		owner.CloseDependentProcess()
		return nil, err
	}
	if err = r.reserve(uint64(unsafe.Sizeof(Execution{}))); err != nil {
		_ = r.close(err)
		owner.CloseDependentProcess()
		return nil, err
	}
	e.main = r
	return e, nil
}

// Next is the single top-level trampoline entry. Nested invocations return
// child tasks to this same driver and never call Next or RunContinuation.
func (e *Execution) Next(ctx context.Context) (*batch.Batch, error) {
	if e == nil || e.main == nil {
		return nil, moerr.NewInternalError(ctx, "retired dependent execution")
	}
	for e.mainStep < len(e.template.mainRoots) {
		task, err := e.main.pull(e.template.mainRoots[e.mainStep])
		if err != nil {
			return nil, err
		}
		if err = vm.RunContinuation(ctx, e.main.budget, task, nil); err != nil {
			return nil, err
		}
		if task.result != nil {
			if e.mainStep == len(e.template.mainRoots)-1 {
				return task.result, nil
			}
			continue
		}
		e.mainStep++
	}
	return nil, nil
}
func (e *Execution) Close() error { return e.close(nil) }

func (e *Execution) close(cause error) error {
	if e == nil || e.main == nil {
		return nil
	}
	err := e.main.close(cause)
	e.main = nil
	// Release every memoized capture-free scalar batch exactly once through
	// the allocation its reducer charged: the FIRST winning row donated its
	// finalized reducer tuple here (see shareScalarResult), and later winning
	// rows merely read the cached pointer.
	for occurrence, donated := range e.scalarShares {
		donated.Clean(e.allocation)
		delete(e.scalarShares, occurrence)
	}
	if e.scalarShareCharged > 0 && e.scalarShareBudget != nil {
		e.scalarShareBudget.ReleaseAllocationCapacity(e.scalarShareCharged)
	}
	e.scalarShareCharged = 0
	e.scalarShareBudget = nil
	e.proc.CloseDependentProcess()
	return err
}

// shareScalarResult donates the FIRST winning row's finalized result for the
// given occurrence into the Execution-owned cache. The reducer's tuple is
// detached first so the donating invocation's own Close cannot double-free
// it; the Execution's close is the single release owner.
func (e *Execution) shareScalarResult(occurrence uint32, inv *invocation) {
	if e.scalarShares == nil || inv.reducer == nil || inv.result == nil {
		return
	}
	// Ownership stays with the donating reducer until the transfer is
	// GUARANTEED: the target acquires first, and any failure (a sealed or
	// exhausted budget) leaves both the tuple and its charge exactly where
	// they were, so the first caller still consumes its own result and the
	// reducer's normal Close releases exactly once. Only after a successful
	// reservation does the reducer detach (releasing its charge and dropping
	// its pointer), transferring the release obligation to this Execution.
	budget, err := e.proc.GetExecutionResourceBudget()
	if err != nil {
		return
	}
	cost := inv.reducer.TupleCost()
	if cost == 0 {
		return
	}
	if err := budget.AcquireAllocationCapacity(cost); err != nil {
		return
	}
	inv.reducer.DetachTuple()
	e.scalarShareCharged += cost
	e.scalarShareBudget = budget
	// The donation keeps inv.result itself unchanged: the FIRST winning row
	// must still read its own finalized result from the invocation. Only the
	// ownership changes - the donating invocation's Close must not free the
	// batch (the tuple is detached), and the Execution's close is the single
	// release for the cached pointer shared with later winning rows.
	e.scalarShares[occurrence] = inv.result
}

// captureFreeScalarCall reports the plan-level eligibility for Execution-scoped
// memoization: a single-output SCALAR reduction with no correlated Inputs, no
// Lhs and no Comparison - the closure references nothing from the outer rows
// and its value can only change with a new execution snapshot. Occurrences
// outside this provenance (correlated, comparison-driven, multi-row or
// effect-bearing shapes) always keep their per-row evaluation.
func captureFreeScalarCall(call *plan.DependentExpr) bool {
	return call.Reducer == plan.SubqueryRef_SCALAR &&
		call.Comparison == nil &&
		len(call.Inputs) == 0 &&
		len(call.Lhs) == 0
}

// sharedScalarCallReady is the FULL gate: the precise guarded-MERGE-payload
// membership mark (StageId, set at the planner's flatten admission seam) AND
// the plan-level capture-free provenance AND this template's immutable
// per-occurrence audit result. Everything else - correlated, comparison,
// multi-row, WHEN/USING scopes, volatile or warning-capable or unknown
// nested-program subtrees - keeps its original per-row evaluation.
func sharedScalarCallReady(t *Template, call *plan.DependentExpr) bool {
	return captureFreeScalarCall(call) &&
		t.scalarShareEligible[call.OccurrenceId]
}

// guardedPayloadOccurrences walks the FINAL plan graph for the specialized
// MERGE clause-payload guards: the rewrite wraps every guarded payload slot as
// if(= (winnerRef, lit(ordinal)), <payload expr>, same-typed NULL literal).
// This wrapper is plan-owned final-graph data - it survives deep copy and the
// correlation stage directory remap because it depends on no StageId index -
// and it is produced only for guarded MERGE clause payloads: WHEN slots,
// derived USING subqueries and ordinary SQL never appear inside such a guard,
// even when their calls' shapes look capture-free.
func icebergMergeGuardedPayloadOccurrences(query *plan.Query) map[uint32]bool {
	occurrences := make(map[uint32]bool)
	if query.StmtType != plan.Query_MERGE || query.Correlation == nil || len(query.Steps) == 0 {
		return occurrences
	}
	writer := query.Nodes[query.Steps[query.MainOutputStep()]]
	if writer == nil || writer.ExtraOptions != icebergapi.DMLMergePlanExtraOptions {
		return occurrences
	}
	// The planner-authored membership: the specialized Iceberg MERGE rewrite
	// (rowSelectIcebergMergeClauseValues) stamps GuardedMergePayload on
	// exactly the capture-free SCALAR calls its winner guards coordinate.
	// The correlation finalization only reassigns OccurrenceId/StageId, so
	// the stamp rides each call through copy/remap/serialization; the
	// statement/writer validation above is belt-and-braces for the owning
	// writer. Ordinary CASE/IF lookalikes, MERGE WHEN conditions, derived
	// USING subqueries and every unstamped or older-plan call simply keep
	// per-row semantics.
	for _, n := range query.Nodes {
		for _, e := range n.ProjectList {
			_ = plan.VisitExprTree(e, func(e *plan.Expr) error {
				if call := e.GetDependent(); call != nil && call.GuardedMergePayload {
					occurrences[call.OccurrenceId] = true
				}
				return nil
			})
		}
	}
	return occurrences
}

// auditShareEligibleProgram walks the occurrence's program subtree from its
// rooted nodes and reports whether every reachable expression is provably
// deterministic and side-effect-free for Execution-scoped memoization: no
// nested dependent calls (unknown nested programs stay per-row), no volatile
// or real-time function overloads (the same registry tests the planner uses:
// function.GetFunctionByIdWithoutError + CannotFold/IsRealTimeRelated, see
// cte_reuse.go), and casts only as an identical-type passthrough - a scaling
// or narrowing cast is the row-local clip/warning class and stays per-row.
// isStoredDecimalType reports the narrow proven value domain: the stored
// DECIMAL64/DECIMAL128 representation this optimization was measured for.
func isStoredDecimalType(id int32) bool {
	return id == int32(types.T_decimal64) || id == int32(types.T_decimal128)
}

func auditShareEligibleProgram(query *plan.Query, programID uint32) bool {
	if programID == 0 || int(programID) > len(query.Correlation.Programs) {
		return false
	}
	prog := query.Correlation.Programs[programID-1]
	visited := make(map[int32]bool)
	// root glm93 finding 1: the stored-DECIMAL identity is attribute-exact as
	// well - NumericAttributes (plan.Type field 14) carries the parser/#32002
	// numeric metadata the executor honors, so a differing attribute is a
	// different assignment channel and keeps the occurrence per-row.
	tsame := func(a, b *plan.Type) bool {
		return a != nil && b != nil && a.Id == b.Id &&
			a.Width == b.Width && a.Scale == b.Scale &&
			a.NumericAttributes == b.NumericAttributes
	}
	// The POSITIVE typed class covering every per-invocation evaluation the
	// region performs. Admitted operations are deterministic, warning-free
	// and side-effect-free for the proven shapes ONLY:
	//   - the rewrite's control pair: the IF branch selection over an EQUAL
	//     of two same-typed column/literal comparands (the winner
	//     machinery), with the selected arm audited recursively;
	//   - MIN/MAX by REGISTERED overload over stored DECIMAL arguments;
	//   - an identical-type DECIMAL CAST passthrough (the plan carries its
	//     typed TargetType descriptor as further arguments - accepted only
	//     inside this proven channel);
	//   - CAST_ASSIGN ONLY in the exact identity subcase, where the typed
	//     descriptor equals the value's own type so the assignment cannot
	//     rescale, clip or warn; every other combination - real narrowing,
	//     uncertain typing, volatile or nested dependent forms, unknown
	//     overloads - stays per-row with its original row-local
	//     diagnostics;
	//   - a bare column reference or a literal.
	// The audited occurrence's OWN call is the memoization subject, so it
	// is exempt by exact program identity; any OTHER nested dependent call
	// keeps the occurrence per-row.
	var auditExpr func(e *plan.Expr) bool
	auditExpr = func(e *plan.Expr) bool {
		if e == nil {
			return true
		}
		if e.GetDependent() != nil {
			// A reachable nested call is never proven safe by identity: same
			// ProgramId is not proof of safe nested evaluation, so the
			// occurrence keeps per-row semantics.
			return false
		}
		if e.GetT() != nil {
			// The typed descriptor marker is ONLY a member as an argument of
			// an admitted CAST/CAST_ASSIGN channel; a standalone occurrence
			// is not an auditable value.
			return false
		}
		fn := e.GetF()
		if fn == nil || fn.Func == nil {
			return e.GetCol() != nil || e.GetLit() != nil
		}
		if _, ok := function.GetFunctionByIdWithoutError(fn.Func.Obj); !ok {
			return false
		}
		fnID, _ := function.DecodeOverloadID(fn.Func.Obj)
		switch fnID {
		case function.IFF:
			if len(fn.Args) != 3 {
				return false
			}
			for _, a := range fn.Args {
				if !auditExpr(a) {
					return false
				}
			}
			return true
		case function.EQUAL, function.NOT_EQUAL,
			function.GREAT_THAN, function.GREAT_EQUAL,
			function.LESS_THAN, function.LESS_EQUAL:
			// The pure comparison members of the winner's control
			// machinery: same-typed operands, no coercion warning path.
			if len(fn.Args) != 2 {
				return false
			}
			for _, a := range fn.Args {
				if a == nil || (a.GetCol() == nil && a.GetLit() == nil) {
					return false
				}
			}
			return tsame(&fn.Args[0].Typ, &fn.Args[1].Typ)
		case function.AND, function.OR, function.NOT:
			// The boolean combinators of the same machinery; every operand
			// must itself be an admitted member.
			if len(fn.Args) == 0 || (fnID != function.AND && fnID != function.OR && len(fn.Args) > 1) {
				return false
			}
			for _, a := range fn.Args {
				if !auditExpr(a) {
					return false
				}
			}
			return true
		case function.MIN, function.MAX:
			if len(fn.Args) != 1 || !isStoredDecimalType(fn.Args[0].Typ.Id) {
				return false
			}
			return isStoredDecimalType(e.Typ.Id) && auditExpr(fn.Args[0])
		case function.CAST:
			// The plan's CAST carries its typed TargetType descriptor as
			// further arguments; ONLY the value argument participates in the
			// value audit and ONLY the descriptor markers parse as channel
			// members here.
			if len(fn.Args) < 2 || !isStoredDecimalType(e.Typ.Id) {
				return false
			}
			if !tsame(&fn.Args[0].Typ, &e.Typ) {
				return false
			}
			for _, a := range fn.Args[1:] {
				if a == nil || a.GetT() == nil || !tsame(&a.Typ, &e.Typ) {
					return false
				}
			}
			return auditExpr(fn.Args[0])
		case function.CAST_ASSIGN:
			// #6031303617: CAST_ASSIGN has exactly two registered operands -
			// the value and its typed descriptor. ONLY the exact warning-free
			// stored-DECIMAL identity participates: the result type, the
			// value's own type AND the descriptor all match on the full
			// numeric identity (Id, Width=decimal precision, Scale), and no
			// extra argument is silently skipped. Any extra operand, any
			// non-DECIMAL identity, and any narrowing/rounding channel keeps
			// the occurrence per-row with its original row-local diagnostics.
			if len(fn.Args) != 2 {
				return false
			}
			val, desc := fn.Args[0], fn.Args[1]
			if val == nil || desc == nil {
				return false
			}
			if desc.GetT() == nil {
				return false
			}
			if !isStoredDecimalType(e.Typ.Id) || !isStoredDecimalType(val.Typ.Id) {
				return false
			}
			if !tsame(&e.Typ, &val.Typ) || !tsame(&desc.Typ, &val.Typ) {
				return false
			}
			return auditExpr(val)
		default:
			return false
		}
	}
	allowedNode := func(n *plan.Node) bool {
		switch n.NodeType {
		case plan.Node_PROJECT, plan.Node_TABLE_SCAN, plan.Node_AGG:
			return true
		}
		return false
	}
	var walkNode func(id int32) bool
	walkNode = func(id int32) bool {
		if id < 0 || int(id) >= len(query.Nodes) || visited[id] {
			return true
		}
		visited[id] = true
		node := query.Nodes[id]
		if !allowedNode(node) {
			return false
		}
		eligible := true
		_ = plan.VisitExpressionsInOwner(node, func(fe *plan.Expr) error {
			if eligible && !auditExpr(fe) {
				eligible = false
			}
			return nil
		})
		if !eligible {
			return false
		}
		for _, child := range node.Children {
			if !walkNode(child) {
				return false
			}
		}
		// SourceStep entries index query.Steps: the native Compile resolves
		// query.Steps[step] and a region whose local source edge needs data
		// pulls it (source.go r.pull(query.Steps[step])). The resolved node
		// is the MATERIALIZED STEP BOUNDARY of the owning outer pipeline:
		// the main step computed it once (its sink), and this region reads
		// rows through the registered reader edge (the regionTemplate build
		// assigns SINK_SCAN readerIDs over those steps). The boundary node's
		// OWN expressions - the guarded payload projections the region then
		// re-frames per invocation - are audited fully here with the same
		// typed class; the boundary's CHILDREN are the outer step's own
		// once-run computation and are not this region's frames, so the walk
		// stops at the sink edge without a visited flag and without any
		// suppression of a later direct-child inspection. Any out-of-range
		// or unresolvable edge fails closed.
		for _, step := range node.SourceStep {
			if step < 0 || int(step) >= len(query.Steps) {
				return false
			}
			// readSource pulls the resolved root inside this region and its
			// DESCENDANTS also execute, so the audit walks the whole
			// resolved producer subtree with the same owner-aware
			// recursion and fail-closed bounds - never the resolved node
			// alone.
			if !walkNode(query.Steps[step]) {
				return false
			}
		}
		return true
	}
	// The invocation pulls its program RootNode on every row: the root's
	// OWN expressions are per-invocation work and are audited like every
	// other node of the region's reachable graph.
	return walkNode(prog.RootNode)
}

func (e *Execution) Open(parent *process.Process, call *plan.DependentExpr, slots, lhs []*vector.Vector) (colexec.DependentInvocation, error) {
	if call.ProgramId == 0 || int(call.ProgramId) >= len(e.template.regions) {
		return nil, moerr.NewInternalErrorNoCtx("dependent program id")
	}
	if sharedScalarCallReady(e.template, call) {
		if cached, ok := e.scalarShares[call.OccurrenceId]; ok {
			return &sharedScalarInvocation{result: cached}, nil
		}
	}
	p := e.template.query.Correlation.Programs[call.ProgramId-1]
	child, err := parent.NewDependentProcess(call.ProgramId, slots, len(e.template.query.Nodes))
	if err != nil {
		return nil, err
	}
	r, err := e.newRegion(child, call.ProgramId)
	if err != nil {
		child.CloseDependentProcess()
		return nil, err
	}
	r.existenceOnly = call.Reducer == plan.SubqueryRef_EXISTS || call.Reducer == plan.SubqueryRef_NOT_EXISTS
	if err = r.reserve(uint64(unsafe.Sizeof(invocation{})) + uint64(unsafe.Sizeof(batch.Batch{}))); err != nil {
		_ = r.close(err)
		child.CloseDependentProcess()
		return nil, err
	}
	lhsBatch := &batch.Batch{Vecs: lhs}
	lhsBatch.SetRowCount(1)
	inv := &invocation{region: r, proc: child}
	if sharedScalarCallReady(e.template, call) {
		inv.shareExecution, inv.shareOccurrence = e, call.OccurrenceId
	}
	inv.reducer, err = colexec.NewDependentReducer(child, call, p.OutputTypes, lhsBatch, e.allocation, e.template.comparisons[call.OccurrenceId])
	if err != nil {
		_ = inv.Close(err)
		return nil, err
	}
	return inv, nil
}

type invocation struct {
	region  *region
	proc    *process.Process
	reducer *colexec.DependentReducer
	done    bool
	result  *batch.Batch
	// shareExecution/shareOccurrence are set only for the eligible
	// capture-free scalar class so the FIRST winning row donates its
	// finalized result; they are nil for every ineligible invocation.
	shareExecution  *Execution
	shareOccurrence uint32
}

func (i *invocation) Step(context.Context) (vm.ContinuationStep, error) {
	if i.done {
		if err := i.region.close(nil); err != nil {
			return vm.ContinuationStep{}, err
		}
		var err error
		i.result, err = i.reducer.Finish()
		if err == nil && i.shareExecution != nil && i.result != nil {
			// First-demand donating: only a successful finalized result is
			// cached, never a partial or failed one.
			i.shareExecution.shareScalarResult(i.shareOccurrence, i)
		}
		return vm.ContinuationStep{Done: err == nil}, err
	}
	task, err := i.region.pull(i.region.template.root)
	if task != nil {
		// Reducers can decide after one row. Never evaluate later projection
		// demands before the reducer has accepted this row.
		task.maxRows = 1
	}
	return vm.ContinuationStep{Child: task}, err
}
func (i *invocation) Accept(child vm.Continuation) error {
	task, ok := child.(*pullTask)
	if !ok {
		return moerr.NewInternalErrorNoCtx("invalid dependent relation result")
	}
	if task.result == nil {
		i.done = true
		return nil
	}
	stop, err := i.reducer.Feed(task.result)
	i.done = stop
	return err
}
func (i *invocation) Result() *batch.Batch { return i.result }
func (i *invocation) Close(cause error) error {
	err := i.region.close(cause)
	if i.reducer != nil {
		i.reducer.Close()
		i.reducer = nil
	}
	i.proc.CloseDependentProcess()
	return err
}

type region struct {
	existenceOnly bool
	sources       []*sourceEdge
	frontier      *sourceEdge
	frontierStep  int32
	execution     *Execution
	template      *regionTemplate
	proc          *process.Process
	budget        *process.ExecutionResourceGeneration
	cursors       []*cursor
	charged       uint64
	closed        bool
	failure       error
}

func (e *Execution) newRegion(proc *process.Process, id uint32) (*region, error) {
	t := &e.template.regions[id]
	budget, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, err
	}
	size := uint64(unsafe.Sizeof(region{})) + uint64(len(t.nodes))*uint64(unsafe.Sizeof((*cursor)(nil))) + uint64(len(e.template.query.Steps))*uint64(unsafe.Sizeof((*sourceEdge)(nil)))
	if err = budget.AcquireAllocationCapacity(size); err != nil {
		return nil, err
	}
	return &region{execution: e, template: t, proc: proc, budget: budget, cursors: make([]*cursor, len(t.nodes)), sources: make([]*sourceEdge, len(e.template.query.Steps)), charged: size}, nil
}
func (r *region) reserve(size uint64) error {
	if err := r.budget.AcquireAllocationCapacity(size); err != nil {
		return err
	}
	r.charged += size
	return nil
}
func (r *region) pull(id int32) (*pullTask, error) {
	if r.failure != nil {
		return nil, r.failure
	}
	if r.closed {
		return nil, moerr.NewInternalErrorNoCtx("retired dependent region")
	}
	index, ok := r.template.index[id]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("dependent region boundary")
	}
	c := r.cursors[index]
	if c == nil {
		if err := r.reserve(uint64(unsafe.Sizeof(cursor{}))); err != nil {
			return nil, err
		}
		c = &cursor{region: r, template: r.template.nodes[index]}
		r.cursors[index] = c
	}
	if c.busy {
		return nil, moerr.NewInternalErrorNoCtx("concurrent dependent cursor demand")
	}
	size := uint64(unsafe.Sizeof(pullTask{}))
	if err := r.budget.AcquireAllocationCapacity(size); err != nil {
		return nil, err
	}
	c.busy = true
	return &pullTask{cursor: c, charged: size, maxRows: 8192}, nil
}
func (r *region) close(cause error) error {
	if cause == nil {
		cause = r.failure
	}
	if r.closed {
		return nil
	}
	r.closed = true
	var err error
	for i := len(r.cursors) - 1; i >= 0; i-- {
		c := r.cursors[i]
		if c == nil {
			continue
		}
		if c.frame != nil {
			c.frame.Close()
			c.frame = nil
		}
		if c.boundFrame != nil {
			c.boundFrame.Close()
			c.boundFrame = nil
		}
		if c.recursive != nil {
			err = joinCloseError(err, c.recursive.close(cause))
			c.recursive = nil
		}
		if c.kernel != nil {
			if r.template.owner == 0 {
				r.execution.affectedRows += c.kernel.affectedRows()
			}
			err = joinCloseError(err, c.kernel.close(cause))
			c.kernel = nil
		}
	}
	// Every native/frame borrower is quiescent before any input owner retires.
	for i := len(r.cursors) - 1; i >= 0; i-- {
		c := r.cursors[i]
		if c == nil {
			continue
		}
		if c.source != nil {
			if terminal, ok := c.source.(interface{ CloseWithCause(error) error }); ok {
				err = joinCloseError(err, terminal.CloseWithCause(errors.Join(cause, err)))
			} else {
				err = joinCloseError(err, c.source.Close())
			}
			c.source = nil
		}
		if c.ownedInput != nil {
			c.ownedInput.Clean(c.ownedInputMP)
			c.ownedInput = nil
		}
		if c.output != nil {
			c.output.Clean(r.execution.allocation)
			c.output = nil
		}
		c.input = nil
	}
	for _, s := range r.sources {
		if s != nil {
			s.close()
		}
	}
	r.sources = nil
	r.cursors = nil
	r.budget.ReleaseAllocationCapacity(r.charged)
	r.charged = 0
	return err
}

type cursor struct {
	recursive      *recursiveCursor
	sourceArm      int
	sourcePosition int
	sourcePending  bool
	edge           *sourceEdge
	ownedInput     *batch.Batch
	// ownedInputMP is the pool ownedInput belongs to (its source's).
	ownedInputMP *mpool.MPool
	region       *region
	template     *nodeTemplate
	source       Source
	input        *batch.Batch
	output       *batch.Batch
	row          int
	expr         int
	frame        *colexec.DependentExpressionFrame
	eof          bool
	busy         bool
	kernel       *nativeKernel
	pendingInput int
	boundFrame   *colexec.DependentExpressionFrame
	boundPC      int
	boundDone    bool
	limit        uint64
	offset       uint64
	emitted      uint64
	foundRows    uint64
	countedRow   bool
}
type pullTask struct {
	maxRows int
	cursor  *cursor
	started bool
	result  *batch.Batch
	charged uint64
}

func (t *pullTask) Step(ctx context.Context) (vm.ContinuationStep, error) {
	c := t.cursor
	r := c.region
	if !t.started {
		t.started = true
		if c.output != nil {
			c.output.CleanOnlyData()
		}
	}
	if !c.boundDone {
		if c.template.bounds == nil {
			c.limit = ^uint64(0)
			c.boundDone = true
		} else {
			if c.boundFrame == nil {
				var err error
				c.boundFrame, err = c.template.bounds.NewFrame(r.proc, nil, r.execution, r.execution.allocation)
				if err != nil {
					return vm.ContinuationStep{}, err
				}
				c.limit = ^uint64(0)
			}
			count := 0
			if c.template.limitIndex >= 0 {
				count++
			}
			if c.template.offsetIndex >= 0 {
				count++
			}
			for c.boundPC < count {
				value, err := c.boundFrame.Value(c.boundPC)
				if err != nil {
					task, err := c.boundFrame.Task(c.boundPC)
					return vm.ContinuationStep{Child: task}, err
				}
				if value.Length() != 1 || value.IsNull(0) || value.GetType().Oid != types.T_uint64 {
					return vm.ContinuationStep{}, moerr.NewInternalError(ctx, "invalid dependent limit bound")
				}
				v := vector.GetFixedAtNoTypeCheck[uint64](value, 0)
				if c.boundPC == c.template.limitIndex {
					c.limit = v
				} else {
					c.offset = v
				}
				c.boundPC++
			}
			c.boundFrame.Close()
			c.boundFrame = nil
			c.boundDone = true
		}
	}
	for {
		if err := context.Cause(ctx); err != nil {
			return vm.ContinuationStep{}, err
		}
		if c.emitted >= c.limit && !c.template.countFoundRows {
			if c.output != nil && c.output.RowCount() > 0 {
				t.result = c.output
			}
			return vm.ContinuationStep{Done: true}, nil
		}
		if c.input == nil || c.row == c.input.RowCount() {
			if c.output != nil && c.output.RowCount() > 0 {
				t.result = c.output
				return vm.ContinuationStep{Done: true}, nil
			}
			if c.eof {
				if c.template.countFoundRows {
					r.proc.SetFoundRows(c.foundRows)
				}
				return vm.ContinuationStep{Done: true}, nil
			}
			if c.ownedInput != nil {
				c.ownedInput.Clean(c.ownedInputMP)
				c.ownedInput = nil
			}
			c.input = nil
			c.row = 0
			if c.template.node.NodeType == plan.Node_SINK_SCAN || c.template.node.NodeType == plan.Node_RECURSIVE_SCAN {
				child, err := c.readSource(ctx)
				if err != nil || child != nil {
					return vm.ContinuationStep{Child: child}, err
				}
				continue
			}
			if c.template.node.NodeType == plan.Node_RECURSIVE_CTE {
				if c.recursive == nil {
					var err error
					c.recursive, err = c.openRecursive()
					if err != nil {
						return vm.ContinuationStep{}, err
					}
				}
				child, err := c.recursive.task()
				return vm.ContinuationStep{Child: child}, err
			}
			if c.template.kernel != nil {
				if c.kernel == nil {
					var err error
					c.kernel, err = openKernel(r.proc, c.template.kernel, len(c.template.children), r.execution.account, r.execution)
					if err != nil {
						return vm.ContinuationStep{}, err
					}
				}
				var need int
				var err error
				var continuation vm.Continuation
				c.input, need, continuation, err = c.kernel.read()
				if err != nil {
					return vm.ContinuationStep{}, err
				}
				if continuation != nil {
					return vm.ContinuationStep{Child: continuation}, nil
				}
				if need >= 0 {
					c.pendingInput = need
					child, err := r.pull(c.template.children[need])
					return vm.ContinuationStep{Child: child}, err
				}
				if c.input == nil {
					c.eof = true
				}
				continue
			}
			if c.template.source == nil {
				child, err := r.pull(c.template.children[0])
				if child != nil {
					child.maxRows = t.maxRows
					if remaining := c.limit - c.emitted; remaining < uint64(child.maxRows) && (!c.template.countFoundRows || c.emitted < c.limit) {
						child.maxRows = int(remaining)
					}
				}
				return vm.ContinuationStep{Child: child}, err
			}
			if c.source == nil {
				var err error
				c.source, err = c.template.source(r.proc, r.execution.allocation, r.execution.account)
				if err != nil {
					return vm.ContinuationStep{}, err
				}
			}
			var err error
			c.input, err = c.source.Read(ctx)
			if err != nil {
				return vm.ContinuationStep{}, err
			}
			if c.input == nil {
				c.eof = true
			}
			continue
		}
		if c.frame == nil {
			var err error
			c.frame, err = c.template.expressions.NewFrame(r.proc, []colexec.DependentInput{{Batch: c.input, Row: c.row}}, r.execution, r.execution.allocation)
			if err != nil {
				return vm.ContinuationStep{}, err
			}
			c.frame.ValuesWarningRow = c.template.valuesWarningRow
			c.expr = 0
			c.countedRow = false
		}
		nfilters := len(c.template.node.FilterList)
		noutputs := len(c.template.output)
		if r.existenceOnly && c.template == r.template.nodes[r.template.index[r.template.root]] {
			noutputs = 0
		}
		nexprs := nfilters + noutputs
		if c.expr == nfilters && !c.countedRow {
			c.countedRow = true
			if c.template.countFoundRows {
				c.foundRows++
			}
		}
		if c.expr == nfilters && c.template.countFoundRows && c.emitted >= c.limit {
			c.frame.Close()
			c.frame = nil
			c.row++
			continue
		}
		if c.expr == nfilters && c.offset > 0 {
			c.offset--
			c.frame.Close()
			c.frame = nil
			c.row++
			continue
		}
		if c.expr < nexprs {
			value, err := c.frame.Value(c.expr)
			if err != nil {
				child, err := c.frame.Task(c.expr)
				return vm.ContinuationStep{Child: child}, err
			}
			if c.expr < nfilters && (value.IsNull(0) || !vector.GetFixedAtNoTypeCheck[bool](value, 0)) {
				c.frame.Close()
				c.frame = nil
				c.row++
				continue
			}
			c.expr++
			continue
		}
		if c.output == nil {
			size := uint64(unsafe.Sizeof(batch.Batch{})) + uint64(noutputs)*uint64(unsafe.Sizeof(vector.Vector{})+unsafe.Sizeof((*vector.Vector)(nil)))
			if err := r.reserve(size); err != nil {
				return vm.ContinuationStep{}, err
			}
			c.output = batch.NewWithSize(noutputs)
			for i, typ := range c.template.output[:noutputs] {
				c.output.Vecs[i] = vector.NewVec(typ, r.execution.allocation)
			}
		}
		for i, vec := range c.output.Vecs {
			value, err := c.frame.Value(nfilters + i)
			if err != nil {
				return vm.ContinuationStep{}, err
			}
			if err = vec.UnionOne(value, 0, r.execution.allocation); err != nil {
				return vm.ContinuationStep{}, err
			}
		}
		c.output.SetRowCount(c.output.RowCount() + 1)
		c.emitted++
		c.frame.Close()
		c.frame = nil
		c.row++
		if c.output.RowCount() == t.maxRows {
			t.result = c.output
			return vm.ContinuationStep{Done: true}, nil
		}
	}
}
func (t *pullTask) Accept(child vm.Continuation) error {
	if p, ok := child.(*recursivePull); ok {
		t.cursor.input = p.result
		t.cursor.eof = p.result == nil
		return nil
	}
	if p, ok := child.(*pullTask); ok {
		if t.cursor.sourcePending {
			t.cursor.sourcePending = false
			return t.cursor.edge.accept(p.result)
		}
		if t.cursor.kernel != nil {
			return t.cursor.kernel.supply(t.cursor.pendingInput, p.result)
		}
		t.cursor.input = p.result
		t.cursor.eof = p.result == nil
	}
	return nil
}

func joinCloseError(first, next error) error {
	if first == nil {
		return next
	}
	if next == nil {
		return first
	}
	return errors.Join(first, next)
}
func (t *pullTask) Close(err error) error {
	if err != nil && t.cursor.region.failure == nil {
		t.cursor.region.failure = err
	}
	t.cursor.busy = false
	t.cursor.region.budget.ReleaseAllocationCapacity(t.charged)
	t.charged = 0
	return nil
}

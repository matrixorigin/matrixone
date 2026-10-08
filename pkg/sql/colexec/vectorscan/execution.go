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

// Package vectorscan prepares immutable vector-index plan specifications into
// execution-local state shared by standalone scans and correlated APPLY.
package vectorscan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Execution owns one correlated APPLY execution generation. Its specification is
// a private copy with statement-level expressions bound; its executors retain
// the row-dependent query, limit and algorithm expressions until Close.
type Execution struct {
	spec      *plan.IndexSearchScan
	executors []colexec.ExpressionExecutor
	queryVec  *vector.Vector
	limitVec  *vector.Vector
	// algoVecs[i] is the evaluated spec.AlgoExprs[i] of the current batch.
	algoVecs []*vector.Vector
}

// PrepareScalar returns an execution-local specification whose dynamic
// expressions are folded. The caller may serialize or otherwise mutate this
// returned copy; template is never modified.
func PrepareScalar(template *plan.IndexSearchScan, proc *process.Process) (*plan.IndexSearchScan, error) {
	spec, err := newGenerationSpec(template, proc)
	if err != nil {
		return nil, err
	}
	if err = foldExpr(&spec.QueryPayload, proc); err != nil {
		return nil, err
	}
	if err = foldExpr(&spec.CandidateLimit, proc); err != nil {
		return nil, err
	}
	for i := range spec.AlgoExprs {
		if err = foldExpr(&spec.AlgoExprs[i], proc); err != nil {
			return nil, err
		}
	}
	return spec, nil
}

// PrepareCorrelatedExecution creates a sealed execution generation. Partial executor
// construction is cleaned by the colexec constructor before returning error.
func PrepareCorrelatedExecution(template *plan.IndexSearchScan, proc *process.Process) (*Execution, error) {
	spec, err := newGenerationSpec(template, proc)
	if err != nil {
		return nil, err
	}
	exprs := append([]*plan.Expr{spec.QueryPayload, spec.CandidateLimit}, spec.AlgoExprs...)
	executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, exprs)
	if err != nil {
		return nil, err
	}
	return &Execution{spec: spec, executors: executors}, nil
}

func newGenerationSpec(template *plan.IndexSearchScan, proc *process.Process) (*plan.IndexSearchScan, error) {
	if template == nil || template.Index == nil || template.QueryPayload == nil || template.CandidateLimit == nil {
		return nil, moerr.NewInvalidInput(proc.Ctx, "vector index scan has incomplete metadata")
	}
	if len(template.AlgoExprs) != len(template.AlgoExprNames) {
		return nil, moerr.NewInvalidInput(proc.Ctx, "vector index scan algorithm expressions are not all named")
	}
	spec := plan2.DeepCopyIndexSearchScan(template)
	for i := range spec.PreFilters {
		if err := foldExpr(&spec.PreFilters[i], proc); err != nil {
			return nil, err
		}
	}
	if spec.DistanceRange != nil {
		if err := foldExpr(&spec.DistanceRange.LowerBound, proc); err != nil {
			return nil, err
		}
		if err := foldExpr(&spec.DistanceRange.UpperBound, proc); err != nil {
			return nil, err
		}
	}
	return spec, nil
}

func foldExpr(expr **plan.Expr, proc *process.Process) error {
	if expr == nil || *expr == nil {
		return nil
	}
	folded, err := plan2.ConstantFold(batch.EmptyForConstFoldBatch, *expr, proc, true, true)
	if err != nil {
		return err
	}
	*expr = folded
	return nil
}

// EvalBatch evaluates all row-dependent expressions once for a provider batch.
// Returned vectors remain owned by the executors and are valid until the next
// EvalBatch or Close.
func (b *Execution) EvalBatch(in *batch.Batch, proc *process.Process) error {
	if b == nil || len(b.executors) < 2 {
		return moerr.NewInvalidState(proc.Ctx, "correlated vector scan is not prepared")
	}
	var err error
	b.queryVec, err = b.executors[0].Eval(proc, []*batch.Batch{in}, nil)
	if err != nil {
		return err
	}
	b.limitVec, err = b.executors[1].Eval(proc, []*batch.Batch{in}, nil)
	if err != nil {
		return err
	}
	b.algoVecs = b.algoVecs[:0]
	for _, executor := range b.executors[2:] {
		vec, err := executor.Eval(proc, []*batch.Batch{in}, nil)
		if err != nil {
			return err
		}
		b.algoVecs = append(b.algoVecs, vec)
	}
	return nil
}

// RequestAt materializes one row of a correlated provider into an owned
// request. ok=false represents a NULL query vector and therefore an empty
// search result.
func (b *Execution) RequestAt(row int, identity searchplugin.ScanIdentity) (req searchplugin.Request, ok bool, err error) {
	if b == nil || b.spec == nil || b.queryVec == nil || b.limitVec == nil {
		return req, false, moerr.NewInvalidStateNoCtx("correlated vector scan has no evaluated provider batch")
	}
	if row < 0 || row >= b.queryVec.Length() || row >= b.limitVec.Length() {
		return req, false, moerr.NewInvalidInputNoCtx("correlated vector scan provider row is out of range")
	}
	if b.queryVec.IsNull(uint64(row)) {
		return req, false, nil
	}
	if b.limitVec.IsNull(uint64(row)) || types.T(b.limitVec.GetType().Oid) != types.T_uint64 {
		return req, false, moerr.NewInvalidInputNoCtx("vector index result limit did not evaluate to uint64")
	}
	resultLimit := vector.GetFixedAtNoTypeCheck[uint64](b.limitVec, row)
	req = requestForValues(
		b.spec,
		append([]byte(nil), b.queryVec.GetBytesAt(row)...),
		b.spec.QueryPayload.Typ,
		resultLimit,
		identity,
	)
	for i, vec := range b.algoVecs {
		if row >= vec.Length() {
			return searchplugin.Request{}, false,
				moerr.NewInvalidInputNoCtx("vector index algorithm expression provider row is out of range")
		}
		at := uint64(row)
		if vec.IsConst() {
			at = 0
		}
		lit := rule.GetConstantValue(vec, true, at)
		if lit == nil {
			return searchplugin.Request{}, false, moerr.NewInvalidInputNoCtxf(
				"vector index algorithm expression %s did not evaluate to a constant", b.spec.AlgoExprNames[i])
		}
		req.AlgoValues = append(req.AlgoValues, searchplugin.AlgoValue{Name: b.spec.AlgoExprNames[i], Value: lit})
	}
	return req, true, nil
}

// Spec returns the immutable-for-this-generation metadata and bound static
// expressions supplied to every reader created by this execution.
func (b *Execution) Spec() *plan.IndexSearchScan {
	if b == nil {
		return nil
	}
	return b.spec
}

// Close releases one generation. It is idempotent.
func (b *Execution) Close() {
	if b == nil {
		return
	}
	for _, executor := range b.executors {
		executor.Free()
	}
	b.executors = nil
	b.queryVec, b.limitVec, b.algoVecs = nil, nil, nil
	b.spec = nil
}

// Identity resolves the physical relation owner once, using the same
// publisher-over-snapshot precedence as ordinary table scans.
func Identity(
	spec *plan.IndexSearchScan,
	currentSnapshot timestamp.Timestamp,
	txnOffset int,
	partitionCount int32,
	partitionIndex int32,
) (searchplugin.ScanIdentity, error) {
	identity := searchplugin.ScanIdentity{
		TxnOffset:      txnOffset,
		PartitionCount: partitionCount,
		PartitionIndex: partitionIndex,
	}
	if identity.PartitionCount <= 0 {
		identity.PartitionCount = 1
	}
	if spec == nil {
		return identity, moerr.NewInvalidInputNoCtx("vector index scan is missing metadata")
	}
	historicalSnapshot := spec.ScanSnapshot != nil && spec.ScanSnapshot.TS != nil &&
		!spec.ScanSnapshot.TS.Equal(timestamp.Timestamp{}) && spec.ScanSnapshot.TS.Less(currentSnapshot)
	if historicalSnapshot {
		identity.Snapshot = plan2.DeepCopySnapshot(spec.ScanSnapshot)
	}
	if spec.SourceTable != nil && spec.SourceTable.PubInfo != nil {
		if spec.SourceTable.PubInfo.TenantId < 0 {
			return identity, moerr.NewInvalidInputNoCtx("vector index scan has an invalid publisher tenant")
		}
		accountID := uint32(spec.SourceTable.PubInfo.TenantId)
		identity.PhysicalAccountID = &accountID
	} else if historicalSnapshot && spec.ScanSnapshot.Tenant != nil {
		accountID := spec.ScanSnapshot.Tenant.TenantID
		identity.PhysicalAccountID = &accountID
	}
	return identity, nil
}

// RequestFromScalar extracts an already folded scalar specification. ok=false
// represents a NULL query vector and therefore an empty reader.
func RequestFromScalar(
	spec *plan.IndexSearchScan,
	identity searchplugin.ScanIdentity,
	membership []byte,
	hasMembership bool,
	membershipRequired bool,
) (req searchplugin.Request, ok bool, err error) {
	if spec == nil || spec.QueryPayload == nil {
		return req, false, moerr.NewInvalidInputNoCtx("vector index scan has incomplete bound expressions")
	}
	queryLit := spec.QueryPayload.GetLit()
	if queryLit == nil {
		return req, false, moerr.NewInvalidInputNoCtx("vector index query vector did not fold at execution")
	}
	if queryLit.Isnull {
		return req, false, nil
	}
	if spec.CandidateLimit == nil {
		return req, false, moerr.NewInvalidInputNoCtx("vector index result limit did not fold at execution")
	}
	limitLit := spec.CandidateLimit.GetLit()
	if limitLit == nil || limitLit.Isnull {
		return req, false, moerr.NewInvalidInputNoCtx("vector index result limit did not fold at execution")
	}
	limit, ok := limitLit.Value.(*plan.Literal_U64Val)
	if !ok {
		return req, false, moerr.NewInvalidInputNoCtx("vector index result limit is not uint64")
	}
	req = requestForValues(spec, []byte(queryLit.GetVecVal()), spec.QueryPayload.Typ, limit.U64Val, identity)
	req.CollectExplainDiagnostics = true
	req.MembershipFilter = append([]byte(nil), membership...)
	req.HasMembershipFilter = hasMembership
	req.MembershipFilterRequired = membershipRequired
	if len(spec.AlgoExprs) != len(spec.AlgoExprNames) {
		return searchplugin.Request{}, false,
			moerr.NewInvalidInputNoCtx("vector index scan algorithm expressions are not all named")
	}
	for i, expr := range spec.AlgoExprs {
		lit := expr.GetLit()
		if lit == nil {
			return searchplugin.Request{}, false, moerr.NewInvalidInputNoCtxf(
				"vector index algorithm expression %s did not fold at execution", spec.AlgoExprNames[i])
		}
		req.AlgoValues = append(req.AlgoValues, searchplugin.AlgoValue{Name: spec.AlgoExprNames[i], Value: lit})
	}
	return req, true, nil
}

func requestForValues(
	spec *plan.IndexSearchScan,
	query []byte,
	queryType plan.Type,
	resultLimit uint64,
	identity searchplugin.ScanIdentity,
) searchplugin.Request {
	candidateBudget := resultLimit
	if spec.PostFilterOverFetch {
		candidateBudget = postFilterCandidateBudget(spec, resultLimit)
	}
	return searchplugin.Request{
		QueryPayload:    query,
		QueryType:       queryType,
		ResultLimit:     resultLimit,
		CandidateBudget: candidateBudget,
		PreFilters:      spec.PreFilters,
		DistanceRange:   spec.DistanceRange,
		Identity:        identity,
	}
}

// postFilterCandidateBudget returns the candidate budget the search plugin of
// spec sizes for a post-filtered result of resultLimit rows.
func postFilterCandidateBudget(spec *plan.IndexSearchScan, resultLimit uint64) uint64 {
	if p, ok := indexplugin.Get(spec.GetIndex().GetIndexAlgo()); ok {
		if searcher, ok := p.(indexplugin.SearchPlugin); ok {
			if budget, ok := searcher.Search().(searchplugin.CandidateBudgetHooks); ok {
				return budget.PostFilterCandidateBudget(resultLimit)
			}
		}
	}
	return overfetch.FilteredPostModeLimit(resultLimit)
}

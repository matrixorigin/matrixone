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

// Package search defines the optional execution capability for an
// optimizer-visible vector-index scan.  It deliberately exposes the engine
// Reader contract instead of SQL compile internals, so algorithms can own
// their access mechanics without adding algorithm switches to pkg/sql.
package search

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ScanIdentity is the physical identity shared by every hidden-relation read
// in one vector-search execution generation.
type ScanIdentity struct {
	PhysicalAccountID *uint32
	Snapshot          *plan.Snapshot
	TxnOffset         int
	PartitionCount    int32
	PartitionIndex    int32
	// IsRemote identifies the execution route, independently of the object
	// partition ordinal. Only the coordinator collects distributed in-memory rows.
	IsRemote bool
}

// Request contains the fully bound state for one vector-search execution.
// The plan specification passed beside it is immutable catalog/index metadata;
// readers must not evaluate or mutate its dynamic expressions.
type Request struct {
	QueryPayload []byte
	QueryType    plan.Type
	// QueryIsNull means the query payload evaluated to NULL.
	QueryIsNull bool
	// ResultLimit is the folded IndexSearchScan.CandidateLimit; 0 when the
	// scan carries no candidate limit.
	ResultLimit     uint64
	CandidateBudget uint64
	// AlgoValues are the evaluated IndexSearchScan.AlgoExprs, by name, for this
	// search. Only the index algorithm that declared them interprets them.
	AlgoValues       []AlgoValue
	PreFilters       []*plan.Expr
	DistanceRange    *plan.DistRange
	MembershipFilter []byte
	// HasMembershipFilter distinguishes an exact empty key set from the
	// absence of a runtime membership predicate (for example RF PASS).
	HasMembershipFilter bool
	// MembershipFilterRequired means candidate limiting is only semantically
	// valid after this exact membership predicate has been applied.
	MembershipFilterRequired bool
	// MembershipFilterPassed means a membership runtime filter was planned but
	// its producer returned PASS, so no membership predicate is applied.
	MembershipFilterPassed bool
	// CollectExplainDiagnostics is enabled only for standalone scalar scans.
	// Correlated APPLY executes one reader per provider row and must not retain
	// per-round diagnostics with unbounded outer-row cardinality.
	CollectExplainDiagnostics bool
	Identity                  ScanIdentity
}

// AlgoValue is one evaluated row-dependent algorithm expression.
type AlgoValue struct {
	Name  string
	Value *plan.Literal
}

// AlgoValue returns the evaluated algorithm expression named name.
func (r Request) AlgoValue(name string) (*plan.Literal, bool) {
	for _, v := range r.AlgoValues {
		if v.Name == name {
			return v.Value, true
		}
	}
	return nil, false
}

// Hooks builds the reader for one vector-index scan execution generation.
// The returned reader owns all per-search child readers and must release them
// from Close on success, error, cancellation, and prepared-plan reuse.
type Hooks interface {
	NewReader(proc *process.Process, spec *plan.IndexSearchScan, req Request) (engine.Reader, error)
}

// ParallelHooks optionally partitions one coordinator-local search into disjoint
// readers. The returned slice must match parallelism, including empty shards.
// Consumers without local parallelism, including APPLY, can keep Hooks.NewReader.
type ParallelHooks interface {
	// CanParallelize reports whether spec can be split into partitioned readers.
	CanParallelize(spec *plan.IndexSearchScan) (bool, error)
	NewReaders(proc *process.Process, spec *plan.IndexSearchScan, req Request, parallelism int) ([]engine.Reader, error)
}

// EmptyScanHooks optionally checks a scan that returns no rows without
// searching: its query payload is NULL (req.QueryIsNull) or a runtime filter
// dropped it. It runs instead of NewReader; the scan stays empty unless it
// errors.
type EmptyScanHooks interface {
	EmptyScan(proc *process.Process, spec *plan.IndexSearchScan, req Request) error
}

// ExplainHooks optionally describes the algorithm settings of a scan for
// EXPLAIN, as "Name: value" fragments in display order.
type ExplainHooks interface {
	ExplainSettings(spec *plan.IndexSearchScan) ([]string, error)
}

// CandidateBudgetHooks optionally sizes the candidate budget of a scan whose
// results are post-filtered (IndexSearchScan.PostFilterOverFetch). Without it
// the budget is overfetch.FilteredPostModeLimit(resultLimit).
type CandidateBudgetHooks interface {
	PostFilterCandidateBudget(resultLimit uint64) uint64
}

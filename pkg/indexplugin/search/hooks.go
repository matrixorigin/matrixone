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
}

// Request contains the fully bound state for one vector-search execution.
// The plan specification passed beside it is immutable catalog/index metadata;
// readers must not evaluate or mutate its dynamic expressions.
type Request struct {
	QueryVector      []byte
	QueryType        plan.Type
	ResultLimit      uint64
	CandidateBudget  uint64
	FirstRoundLimit  uint64
	HasFirstRound    bool
	PreFilters       []*plan.Expr
	DistanceRange    *plan.DistRange
	MembershipFilter []byte
	// HasMembershipFilter distinguishes an exact empty key set from the
	// absence of a runtime membership predicate (for example RF PASS).
	HasMembershipFilter bool
	Identity            ScanIdentity
}

// Hooks builds the reader for one vector-index scan execution generation.
// The returned reader owns all per-search child readers and must release them
// from Close on success, error, cancellation, and prepared-plan reuse.
type Hooks interface {
	NewReader(proc *process.Process, spec *plan.VectorIndexScan, req Request) (engine.Reader, error)
}

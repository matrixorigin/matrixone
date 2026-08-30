// Copyright 2026 Matrix Origin
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

// Package coverage answers one question: is an index's durable state current
// enough that the optimizer may use it as a MANDATORY filter?
//
// The distinction matters only for asynchronously maintained indexes. A search
// operator (MATCH, vector top-k) may return slightly stale results and still be
// doing its job. An index probe ANDed into an ordinary SQL predicate may not: a
// row written inside the maintenance lag satisfies the predicate but has no
// posting yet, so the probe removes it before the predicate is ever evaluated,
// and a strongly consistent query silently loses rows.
//
// This is an OPTIONAL capability, in the shape of SearchPlugin: an algorithm
// that cannot answer simply does not implement it, and the planner then treats
// it as "not covered" rather than every plugin carrying a no-op.
package coverage

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// Request identifies the index and the read snapshot to test against.
type Request struct {
	// CNUUID and Txn are how an implementation reaches the catalog; the check
	// runs inside the planning transaction so it observes a consistent state.
	CNUUID string
	Txn    client.TxnOperator

	// TableID is the BASE table the index is defined on, as the planner already
	// resolved it. It is passed rather than a name pair because the maintenance
	// catalogs live in the system tenant, where resolving a normal tenant's name
	// would silently find the wrong table (or nothing).
	TableID  uint64
	IndexDef *plan.IndexDef

	// Snapshot is the timestamp the query reads at. The index covers it only if
	// every base-table change visible at Snapshot is already indexed.
	Snapshot types.TS
}

// Hooks reports index freshness.
type Hooks interface {
	// CoversSnapshot reports whether the index's durable state reflects every
	// base-table change visible at req.Snapshot.
	//
	// It MUST FAIL CLOSED. Any uncertainty — no maintenance job, a paused or
	// failed one, an unreadable watermark, a lookup error — is false, not an
	// error the caller has to interpret. Returning true when the index is behind
	// produces wrong query results; returning false only forgoes an
	// optimization.
	CoversSnapshot(ctx context.Context, req Request) (bool, error)
}

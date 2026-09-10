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

// Package coverage answers one question: is the index view used by execution
// complete for the query, so the optimizer may use it as a MANDATORY filter?
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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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

	// Snapshot is the transaction read timestamp, retained for observability.
	Snapshot types.TS

	// SourceCommitTS is the greatest source DML commit or partition-state
	// retention boundary represented by the query CN at Snapshot. The index is
	// usable only after its watermark reaches this timestamp. It intentionally
	// excludes flush/merge lifecycle timestamps.
	SourceCommitTS types.TS

	// IndexStorageTable, IndexMetadataDB, and IndexMetadataTable are the index's
	// hidden tables, resolved by the planner. The freshness check reads the loaded
	// generation's build_ts from the cache keyed by IndexStorageTable, and on a cold
	// cache reads MAX(build_ts) from IndexMetadataDB.IndexMetadataTable (what a fresh
	// load would see). Empty when the planner could not resolve them.
	IndexStorageTable  string
	IndexMetadataDB    string
	IndexMetadataTable string

	// ScanSnapshotTS is the effective historical read TS for a {snapshot=...}/AS OF
	// query, or nil for a current read. When set, the freshness check targets the
	// snapshot-bound index generation (cache key index_table@snapshot) and reads the
	// metadata as of this TS, matching how the search loads a historical generation.
	ScanSnapshotTS *timestamp.Timestamp
}

// Hooks reports index freshness.
type Hooks interface {
	// CoversSnapshot reports whether the actual index view used by execution
	// contains every candidate visible to the query, including transaction-local
	// writes. The view must remain compatible with req.Snapshot when executed;
	// an independently refreshed cache or a later generation can lose postings
	// still visible to an older snapshot. Catalog progress alone is insufficient.
	//
	// It MUST FAIL CLOSED. Any uncertainty — no maintenance job, a paused or
	// failed one, an unreadable watermark, a lookup error — is false, not an
	// error the caller has to interpret. Returning true when the index is behind
	// produces wrong query results; returning false only forgoes an
	// optimization.
	//
	// It also returns the build_ts of the generation a probe would search (0 = unknown) --
	// the same value the coverage decision is derived from. When covered is false on a
	// current read, the planner uses this build_ts as the lower bound of a table_changes
	// tail (a partial plan) instead of a full scan; returning it here avoids a second
	// metadata read. build_ts is reported even when the index is not live (liveness gates
	// only the mandatory-filter decision, not the gap bound).
	CoversSnapshot(ctx context.Context, req Request) (covered bool, buildTS types.TS, err error)
}

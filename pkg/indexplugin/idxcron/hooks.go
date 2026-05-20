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

// Package idxcron defines the cron-side hook layer every index
// algorithm plugin implements. Mirrors the per-layer sub-package
// pattern (catalog/, compile/, plan/, iscp/): the interface lives
// here; each algorithm's implementation lives under
// pkg/<algopkg>/plugin/idxcron/.
//
// The hook gates the scheduled-rebuild path driven by
// pkg/vectorindex/idxcron/executor.go. The executor handles the
// universal pre-checks (auto_update, day, hour, createdAt + interval);
// the per-algo Updatable hook owns everything beyond — including the
// lastUpdateAt cadence, source-data-size gating, and any metadata
// mutation each algorithm needs.
package idxcron

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// UpdatableInput is what the executor hands each Updatable call.
// Struct (not positional args) so future fields can be added without
// churning every implementor.
//
// Fields are read-only with one exception: Metadata.Modify is allowed
// — IVF-FLAT's hook mutates kmeans_train_percent on every tick. Those
// edits ride along on the same txn the reindex SQL runs in, so the
// next ALTER ... REINDEX sees the new value.
type UpdatableInput struct {
	Sqlproc   *sqlexec.SqlProcess
	TableDef  *plan.TableDef
	IndexName string

	// Metadata is the per-task metadata blob captured at CREATE INDEX
	// time (e.g. ivf's kmeans_train_percent, cuvs's threads_build).
	// Hooks may consult it via ResolveVariableFunc and rewrite it via
	// Metadata.Modify. Nil if no metadata was captured for this task.
	Metadata *sqlexec.Metadata

	// CreatedAt is when the cron task was registered (CREATE INDEX
	// time). The executor already enforces createdAt + Interval >
	// now as a universal pre-hook check, so hooks generally don't
	// re-check.
	CreatedAt types.Timestamp

	// LastUpdateAt is the last time this reindex ran (nil before the
	// first run). Hooks use it for cadence checks beyond the
	// executor's universal one — e.g. IVF-FLAT case 3 needs
	// lastUpdateAt + 2*interval.
	LastUpdateAt *types.Timestamp

	// Interval is the rebuild cadence derived from indexAlgoParams
	// (day * 24h, defaulting to one week). Universal to all algos.
	Interval time.Duration
}

// Hooks is the per-algo idxcron hook layer. Implementations live
// under pkg/<algopkg>/plugin/idxcron/. Currently a single method —
// the contract may grow as more cron-side decisions move out of the
// executor.
type Hooks interface {
	// Updatable reports whether the cron-triggered reindex should
	// fire for the given (table, index). Called by the idxcron
	// executor AFTER its universal time-cadence checks pass
	// (auto_update on, currentHour matches, createdAt + interval
	// elapsed) but BEFORE the ALTER REINDEX SQL.
	//
	// Returns:
	//   - (true,  "",     nil) — proceed with rebuild
	//   - (false, reason, nil) — skip this tick; reason is logged
	//   - (_,     _,      err) — task error
	//
	// Each implementation owns its algorithm-specific gating —
	// CDC delta-record count for cuvs (CAGRA / IVF-PQ), source-size
	// nsample heuristic plus kmeans_train_percent mutation for
	// IVF-FLAT, trivial-true for HNSW / fulltext.
	Updatable(in UpdatableInput) (ok bool, reason string, err error)
}

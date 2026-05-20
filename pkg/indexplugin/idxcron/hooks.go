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
// universal time-cadence check (auto_update, day, hour, interval);
// the per-algo Updatable hook decides whether the rebuild is
// actually worth doing — typically by counting CDC delta records
// and comparing against an algorithm-specific minimum.
package idxcron

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// Hooks is the per-algo idxcron hook layer. Implementations live
// under pkg/<algopkg>/plugin/idxcron/. Currently a single method —
// the contract may grow as more cron-side decisions move out of the
// executor.
type Hooks interface {
	// Updatable reports whether the cron-triggered reindex should
	// fire for the given (table, index). Called by the idxcron
	// executor AFTER its time-cadence check passes (lastUpdateAt +
	// interval < now, auto_update on, currentHour matches) but
	// BEFORE the ALTER REINDEX SQL.
	//
	// Returns:
	//   - (true,  "",     nil) — proceed with rebuild
	//   - (false, reason, nil) — skip this tick; reason is logged
	//   - (_,     _,      err) — task error
	//
	// Implementations may query the storage table via sqlproc to
	// count CDC delta records / source-table size and compare against
	// algorithm-specific minimums (e.g. IVF-PQ: lists; CAGRA:
	// intermediate_graph_degree; IVF-FLAT: nlist / kmeans nsample
	// heuristic). HNSW / fulltext trivially return (true, "", nil) —
	// they have no minimum-size constraint.
	Updatable(sqlproc *sqlexec.SqlProcess, tableDef *plan.TableDef, indexName string) (ok bool, reason string, err error)
}

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

// Package idxcron is IVF-FLAT's idxcron hook implementation.
//
// IVF-FLAT's pre-rebuild logic (the lists / nsample heuristic, the
// dsize floor, the kmeans_train_percent runtime adjustment) currently
// lives in the executor's `(*IndexUpdateTaskInfo).checkIndexUpdatable`
// — that body needs `task.Metadata`, `LastUpdateAt`, and `interval`,
// none of which the Updatable hook signature surfaces. Migrating it
// would widen the contract enough that every other algorithm pays
// for IVF-FLAT-only state, so the migration is deferred.
//
// The hook here is a trivial pass-through: the executor's
// checkIndexUpdatable still runs for IVF-FLAT before this hook is
// reached, so the existing behaviour is preserved. CAGRA / IVF-PQ
// add real bodies under their own plugin/idxcron/ — for IVF-FLAT
// nothing further to do.
package idxcron

import (
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

type Hooks struct{}

var _ idxcronplugin.Hooks = Hooks{}

func (Hooks) Updatable(_ *sqlexec.SqlProcess, _ *plan.TableDef, _ string) (bool, string, error) {
	return true, "", nil
}

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

// Package idxcron is CAGRA's idxcron hook implementation. Delegates
// to the shared cuvs body in pkg/vectorindex/cuvs/idxcron — the
// per-algo piece is just the spec (which storage table + which
// params key holds the threshold).
package idxcron

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	cuvsidxcron "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs/idxcron"
)

type Hooks struct{}

var _ idxcronplugin.Hooks = Hooks{}

// Updatable gates CAGRA's cron-triggered rebuild on the cuvs minimum
// data size — intermediate_graph_degree, which is what cuvs CAGRA
// requires for a non-degenerate graph. Below that, brute-force
// search is the natural fallback and a rebuild would either fail or
// produce a graph too small to be useful.
func (Hooks) Updatable(in idxcronplugin.UpdatableInput) (bool, string, error) {
	return cuvsidxcron.CuvsUpdatable(in, cuvsidxcron.CuvsUpdatableSpec{
		StorageTableType: catalog.Cagra_TblType_Storage,
		ThresholdParam:   catalog.IntermediateGraphDegree,
	})
}

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

// Package idxcron is IVF-PQ's idxcron hook implementation. Mirrors
// CAGRA — delegates to the shared cuvs body, swapping in IVF-PQ's
// storage table type and threshold key (lists).
package idxcron

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	cuvsidxcron "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs/idxcron"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

type Hooks struct{}

var _ idxcronplugin.Hooks = Hooks{}

// Updatable gates IVF-PQ's cron-triggered rebuild on the cuvs k-means
// minimum — lists. With fewer points than lists, k-means can't form
// the cluster centroids, so the rebuild has nothing to train on.
// Brute-force search handles small-scale queries until the dataset
// crosses the threshold.
func (Hooks) Updatable(sqlproc *sqlexec.SqlProcess, tableDef *plan.TableDef, indexName string) (bool, string, error) {
	return cuvsidxcron.CuvsUpdatable(sqlproc, tableDef, indexName, cuvsidxcron.CuvsUpdatableSpec{
		StorageTableType: catalog.Ivfpq_TblType_Storage,
		ThresholdParam:   catalog.IndexAlgoParamLists,
	})
}

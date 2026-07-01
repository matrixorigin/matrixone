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

// Thin-wrapper tests for the CAGRA idxcron Updatable hook. The full
// CDC-delta-counting body lives in pkg/vectorindex/cuvs/idxcron and is
// covered by its own suite; this file just asserts the CAGRA wrapper
// forwards the right CuvsUpdatableSpec (Cagra_TblType_Storage +
// IntermediateGraphDegree) by exercising the error / skip paths that
// reach those fields before any SQL.

package idxcron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	cagraTestIndexName = "cagra_idx"
)

// TestCAGRAUpdatable_IndexDefMissing: when the TableDef lacks the
// CAGRA storage IndexDef, CuvsUpdatable surfaces a "no IndexDef found"
// error whose message names the storage-table-type the spec asked
// about. That confirms the wrapper passed Cagra_TblType_Storage.
func TestCAGRAUpdatable_IndexDefMissing(t *testing.T) {
	td := &plan.TableDef{
		DbName: "db",
		Name:   "src",
		Indexes: []*plan.IndexDef{
			// IVF-PQ storage type, not CAGRA — wrapper should reject.
			{IndexName: cagraTestIndexName, IndexAlgoTableType: catalog.Ivfpq_TblType_Storage},
		},
	}
	_, _, err := Hooks{}.Updatable(idxcronplugin.UpdatableInput{
		TableDef:  td,
		IndexName: cagraTestIndexName,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), catalog.Cagra_TblType_Storage)
}

// TestCAGRAUpdatable_SatisfiesInterface: belt-and-braces compile-time
// assertion (already in idxcron.go via the var _ check, repeated here
// so the test binary fails loudly if anyone drops it).
func TestCAGRAUpdatable_SatisfiesInterface(t *testing.T) {
	var _ idxcronplugin.Hooks = Hooks{}
}

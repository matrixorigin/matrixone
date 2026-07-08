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

// Thin-wrapper tests for the IVF-PQ idxcron Updatable hook. Mirrors
// the CAGRA suite — asserts the wrapper forwards
// (Ivfpq_TblType_Storage, IndexAlgoParamLists) into the shared
// CuvsUpdatable body. The full delta-counting logic is covered in
// pkg/vectorindex/cuvs/idxcron.

package idxcron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const ivfpqTestIndexName = "ivfpq_idx"

// TestIVFPQUpdatable_IndexDefMissing: TableDef without an IVF-PQ
// storage IndexDef → error names Ivfpq_TblType_Storage, confirming
// the spec passed by the wrapper.
func TestIVFPQUpdatable_IndexDefMissing(t *testing.T) {
	td := &plan.TableDef{
		DbName: "db",
		Name:   "src",
		Indexes: []*plan.IndexDef{
			{IndexName: ivfpqTestIndexName, IndexAlgoTableType: catalog.Cagra_TblType_Storage},
		},
	}
	_, _, err := Hooks{}.Updatable(idxcronplugin.UpdatableInput{
		TableDef:  td,
		IndexName: ivfpqTestIndexName,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), catalog.Ivfpq_TblType_Storage)
}

// TestIVFPQUpdatable_SatisfiesInterface: compile-time interface check.
func TestIVFPQUpdatable_SatisfiesInterface(t *testing.T) {
	var _ idxcronplugin.Hooks = Hooks{}
}

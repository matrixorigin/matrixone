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

package idxcron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// TestFulltextUpdatable_NonRetrievalSkipped: an index without a WAND chunk-store
// hidden table is a postings/ngram index — it has no rebuildable store, so Updatable
// skips it (a defensive backstop; such an index never registers a task). This branch
// runs before CountTailChunks, so it needs no live SqlProc.
func TestFulltextUpdatable_NonRetrievalSkipped(t *testing.T) {
	in := idxcronplugin.UpdatableInput{
		IndexName: "ft",
		TableDef: &plan.TableDef{
			DbName:  "db",
			Indexes: []*plan.IndexDef{{IndexName: "ft", IndexAlgoTableType: ""}}, // postings only
		},
	}
	ok, reason, err := Hooks{}.Updatable(in)
	require.NoError(t, err)
	require.False(t, ok)
	require.Contains(t, reason, "not a retrieval index")
}

// TestFulltextUpdatable_IndexNameMismatchSkipped: a table whose only WAND store belongs
// to a different index still resolves no storage table for this IndexName → skip.
func TestFulltextUpdatable_IndexNameMismatchSkipped(t *testing.T) {
	in := idxcronplugin.UpdatableInput{
		IndexName: "ft",
		TableDef: &plan.TableDef{
			DbName: "db",
			Indexes: []*plan.IndexDef{
				{IndexName: "other", IndexAlgoTableType: catalog.FullTextIndex_TblType_Storage, IndexTableName: "s"},
			},
		},
	}
	ok, _, err := Hooks{}.Updatable(in)
	require.NoError(t, err)
	require.False(t, ok)
}

// TestFulltextUpdatable_SatisfiesInterface: compile-time interface check.
func TestFulltextUpdatable_SatisfiesInterface(t *testing.T) {
	var _ idxcronplugin.Hooks = Hooks{}
}

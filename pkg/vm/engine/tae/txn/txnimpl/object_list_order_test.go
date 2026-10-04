// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func makeIncrementalObject(
	marker byte,
	appendable bool,
	createdAt, deletedAt int64,
) *catalog.ObjectEntry {
	var id objectio.ObjectId
	id[0] = marker
	entry := &catalog.ObjectEntry{
		EntryMVCCNode: catalog.EntryMVCCNode{
			CreatedAt: types.BuildTS(createdAt, 0),
		},
		ObjectMVCCNode: catalog.ObjectMVCCNode{
			ObjectStats: *objectio.NewObjectStatsWithObjectID(&id, appendable, false, false),
		},
	}
	if deletedAt != 0 {
		entry.DeletedAt = types.BuildTS(deletedAt, 0)
	}
	return entry
}

func TestForeachIncrementalObjectUsesGroupBounds(t *testing.T) {
	tree := btree.NewBTreeG((*catalog.ObjectEntry).Less)
	for _, entry := range []*catalog.ObjectEntry{
		makeIncrementalObject(1, true, 1, 0),
		makeIncrementalObject(2, true, 3, 0),
		makeIncrementalObject(3, true, 5, 0),
		makeIncrementalObject(4, true, 7, 0),
		makeIncrementalObject(8, false, 3, 0),
		makeIncrementalObject(9, false, 4, 0),
		makeIncrementalObject(10, false, 6, 0),
		makeIncrementalObject(11, false, 7, 0),
	} {
		tree.Set(entry)
	}

	it := tree.Iter()
	defer it.Release()
	var markers []byte
	err := foreachIncrementalObject(
		&it,
		types.BuildTS(4, 0),
		types.BuildTS(6, 0),
		func(entry *catalog.ObjectEntry) error {
			markers = append(markers, entry.ID()[0])
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []byte{2, 3, 9, 10}, markers)
}

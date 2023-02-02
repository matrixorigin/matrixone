// Copyright 2022 Matrix Origin
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

package disttae

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/tidwall/btree"
)

type TreeIndex struct {
	tree *btree.BTreeG[*IndexEntry]
}

type IndexEntry struct {
	Tuple Tuple
	RowID types.Rowid
	// this is required, because a row's value may change (wow!) and tuple may be different.
	// we need to validate the entry by checking the current version of row ref.
	RowRefID int64
}

func NewTreeIndex() *TreeIndex {
	return &TreeIndex{
		tree: btree.NewBTreeG(func(a, b *IndexEntry) bool {
			if a.Tuple.Less(b.Tuple) {
				return true
			} else if b.Tuple.Less(a.Tuple) {
				return false
			}
			if res := bytes.Compare(a.RowID[:], b.RowID[:]); res < 0 {
				return true
			} else if res > 0 {
				return false
			}
			if a.RowRefID < b.RowRefID {
				return true
			} else if b.RowRefID < a.RowRefID {
				return false
			}
			return false
		}),
	}
}

func (t *TreeIndex) Set(tuple Tuple, rowID types.Rowid, rowRefID int64) {
	t.tree.Set(&IndexEntry{
		Tuple:    tuple,
		RowID:    rowID,
		RowRefID: rowRefID,
	})
}

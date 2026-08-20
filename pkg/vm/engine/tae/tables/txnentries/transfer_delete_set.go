// Copyright 2021 Matrix Origin
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

package txnentries

import (
	"github.com/RoaringBitmap/roaring/v2"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// transferredDeleteSet keeps the source rowids already transferred by one
// transaction entry. Tombstone objects can be rewritten between the two
// transfer phases, so catalog-object ownership alone cannot prevent both
// generations from returning the same logical delete.
type transferredDeleteSet map[types.Blockid]*roaring.Bitmap

func (set transferredDeleteSet) contains(rowID types.Rowid) bool {
	blockID, rowOffset := rowID.Decode()
	rows := set[*blockID]
	return rows != nil && rows.Contains(rowOffset)
}

func (set transferredDeleteSet) add(rowID types.Rowid) {
	blockID, rowOffset := rowID.Decode()
	rows := set[*blockID]
	if rows == nil {
		rows = roaring.New()
		set[*blockID] = rows
	}
	rows.Add(rowOffset)
}

func (set transferredDeleteSet) merge(other transferredDeleteSet) {
	for blockID, rows := range other {
		if current := set[blockID]; current != nil {
			current.Or(rows)
		} else {
			set[blockID] = rows
		}
	}
}

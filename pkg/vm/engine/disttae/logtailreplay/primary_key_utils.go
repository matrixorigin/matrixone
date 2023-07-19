// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"bytes"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func (p *PartitionState) PrimaryKeysMayBeModified(
	from types.TS,
	to types.TS,
	keysVector *vector.Vector,
	packer *types.Packer,
) bool {
	packer.Reset()

	keys := EncodePrimaryKeyVector(keysVector, packer)
	for _, key := range keys {
		if p.PrimaryKeyMayBeModified(from, to, key) {
			return true
		}
	}

	return false
}

func (p *PartitionState) PrimaryKeyMayBeModified(
	from types.TS,
	to types.TS,
	key []byte,
) bool {

	p.shared.Lock()
	lastFlushTimestamp := p.shared.lastFlushTimestamp
	p.shared.Unlock()

	changed := true
	if !lastFlushTimestamp.IsEmpty() {
		if from.Greater(lastFlushTimestamp) {
			changed = false
		}
	} else {
		changed = false
	}
	if changed {
		return true
	}

	iter := p.primaryIndex.Copy().Iter()
	defer iter.Release()

	seek := false
	for {
		if !seek {
			seek = true
			if !iter.Seek(&PrimaryIndexEntry{
				Bytes: key,
			}) {
				return false
			}
		} else {
			if !iter.Next() {
				break
			}
		}

		entry := iter.Item()

		if !bytes.Equal(entry.Bytes, key) {
			break
		}

		if entry.Time.GreaterEq(from) {
			return true
		}

		// deleted entries are not indexed, check all rows for changes
		pivot := RowEntry{
			BlockID: entry.BlockID,
			RowID:   entry.RowID,
			Time:    types.BuildTS(math.MaxInt64, math.MaxUint32),
		}
		iter := p.rows.Copy().Iter()
		seek := false
		for {
			if !seek {
				seek = true
				if !iter.Seek(pivot) {
					break
				}
			} else {
				if !iter.Next() {
					break
				}
			}
			row := iter.Item()
			if row.BlockID.Compare(entry.BlockID) != 0 {
				break
			}
			if !row.RowID.Equal(entry.RowID) {
				break
			}
			if row.Time.GreaterEq(from) {
				iter.Release()
				return true
			}
		}
		iter.Release()

	}

	return false
}

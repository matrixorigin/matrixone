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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func (p *PartitionState) PrimaryKeysMayBeModified(
	tableID uint64,
	from types.TS,
	to types.TS,
	keysVector *vector.Vector,
	packer *types.Packer,
) bool {
	packer.Reset()

	keys := EncodePrimaryKeyVector(keysVector, packer)
	for _, key := range keys {
		if p.PrimaryKeyMayBeModified(tableID, from, to, key) {
			return true
		}
	}

	return false
}

func (p *PartitionState) PrimaryKeyMayBeModified(
	tableID uint64,
	from types.TS,
	to types.TS,
	key []byte,
) bool {
	iter := p.NewPrimaryKeyIter(to, Exact(key))
	defer iter.Close()

	empty := true
	for iter.Next() {
		if ts, ok := p.lastFlushTimestamp.Load(tableID); ok {
			if from.Greater(ts.(types.TS)) {
				empty = false
			}
		} else {
			empty = false
		}
	}
	return empty
}

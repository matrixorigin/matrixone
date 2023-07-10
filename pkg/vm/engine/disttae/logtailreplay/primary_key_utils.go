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
	iter := p.NewPrimaryKeyIter(to, Exact(key))
	defer iter.Close()

	p.shared.Lock()
	lastFlushTimestamp := p.shared.lastFlushTimestamp
	p.shared.Unlock()

	empty := true
	if !lastFlushTimestamp.IsEmpty() {
		if from.Greater(lastFlushTimestamp) {
			empty = false
		}
	} else {
		empty = false
	}
	for iter.Next() {
		empty = false
		row := iter.Entry()
		if row.Time.Greater(from) {
			return true
		}
	}
	return empty
}

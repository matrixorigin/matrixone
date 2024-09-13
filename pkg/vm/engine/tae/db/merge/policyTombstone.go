// Copyright 2024 Matrix Origin
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

package merge

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

type tombstonePolicy struct {
	tombstones []*catalog.ObjectEntry
}

func (t *tombstonePolicy) onObject(entry *catalog.ObjectEntry, config *BasicPolicyConfig) bool {
	if len(t.tombstones) == config.MergeMaxOneRun {
		return false
	}
	if entry.IsTombstone {
		t.tombstones = append(t.tombstones, entry)
		return true
	}
	return false
}

func (t *tombstonePolicy) revise(cpu, mem int64, config *BasicPolicyConfig) []reviseResult {
	if len(t.tombstones) == 0 {
		return nil
	}
	if len(t.tombstones) == 1 {
		createdAt := t.tombstones[0].CreatedAt.Physical()
		if time.Unix(0, createdAt).Add(config.TombstoneLifetime).After(time.Now()) {
			return nil
		}
	}
	return []reviseResult{{t.tombstones, TaskHostDN}}
}

func (t *tombstonePolicy) resetForTable(*catalog.TableEntry) {
	t.tombstones = t.tombstones[:0]
}

func newTombstonePolicy() policy {
	return &tombstonePolicy{
		tombstones: make([]*catalog.ObjectEntry, 0),
	}
}

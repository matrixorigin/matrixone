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

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"

type tombstonePolicy struct {
	tombstones []*catalog.ObjectEntry
}

func newTombstonePolicy() *tombstonePolicy {
	return &tombstonePolicy{
		tombstones: make([]*catalog.ObjectEntry, 0),
	}
}

func (m *tombstonePolicy) onObject(obj *catalog.ObjectEntry) {
	if obj.IsTombstone {
		m.tombstones = append(m.tombstones, obj)
	}
}

func (m *tombstonePolicy) revise(int64, int64, *BasicPolicyConfig) ([]*catalog.ObjectEntry, TaskHostKind) {
	if len(m.tombstones) > 1 {
		return m.tombstones, TaskHostDN
	}
	return nil, TaskHostDN
}

func (m *tombstonePolicy) resetForTable(*catalog.TableEntry) {
	m.tombstones = m.tombstones[:0]
}

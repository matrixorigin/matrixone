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
	"cmp"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

type objSizePolicy struct {
	objects []*catalog.ObjectEntry

	accBuf []int
}

func newObjSizePolicy() *objSizePolicy {
	return &objSizePolicy{
		objects: make([]*catalog.ObjectEntry, 0),
		accBuf:  make([]int, 1),
	}
}

func (m *objSizePolicy) onObject(obj *catalog.ObjectEntry) {
	if !obj.IsTombstone {
		m.objects = append(m.objects, obj)
	}
}

func (m *objSizePolicy) revise(cpu, mem int64, config *BasicPolicyConfig) ([]*catalog.ObjectEntry, TaskHostKind) {
	if len(m.objects) < 2 {
		return nil, TaskHostDN
	}
	objs, taskHostKind := m.reviseDataObjs(config)
	if len(objs) > 1 {
		return objs, taskHostKind
	}
	return nil, taskHostKind
}

func (m *objSizePolicy) reviseDataObjs(config *BasicPolicyConfig) ([]*catalog.ObjectEntry, TaskHostKind) {
	slices.SortFunc(m.objects, func(a, b *catalog.ObjectEntry) int {
		return cmp.Compare(a.Rows(), b.Rows())
	})

	n := 0
	for _, obj := range m.objects {
		if obj.OriginSize() < config.ObjectMinOsize {
			n++
		}
	}
	if n < 5 {
		return nil, TaskHostDN
	}
	size := min(config.MergeMaxOneRun, n)

	revisedObj := make([]*catalog.ObjectEntry, 0, size)
	for _, obj := range m.objects {
		if len(revisedObj) == size {
			break
		}
		if obj.OriginSize() < config.ObjectMinOsize {
			revisedObj = append(revisedObj, obj)
		}
	}
	revisedObj = m.optimize(revisedObj)
	return revisedObj, TaskHostDN
}

func (m *objSizePolicy) resetForTable(*catalog.TableEntry) {
	m.objects = m.objects[:0]
}

func (m *objSizePolicy) optimize(objs []*catalog.ObjectEntry) []*catalog.ObjectEntry {
	// objs are sorted by remaining rows
	m.accBuf = m.accBuf[:1]
	for i, obj := range objs {
		m.accBuf = append(m.accBuf, m.accBuf[i]+obj.GetRows())
	}
	acc := m.accBuf

	isBigGap := func(small, big int) bool {
		if big < int(options.DefaultBlockMaxRows) {
			return false
		}
		return big-small > 3*small
	}

	var i int
	// skip merging objects with big row count gaps, 3x and more
	for i = len(acc) - 1; i > 1 && isBigGap(acc[i-1], acc[i]); i-- {
	}

	objs = objs[:i]

	return objs
}

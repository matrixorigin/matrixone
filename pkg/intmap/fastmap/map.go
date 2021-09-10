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

package fastmap

import (
	"sync"
)

var Pool = sync.Pool{
	New: func() interface{} {
		return New()
	},
}

func New() *Map {
	vs := make([][]int, Group)
	ks := make([][]uint64, Group)
	for i := 0; i < Group; i++ {
		vs[i] = make([]int, 0, 16)
		ks[i] = make([]uint64, 0, 16)
	}
	return &Map{Ks: ks, Vs: vs}
}

func (m *Map) Reset() {
	for i := 0; i < Group; i++ {
		m.Ks[i] = m.Ks[i][:0]
		m.Vs[i] = m.Vs[i][:0]
	}
}

func (m *Map) Set(k uint64, v int) {
	slot := k & GroupMask
	m.Vs[slot] = append(m.Vs[slot], v)
	m.Ks[slot] = append(m.Ks[slot], k)
	return
}

func (m *Map) Get(k uint64) (int, bool) {
	slot := k & GroupMask
	if i := Find(m.Ks[slot], k); i != -1 {
		return m.Vs[slot][i], true
	}
	return -1, false
}

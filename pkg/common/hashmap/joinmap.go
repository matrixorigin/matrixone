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

package hashmap

import (
	"sync/atomic"
)

func NewJoinMap(sels [][]int32, ihm *IntHashMap, shm *StrHashMap) *JoinMap {
	return &JoinMap{
		refCnt:    0,
		shm:       shm,
		ihm:       ihm,
		multiSels: sels,
		valid:     true,
	}
}

func (jm *JoinMap) SetRowCount(cnt int64) {
	jm.rowcnt = cnt
}

func (jm *JoinMap) GetRowCount() int64 {
	return jm.rowcnt
}

func (jm *JoinMap) SetPushedRuntimeFilterIn(b bool) {
	jm.runtimeFilter_In = b
}

func (jm *JoinMap) PushedRuntimeFilterIn() bool {
	return jm.runtimeFilter_In
}

func (jm *JoinMap) Sels() [][]int32 {
	return jm.multiSels
}

func (jm *JoinMap) NewIterator() Iterator {
	if jm.shm == nil {
		return &intHashMapIterator{
			mp:      jm.ihm,
			m:       jm.ihm.m,
			keys:    make([]uint64, UnitLimit),
			keyOffs: make([]uint32, UnitLimit),
			values:  make([]uint64, UnitLimit),
			zValues: make([]int64, UnitLimit),
			hashes:  make([]uint64, UnitLimit),
		}
	} else {
		return &strHashmapIterator{
			mp:            jm.shm,
			m:             jm.shm.m,
			values:        make([]uint64, UnitLimit),
			zValues:       make([]int64, UnitLimit),
			keys:          make([][]byte, UnitLimit),
			strHashStates: make([][3]uint64, UnitLimit),
		}
	}
}

func (jm *JoinMap) IncRef(cnt int32) {
	atomic.AddInt64(&jm.refCnt, int64(cnt))
}

func (jm *JoinMap) IsValid() bool {
	return jm.valid
}

func (jm *JoinMap) Free() {
	if atomic.AddInt64(&jm.refCnt, -1) != 0 {
		return
	}
	for i := range jm.multiSels {
		jm.multiSels[i] = nil
	}
	jm.multiSels = nil
	if jm.ihm != nil {
		jm.ihm.Free()
	} else {
		jm.shm.Free()
	}
	jm.valid = false
}

func (jm *JoinMap) Size() int64 {
	// TODO: add the size of the other JoinMap parts
	if jm.ihm == nil && jm.shm == nil {
		return 0
	}
	if jm.ihm != nil {
		return jm.ihm.Size()
	} else {
		return jm.shm.Size()
	}
}

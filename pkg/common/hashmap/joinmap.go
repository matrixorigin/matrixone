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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func NewJoinMap(sels [][]int32, expr *plan.Expr, ihm *IntHashMap, shm *StrHashMap, hasNull bool, isDup bool) *JoinMap {
	cnt := int64(1)
	return &JoinMap{
		cnt:       &cnt,
		shm:       shm,
		ihm:       ihm,
		expr:      expr,
		multiSels: sels,
		hasNull:   hasNull,
		dupCnt:    new(int64),
		isDup:     isDup,
	}
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

func (jm *JoinMap) Expr() *plan.Expr {
	return jm.expr
}

func (jm *JoinMap) HasNull() bool {
	return jm.hasNull
}

func (jm *JoinMap) IsDup() bool {
	return jm.isDup
}

func (jm *JoinMap) NewIterator() Iterator {
	if jm.shm == nil {
		return &intHashMapIterator{
			mp:      jm.ihm,
			m:       jm.ihm.m,
			ibucket: jm.ihm.ibucket,
			nbucket: jm.ihm.nbucket,
		}
	} else {
		return &strHashmapIterator{
			mp:      jm.shm,
			m:       jm.shm.m,
			ibucket: jm.shm.ibucket,
			nbucket: jm.shm.nbucket,
		}
	}
}

func (jm *JoinMap) Dup() *JoinMap {
	if jm.shm == nil {
		m0 := &IntHashMap{
			m:       jm.ihm.m,
			hashMap: jm.ihm.hashMap,
			hasNull: jm.ihm.hasNull,
			ibucket: jm.ihm.ibucket,
			nbucket: jm.ihm.nbucket,
			keys:    make([]uint64, UnitLimit),
			keyOffs: make([]uint32, UnitLimit),
			values:  make([]uint64, UnitLimit),
			zValues: make([]int64, UnitLimit),
			hashes:  make([]uint64, UnitLimit),
		}
		jm0 := &JoinMap{
			ihm:       m0,
			expr:      jm.expr,
			multiSels: jm.multiSels,
			hasNull:   jm.hasNull,
			cnt:       jm.cnt,
		}
		if atomic.AddInt64(jm.dupCnt, -1) == 0 {
			jm.ihm = nil
			jm.multiSels = nil
		}
		return jm0
	} else {
		m0 := &StrHashMap{
			m:             jm.shm.m,
			hashMap:       jm.shm.hashMap,
			hasNull:       jm.shm.hasNull,
			ibucket:       jm.shm.ibucket,
			nbucket:       jm.shm.nbucket,
			values:        make([]uint64, UnitLimit),
			zValues:       make([]int64, UnitLimit),
			keys:          make([][]byte, UnitLimit),
			strHashStates: make([][3]uint64, UnitLimit),
		}
		jm0 := &JoinMap{
			shm:       m0,
			expr:      jm.expr,
			multiSels: jm.multiSels,
			hasNull:   jm.hasNull,
			cnt:       jm.cnt,
		}
		if atomic.AddInt64(jm.dupCnt, -1) == 0 {
			jm.shm = nil
			jm.multiSels = nil
		}
		return jm0
	}
}

func (jm *JoinMap) IncRef(ref int64) {
	atomic.AddInt64(jm.cnt, ref)
}

func (jm *JoinMap) SetDupCount(ref int64) {
	atomic.AddInt64(jm.dupCnt, ref)
}

func (jm *JoinMap) Free() {
	if atomic.AddInt64(jm.cnt, -1) != 0 {
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

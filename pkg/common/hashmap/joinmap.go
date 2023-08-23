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

func NewJoinMap(sels [][]int32, expr *plan.Expr, mp any, hasNull bool, isDup bool) *JoinMap {
	cnt := int64(1)
	return &JoinMap{
		cnt:     &cnt,
		mp:      mp,
		expr:    expr,
		sels:    sels,
		hasNull: hasNull,
		dupCnt:  new(int64),
		isDup:   isDup,
	}
}

func (jm *JoinMap) Sels() [][]int32 {
	return jm.sels
}

func (jm *JoinMap) Map() any {
	return jm.mp
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
	switch m := jm.mp.(type) {
	case *IntHashMap:
		return &intHashMapIterator{
			mp:      m,
			m:       m.m,
			ibucket: m.ibucket,
			nbucket: m.nbucket,
		}
	case *StrHashMap:
		return &strHashmapIterator{
			mp:      m,
			m:       m.m,
			ibucket: m.ibucket,
			nbucket: m.nbucket,
		}
	default:
		panic("wrong join map!")
	}
}

func (jm *JoinMap) Dup() *JoinMap {
	switch m := jm.mp.(type) {
	case *IntHashMap:
		m0 := &IntHashMap{
			m:       m.m,
			hashMap: m.hashMap,
			hasNull: m.hasNull,
			ibucket: m.ibucket,
			nbucket: m.nbucket,
			keys:    make([]uint64, UnitLimit),
			keyOffs: make([]uint32, UnitLimit),
			values:  make([]uint64, UnitLimit),
			zValues: make([]int64, UnitLimit),
			hashes:  make([]uint64, UnitLimit),
		}
		jm0 := &JoinMap{
			mp:      m0,
			expr:    jm.expr,
			sels:    jm.sels,
			hasNull: jm.hasNull,
			cnt:     jm.cnt,
		}
		if atomic.AddInt64(jm.dupCnt, -1) == 0 {
			jm.mp = nil
			jm.sels = nil
		}
		return jm0
	case *StrHashMap:
		m0 := &StrHashMap{
			m:             m.m,
			hashMap:       m.hashMap,
			hasNull:       m.hasNull,
			ibucket:       m.ibucket,
			nbucket:       m.nbucket,
			values:        make([]uint64, UnitLimit),
			zValues:       make([]int64, UnitLimit),
			keys:          make([][]byte, UnitLimit),
			strHashStates: make([][3]uint64, UnitLimit),
		}
		jm0 := &JoinMap{
			mp:      m0,
			expr:    jm.expr,
			sels:    jm.sels,
			hasNull: jm.hasNull,
			cnt:     jm.cnt,
		}
		if atomic.AddInt64(jm.dupCnt, -1) == 0 {
			jm.mp = nil
			jm.sels = nil
		}
		return jm0
	default:
		panic("wrong join map!")
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
	for i := range jm.sels {
		jm.sels[i] = nil
	}
	jm.sels = nil
	switch m := jm.mp.(type) {
	case *IntHashMap:
		m.Free()
	case *StrHashMap:
		m.Free()
	default:
		panic("wrong join map!")
	}
}

func (jm *JoinMap) Size() int64 {
	// TODO: add the size of the other JoinMap parts
	if jm.mp == nil {
		return 0
	}
	switch m := jm.mp.(type) {
	case *IntHashMap:
		return m.Size()
	case *StrHashMap:
		return m.Size()
	default:
		panic("wrong join map!")
	}
}

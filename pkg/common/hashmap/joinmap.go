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

func NewJoinMap(sels [][]int32, expr *plan.Expr, ihm *IntHashMap, shm *StrHashMap, hasNull bool) *JoinMap {
	return &JoinMap{
		refCnt:    0,
		shm:       shm,
		ihm:       ihm,
		expr:      expr,
		multiSels: sels,
		hasNull:   hasNull,
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

func (jm *JoinMap) NewIterator() Iterator {
	if jm.shm == nil {
		return &intHashMapIterator{
			mp: jm.ihm,
			m:  jm.ihm.m,
		}
	} else {
		return &strHashmapIterator{
			mp: jm.shm,
			m:  jm.shm.m,
		}
	}
}

func (jm *JoinMap) IncRef() {
	atomic.AddInt64(&jm.refCnt, 1)
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

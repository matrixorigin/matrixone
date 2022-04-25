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

package checkpoint

import (
	"errors"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
)

var (
	ErrNotFoundNode = errors.New("not found node")
)

type dbCheckpointer struct {
	mu    *sync.RWMutex
	id    uint64
	uints map[uint64]data.CheckpointUnit
}

func newDBCheckpointer(id uint64) *dbCheckpointer {
	return &dbCheckpointer{
		id: id,
		mu: new(sync.RWMutex),
	}
}

func (ck *dbCheckpointer) addUnits(units map[uint64]data.CheckpointUnit) {
	newUnits := make(map[uint64]data.CheckpointUnit)
	ck.mu.RLock()
	for _, unit := range units {
		if ck.uints[unit.GetID()] == nil {
			newUnits[unit.GetID()] = unit
		}
	}
	ck.mu.RUnlock()
	if len(newUnits) == 0 {
		return
	}
	ck.mu.Lock()
	defer ck.mu.Unlock()
	for _, unit := range newUnits {
		ck.uints[unit.GetID()] = unit
	}
}

func (ck *dbCheckpointer) deleteUnit(id uint64) (err error) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	unit := ck.uints[id]
	if unit == nil {
		err = ErrNotFoundNode
		return
	}
	return
}

func (ck *dbCheckpointer) doCheckpoint() {
	units := ck.getUnits()
	for _, unit := range units {
		unit.TryCheckpoint(0)
	}
}

func (ck *dbCheckpointer) getUnits() map[uint64]data.CheckpointUnit {
	ck.mu.RLock()
	defer ck.mu.RUnlock()
	cloned := make(map[uint64]data.CheckpointUnit)
	for k, v := range ck.uints {
		cloned[k] = v
	}
	return cloned
}

func (ck *dbCheckpointer) String() string {
	ck.mu.RLock()
	defer ck.mu.RUnlock()
	str := fmt.Sprintf("DBCheckpointer<%d>:{", ck.id)
	for k, _ := range ck.uints {
		str = fmt.Sprintf("%s %d", str, k)
	}
	str = fmt.Sprintf("%s }", str)
	return str
}

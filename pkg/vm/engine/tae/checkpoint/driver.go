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
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

type changeRequests struct {
	dbId  uint64
	units map[uint64]data.CheckpointUnit
}

func newChangeRequests(id uint64) *changeRequests {
	return &changeRequests{
		dbId:  id,
		units: make(map[uint64]data.CheckpointUnit),
	}
}

func (reqs *changeRequests) addUnit(unit data.CheckpointUnit) {
	reqs.units[unit.GetID()] = unit
}

type changeRequest struct {
	dbId uint64
	unit data.CheckpointUnit
}

type driver struct {
	common.ClosedState
	sm.StateMachine
	mu   sync.RWMutex
	dbs  map[uint64]*dbCheckpointer
	mask *roaring64.Bitmap
}

func NewDriver() *driver {
	f := &driver{
		mask: roaring64.New(),
	}
	wg := new(sync.WaitGroup)
	// rqueue := sm.NewWaitableQueue(10000, 100, f, wg, nil, nil, f.onRequests)
	rqueue := sm.NewSafeQueue(10000, 100, f.onRequests)
	// TODO: flushQueue should be non-blocking
	// wqueue := sm.NewWaitableQueue(20000, 1000, f, wg, nil, nil, f.onCheckpoint)
	wqueue := sm.NewSafeQueue(20000, 1000, f.onCheckpoint)
	f.StateMachine = sm.NewStateMachine(wg, f, rqueue, wqueue)
	return f
}

func (f *driver) String() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	str := fmt.Sprintf("driver<cnt=%d>{", len(f.dbs))
	for _, sf := range f.dbs {
		str = fmt.Sprintf("%s\n%s", str, sf.String())
	}
	if len(f.dbs) > 0 {
		str = fmt.Sprintf("%s\n}", str)
	} else {
		str = fmt.Sprintf("%s}", str)
	}
	return str
}

func (f *driver) onCheckpoint(items ...interface{}) {
	defer f.mask.Clear()
	for _, item := range items {
		mask := item.(*roaring64.Bitmap)
		f.mask.Or(mask)
	}
	it := f.mask.Iterator()
	for it.HasNext() {
		dbId := it.Next()
		s := f.getOrAddCheckpointer(dbId)
		if s == nil {
			continue
		}
		s.doCheckpoint()
	}
}

func (f *driver) onRequests(items ...interface{}) {
	changes := make(map[uint64]*changeRequests)
	for _, item := range items {
		switch req := item.(type) {
		case *changeRequest:
			requests := changes[req.dbId]
			if requests == nil {
				requests = newChangeRequests(req.dbId)
				changes[req.dbId] = requests
			}
			requests.addUnit(req.unit)
		}
	}
	if len(changes) != 0 {
		for _, req := range changes {
			f.onChangeRequest(req)
		}
	}
}

func (f *driver) OnStats(stats interface{}) {
	err, _ := f.EnqueueRecevied(stats)
	if err != nil {
		logutil.Warnf("%v", err)
	}
}

func (f *driver) DBCnt() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.dbs)
}

func (f *driver) OnChange(dbId uint64, unit data.CheckpointUnit) {
	ctx := &changeRequest{dbId: dbId, unit: unit}
	_, err := f.EnqueueRecevied(ctx)
	if err != nil {
		logutil.Warnf("%v", err)
	}
}

func (f *driver) onChangeRequest(req *changeRequests) {
	ckp := f.getOrAddCheckpointer(req.dbId)
	ckp.addUnits(req.units)
}

func (f *driver) getOrAddCheckpointer(id uint64) *dbCheckpointer {
	f.mu.RLock()
	ckp := f.dbs[id]
	f.mu.RUnlock()
	if ckp != nil {
		return ckp
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ckp = newDBCheckpointer(id)
	f.dbs[id] = ckp
	return ckp
}

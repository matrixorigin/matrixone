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

package shard

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"

	"github.com/RoaringBitmap/roaring/roaring64"
)

// A offset recorder for (IndexId.Id + IndexId.Offset) level
type sliceEntry struct {
	idx  *SliceIndex
	mask *roaring64.Bitmap // a set of SliceInfo.Offset
}

func newSliceEntry(idx *SliceIndex) *sliceEntry {
	return &sliceEntry{
		idx:  idx,
		mask: roaring64.NewBitmap(),
	}
}

// Committed returns true if all SliceInfo.Offset of a SliceIndex have been seen.
func (s *sliceEntry) Committed() bool {
	return uint32(s.mask.GetCardinality()) == s.idx.Info.Size
}

func (s *sliceEntry) Commit(offset uint32) {
	s.mask.Add(uint64(offset))
}

func (s *sliceEntry) Repr() string {
	return s.idx.Info.Repr()
}

func (s *sliceEntry) String() string {
	if s.Committed() {
		return fmt.Sprintf("%s-{C}", s.Repr())
	}
	return fmt.Sprintf("%d-%s", s.idx.Id.Offset, s.mask.String())
}

// A offset recorder for (IndexId.Id) level
type commitEntry struct {
	idx    *SliceIndex
	mask   *roaring64.Bitmap      // a set of IndexId.Offset
	slices map[uint32]*sliceEntry // IndexId.Offset -> sliceEntry.
}

func (n *commitEntry) Repr() string {
	return n.idx.Id.String()
}

func (n *commitEntry) String() string {
	if n.Committed() {
		return fmt.Sprintf("%s-{C}", n.Repr())
	}
	s := fmt.Sprintf("%s-%s", n.Repr(), n.mask.String())
	for _, slice := range n.slices {
		s = fmt.Sprintf("%s\n\t%s", s, slice.String())
	}
	return s
}

func (n *commitEntry) getSliceEntryFor(idx *SliceIndex) *sliceEntry {
	if n.slices == nil {
		n.slices = make(map[uint32]*sliceEntry)
	}
	slice := n.slices[idx.Id.Offset]
	if slice == nil {
		slice = newSliceEntry(idx)
		n.slices[idx.Id.Offset] = slice
	}
	return slice
}

func (n *commitEntry) Commit(idx *SliceIndex) {
	if idx.IsSlice() {
		n.getSliceEntryFor(idx).Commit(idx.Info.Offset)
	} else {
		n.mask.Add(uint64(idx.Id.Offset))
	}
}

// Committed returns true if all IndexId.Offset have been committed
func (n *commitEntry) Committed() bool {
	if uint32(n.mask.GetCardinality()) == n.idx.Id.Size {
		return true
	}
	updated := false
	for offset, slice := range n.slices {
		if slice.Committed() {
			n.mask.Add(uint64(offset))
			updated = true
		}
	}
	if updated {
		return uint32(n.mask.GetCardinality()) == n.idx.Id.Size
	}
	return false
}

func (n *commitEntry) GetId() uint64 {
	return n.idx.Id.Id
}

func newCommitEntry(idx *SliceIndex) *commitEntry {
	n := &commitEntry{
		idx:  idx,
		mask: roaring64.NewBitmap(),
	}
	return n
}

type proxy struct {
	logmu      sync.RWMutex
	alumu      sync.RWMutex
	id         uint64
	mgr        *manager
	mask       *roaring64.Bitmap
	stopmask   *roaring64.Bitmap
	batches    []*SliceIndice
	lastIndex  uint64
	safeId     uint64
	lastSafeId uint64
	indice     map[uint64]*commitEntry
	idAlloctor *common.IDAlloctor
}

func newProxy(id uint64, mgr *manager) *proxy {
	p := &proxy{
		id:       id,
		mgr:      mgr,
		mask:     roaring64.New(),
		stopmask: roaring64.New(),
		batches:  make([]*SliceIndice, 0, 10),
		indice:   make(map[uint64]*commitEntry),
	}
	if mgr != nil && mgr.GetRole() == wal.HolderRole {
		p.idAlloctor = new(common.IDAlloctor)
	}
	return p
}

func (p *proxy) GetPendingEntries() uint64 {
	p.logmu.RLock()
	defer p.logmu.RUnlock()
	return p.mask.GetCardinality()
}

func (p *proxy) String() string {
	p.logmu.RLock()
	defer p.logmu.RUnlock()
	s := fmt.Sprintf("Shard<%d>[SafeId=%d](%s)", p.id, p.GetSafeId(), p.mask.String())
	return s
}

func (p *proxy) GetId() uint64 {
	return p.id
}

func (p *proxy) LogIndice(indice ...*Index) {
	p.logmu.Lock()
	for _, index := range indice {
		p.logIndexLocked(index)
	}
	p.logmu.Unlock()
}

func (p *proxy) logIndexLocked(index *Index) {
	p.mask.Add(index.Id.Id)
	if index.Id.Id > p.lastIndex+uint64(1) {
		p.stopmask.AddRange(p.lastIndex+uint64(1), index.Id.Id)
	} else if index.Id.Id < p.lastIndex {
		panic(fmt.Sprintf("logic error: S-%v lastIndex-%d, currIndex-%d", index.ShardId, p.lastIndex, index.Id.Id))
	}
	p.lastIndex = index.Id.Id
}

func (p *proxy) InitSafeId(id uint64) {
	if p.idAlloctor != nil {
		p.idAlloctor.SetStart(id)
	}
	p.SetSafeId(id)
	p.lastIndex = id
}

func (p *proxy) GetLastId() uint64 {
	p.logmu.RLock()
	defer p.logmu.RUnlock()
	return p.lastIndex
}

// LogIndex will add the index to the pending set until Checkpoint is called
func (p *proxy) LogIndex(index *Index) {
	if p.idAlloctor != nil {
		index.Id.Id = p.idAlloctor.Alloc()
	}
	// if index.Id.Id == uint64(0) {
	// 	panic("logic error")
	// }
	p.logmu.Lock()
	p.logIndexLocked(index)
	p.logmu.Unlock()
}

func (p *proxy) AppendIndex(index *Index) {
	bat := NewSimpleBatchIndice(index)
	p.AppendBatchIndice(bat)
}

func (p *proxy) AppendBatchIndice(bat *SliceIndice) {
	// logutil.Infof("[WAL] Append: %s", bat.String())
	p.alumu.Lock()
	defer p.alumu.Unlock()
	p.batches = append(p.batches, bat)
}

// Checkpoint tries to check all committed indices and increase safe_id
func (p *proxy) Checkpoint() {
	now := time.Now()
	p.alumu.Lock()
	bats := p.batches
	p.batches = make([]*SliceIndice, 0, 100)
	p.alumu.Unlock()

	mask := roaring64.NewBitmap()
	for _, bat := range bats {
		for _, idx := range bat.indice {
			if !idx.IsSlice() && idx.Id.IsSingle() {
				mask.Add(idx.Id.Id)
				continue
			}
			node := p.indice[idx.Id.Id]
			if node == nil {
				node = newCommitEntry(idx)
				p.indice[idx.Id.Id] = node
			}
			node.Commit(idx)
		}
	}
	deletes := make([]uint64, 0, 10)
	for i, node := range p.indice {
		if node.Committed() {
			deletes = append(deletes, i)
			mask.Add(node.GetId())
		}
		// logutil.Info(node.String())
	}
	for _, id := range deletes {
		delete(p.indice, id)
	}

	p.logmu.Lock()
	p.mask.Xor(mask)
	// logutil.Infof("p.mask is %s", p.mask.String())
	// logutil.Infof("p.stopmask is %s", p.stopmask.String())
	maskNum := p.mask.GetCardinality()
	stopNum := p.stopmask.GetCardinality()
	if maskNum == 0 {
		if stopNum != 0 {
			p.stopmask.Clear()
		}
		p.SetSafeId(p.lastIndex)
	} else if stopNum > 0 {
		it := p.mask.Iterator()
		start := it.Next()
		for pos := start - 1; pos >= uint64(0); pos-- {
			if !p.stopmask.Contains(pos) {
				p.SetSafeId(pos)
				break
			}
		}
		p.stopmask.RemoveRange(uint64(0), p.GetSafeId())
	} else {
		it := p.mask.Iterator()
		pos := it.Next()
		if pos == 0 {
			p.SetSafeId(pos)
		} else {
			p.SetSafeId(pos - 1)
		}
	}
	p.logmu.Unlock()
	id := p.GetSafeId()
	if p.mgr != nil && p.mgr.driver != nil && id != p.lastSafeId {
		logutil.Infof("Shard-%d | pending-%d, safeid-%d, lastsafeid-%d | %s", p.id, maskNum, id, p.lastSafeId, time.Since(now))
		if id < p.lastSafeId {
			panic("logic error")
		}
		safeId := SafeId{
			Id:      id,
			ShardId: p.id,
		}
		logEntry := SafeIdToEntry(safeId)
		if err := p.mgr.driver.AppendEntry(logEntry); err != nil {
			panic(err)
		}
		err := logEntry.WaitDone()
		if err != nil {
			panic(err)
		}
		logEntry.Free()
		p.lastSafeId = id
	} else {
		logutil.Infof("Shard-%d: pending-%d, safeid-%d %s", p.id, maskNum, id, time.Since(now))
	}
	if p.mgr != nil {
		p.mgr.UpdateSafeId(p.id, id)
	}
}

func (p *proxy) SetSafeId(id uint64) {
	atomic.StoreUint64(&p.safeId, id)
}

func (p *proxy) GetSafeId() uint64 {
	return atomic.LoadUint64(&p.safeId)
}

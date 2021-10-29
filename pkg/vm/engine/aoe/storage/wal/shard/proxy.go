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

type commitEntry struct {
	id   *IndexId
	mask *roaring64.Bitmap
}

func (n *commitEntry) Committed() bool {
	return uint32(n.mask.GetCardinality()) == n.id.Size
}

func (n *commitEntry) GetId() uint64 {
	return n.id.Id
}

func newCommitEntry(id *IndexId) *commitEntry {
	n := &commitEntry{
		id:   id,
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
	snippets   []*snippets
	snipIdx    map[uint64]int
	lastIndex  uint64
	safeId     uint64
	lastSafeId uint64
	pending    uint64
	indice     map[uint64]*commitEntry
	idAlloctor *common.IdAlloctor
}

func newProxy(id uint64, mgr *manager) *proxy {
	p := &proxy{
		id:       id,
		mgr:      mgr,
		mask:     roaring64.New(),
		stopmask: roaring64.New(),
		snipIdx:  make(map[uint64]int),
		snippets: make([]*snippets, 0, 10),
		indice:   make(map[uint64]*commitEntry),
	}
	if mgr != nil && mgr.GetRole() == wal.HolderRole {
		p.idAlloctor = new(common.IdAlloctor)
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
		panic(fmt.Sprintf("logic error: lastIndex-%d, currIndex-%d", p.lastIndex, index.Id.Id))
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
	snip := NewSimpleSnippet(index)
	p.AppendSnippet(snip)
}

func (p *proxy) AppendSnippet(snip *Snippet) {
	// logutil.Infof("append snippet: %s", snip.String())
	p.alumu.Lock()
	defer p.alumu.Unlock()
	pos, ok := p.snipIdx[snip.GetId()]
	if ok {
		p.snippets[pos].Append(snip)
		return
	}
	p.snipIdx[snip.GetId()] = len(p.snippets)
	group := newSnippets(snip.GetId())
	group.Append(snip)
	p.snippets = append(p.snippets, group)
}

func (p *proxy) ExtendSnippets(snips []*Snippet) {
	if len(snips) == 0 {
		return
	}
	mapped := make(map[uint64][]*Snippet)
	for _, snip := range snips {
		mapped[snip.GetId()] = append(mapped[snip.GetId()], snip)
	}
	p.alumu.Lock()
	defer p.alumu.Unlock()
	for id, ss := range mapped {
		pos, ok := p.snipIdx[id]
		if ok {
			p.snippets[pos].Extend(ss...)
			continue
		}
		p.snipIdx[id] = len(p.snippets)
		group := newSnippets(id)
		group.Extend(ss...)
		p.snippets = append(p.snippets, group)
	}
}

func (p *proxy) Checkpoint() {
	now := time.Now()
	p.alumu.Lock()
	snips := p.snippets
	p.snippets = make([]*snippets, 0, 100)
	p.snipIdx = make(map[uint64]int)
	p.alumu.Unlock()

	mask := roaring64.NewBitmap()
	for _, snip := range snips {
		snip.ForEach(func(id *IndexId) {
			if id.IsSingle() {
				mask.Add(id.Id)
				return
			}
			node := p.indice[id.Id]
			if node == nil {
				node = newCommitEntry(id)
				p.indice[id.Id] = node
			}
			node.mask.Add(uint64(id.Offset))
			if node.Committed() {
				mask.Add(node.GetId())
				delete(p.indice, id.Id)
			}
		})
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
		logEntry.WaitDone()
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

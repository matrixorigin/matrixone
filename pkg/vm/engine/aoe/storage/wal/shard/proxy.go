package shard

import (
	"fmt"
	"matrixone/pkg/logutil"
	"sync"
	"sync/atomic"
	"time"

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
	logmu     sync.RWMutex
	alumu     sync.RWMutex
	id        uint64
	mgr       *manager
	mask      *roaring64.Bitmap
	stopmask  *roaring64.Bitmap
	snippets  []*snippets
	snipIdx   map[uint64]int
	lastIndex uint64
	safeId    uint64
	indice    map[uint64]*commitEntry
}

func newProxy(id uint64, mgr *manager) *proxy {
	return &proxy{
		id:       id,
		mgr:      mgr,
		mask:     roaring64.New(),
		stopmask: roaring64.New(),
		snipIdx:  make(map[uint64]int),
		snippets: make([]*snippets, 0, 10),
		indice:   make(map[uint64]*commitEntry),
	}
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

func (p *proxy) LogIndice(indice ...*LogIndex) {
	p.logmu.Lock()
	for _, index := range indice {
		p.logIndexLocked(index)
	}
	p.logmu.Unlock()
}

func (p *proxy) logIndexLocked(index *LogIndex) {
	p.mask.Add(index.Id.Id)
	if index.Id.Id > p.lastIndex+uint64(1) {
		p.stopmask.AddRange(p.lastIndex+uint64(1), index.Id.Id)
	}
	p.lastIndex = index.Id.Id
}

func (p *proxy) LogIndex(index *LogIndex) {
	p.logmu.Lock()
	p.logIndexLocked(index)
	p.logmu.Unlock()
}

func (p *proxy) AppendIndex(index *LogIndex) {
	snip := NewSimpleSnippet(index)
	p.AppendSnippet(snip)
}

func (p *proxy) AppendSnippet(snip *Snippet) {
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
		p.stopmask.RemoveRange(uint64(0), start)
	} else {
		it := p.mask.Iterator()
		pos := it.Next()
		p.SetSafeId(pos - 1)
	}
	p.logmu.Unlock()
	logutil.Infof("Shard-%d: pending-%d, safeid-%d %s", p.id, maskNum, p.GetSafeId(), time.Since(now))
}

func (p *proxy) SetSafeId(id uint64) {
	atomic.StoreUint64(&p.safeId, id)
}

func (p *proxy) GetSafeId() uint64 {
	return atomic.LoadUint64(&p.safeId)
}

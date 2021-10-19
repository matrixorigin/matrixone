package shard

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v2"
	"matrixone/pkg/vm/engine/aoe/storage/wal"
)

type IndexId = metadata.LogBatchId
type LogIndex = metadata.LogIndex
type Entry = wal.Entry

type snippets struct {
	id    uint64
	snips []*snippet
}

func newSnippets(id uint64) *snippets {
	group := &snippets{
		snips: make([]*snippet, 0),
		id:    id,
	}
	return group
}

func (g *snippets) GetId() uint64 {
	return g.id
}

func (g *snippets) Append(seq *snippet) {
	g.snips = append(g.snips, seq)
}

func (g *snippets) Extend(snips ...*snippet) {
	g.snips = append(g.snips, snips...)
}

func (g *snippets) ForEach(fn func(*IndexId)) {
	for _, seq := range g.snips {
		seq.CompletedRange(nil, fn)
	}
}

type snippet struct {
	shardId uint64
	id      uint64
	offset  uint32
	indice  []*LogIndex
}

func NewSnippet(shardId, id uint64, offset uint32) *snippet {
	return &snippet{
		shardId: shardId,
		id:      id,
		offset:  offset,
		indice:  make([]*LogIndex, 0, 10),
	}
}

func (s *snippet) GetId() uint64 {
	return s.id
}

func (s *snippet) GetShardId() uint64 {
	return s.shardId
}

func (s *snippet) Append(index *LogIndex) {
	copied := *index
	s.indice = append(s.indice, &copied)
}

func (s *snippet) LastIndex() *LogIndex {
	if len(s.indice) == 0 {
		return nil
	}
	return s.indice[len(s.indice)-1]
}

func (s *snippet) CompletedRange(exclude *common.Range, fn func(*IndexId)) common.Range {
	count := uint64(0)
	if exclude == nil {
		r := common.Range{}
		for i := len(s.indice) - 1; i >= 0; i-- {
			if !s.indice[i].IsApplied() {
				break
			}
			r.Left = uint64(i)
			count++
			if fn != nil {
				fn(&s.indice[i].Id)
			}
		}
		if count > 0 {
			r.Right = r.Left + count - 1
		}
		return r
	}
	r := common.Range{}
	excludeEnd := int(exclude.Right)
	for i := len(s.indice) - 1; i >= 0; i-- {
		if !s.indice[i].IsApplied() || i < excludeEnd || i < int(s.offset) {
			break
		}
		count++
		r.Left = uint64(i)
		if fn != nil {
			fn(&s.indice[i].Id)
		}
	}
	if count > 0 {
		r.Right = r.Left + count - 1
	}
	return r
}

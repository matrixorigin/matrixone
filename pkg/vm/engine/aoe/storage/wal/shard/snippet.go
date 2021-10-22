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
	"matrixone/pkg/vm/engine/aoe/storage/common"
)

type snippets struct {
	id    uint64
	snips []*Snippet
}

func newSnippets(id uint64) *snippets {
	group := &snippets{
		snips: make([]*Snippet, 0),
		id:    id,
	}
	return group
}

func (g *snippets) GetId() uint64 {
	return g.id
}

func (g *snippets) Append(seq *Snippet) {
	g.snips = append(g.snips, seq)
}

func (g *snippets) Extend(snips ...*Snippet) {
	g.snips = append(g.snips, snips...)
}

func (g *snippets) ForEach(fn func(*IndexId)) {
	for _, seq := range g.snips {
		seq.CompletedRange(nil, fn)
	}
}

type Snippet struct {
	shardId uint64
	id      uint64
	offset  uint32
	indice  []*Index
}

func NewSnippet(shardId, id uint64, offset uint32) *Snippet {
	return &Snippet{
		shardId: shardId,
		id:      id,
		offset:  offset,
		indice:  make([]*Index, 0, 10),
	}
}

func NewSimpleSnippet(index *Index) *Snippet {
	return &Snippet{
		shardId: index.ShardId,
		indice:  []*Index{index},
	}
}

func (s *Snippet) GetId() uint64 {
	return s.id
}

func (s *Snippet) GetShardId() uint64 {
	return s.shardId
}

func (s *Snippet) Append(index *Index) {
	copied := *index
	s.indice = append(s.indice, &copied)
}

func (s *Snippet) LastIndex() *Index {
	if len(s.indice) == 0 {
		return nil
	}
	return s.indice[len(s.indice)-1]
}

func (s *Snippet) CompletedRange(exclude *common.Range, fn func(*IndexId)) common.Range {
	count := uint64(0)
	if exclude == nil {
		r := common.Range{}
		for i := len(s.indice) - 1; i >= 0; i-- {
			// logutil.Infof("snippet %d-%d %s", s.id, i, s.indice[i].String())
			if !s.indice[i].IsApplied() {
				continue
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
		// logutil.Infof("snippet %d-%d %s", s.id, i, s.indice[i].String())
		if !s.indice[i].IsApplied() || i < excludeEnd || i < int(s.offset) {
			continue
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

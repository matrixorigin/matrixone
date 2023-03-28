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

package blockio

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
)

// prefetch is the parameter of the executed IoPipeline.Prefetch, which
// provides the merge function, which can merge the prefetch requests of
// multiple blocks in an object/file
type prefetch struct {
	name   string
	meta   objectio.Extent
	ids    map[uint32]*objectio.ReadBlockOptions
	pool   *mpool.MPool
	reader objectio.Reader
}

func BuildPrefetch(reader dataio.Reader, m *mpool.MPool) prefetch {
	ids := make(map[uint32]*objectio.ReadBlockOptions)
	return prefetch{
		name:   reader.GetObjectName(),
		meta:   reader.GetObjectExtent(),
		ids:    ids,
		reader: reader.GetObjectReader(),
		pool:   m,
	}
}

func (p *prefetch) AddBlock(idxes []uint16, ids []uint32) {
	blocks := make(map[uint32]*objectio.ReadBlockOptions)
	columns := make(map[uint16]bool)
	for _, idx := range idxes {
		columns[idx] = true
	}
	for _, id := range ids {
		blocks[id] = &objectio.ReadBlockOptions{
			Id:    id,
			Idxes: columns,
		}
	}
	p.mergeIds(blocks)
}

func (p *prefetch) mergeIds(ids2 map[uint32]*objectio.ReadBlockOptions) {
	for id, block := range ids2 {
		if p.ids[id] == nil {
			p.ids[id] = block
			continue
		}
		for index := range block.Idxes {
			p.ids[id].Idxes[index] = true
		}
	}
}

func mergePrefetch(processes []prefetch) map[string]prefetch {
	pc := make(map[string]prefetch)
	for _, p := range processes {
		if pc[p.name].name == "" {
			pc[p.name] = p
			continue
		}
		pre := pc[p.name]
		pre.mergeIds(p.ids)
		pc[p.name] = pre

	}
	return pc
}

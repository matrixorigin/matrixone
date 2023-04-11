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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// prefetch is the parameter of the executed IoPipeline.Prefetch, which
// provides the merge function, which can merge the prefetch requests of
// multiple blocks in an object/file
type prefetch struct {
	name    objectio.ObjectName
	nameStr string
	meta    *objectio.Extent
	ids     map[uint32]*objectio.ReadBlockOptions
	reader  *objectio.ObjectReader
}

func BuildPrefetch(service fileservice.FileService, key objectio.Location) (prefetch, error) {
	reader, err := NewObjectReader(service, key)
	if err != nil {
		return prefetch{}, err
	}
	return buildPrefetch(reader), nil
}

func buildPrefetch(reader *BlockReader) prefetch {
	ids := make(map[uint32]*objectio.ReadBlockOptions)
	return prefetch{
		name:    reader.GetObjectName(),
		nameStr: reader.GetObjectName().String(),
		meta:    reader.GetObjectExtent(),
		ids:     ids,
		reader:  reader.GetObjectReader(),
	}
}
func buildPrefetchNew(reader *BlockReader) prefetch {
	ids := make(map[uint32]*objectio.ReadBlockOptions)
	return prefetch{
		nameStr: reader.GetName(),
		meta:    reader.GetObjectExtent(),
		ids:     ids,
		reader:  reader.GetObjectReader(),
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
		if pc[p.nameStr].name == nil {
			pc[p.nameStr] = p
			continue
		}
		pre := pc[p.nameStr]
		pre.mergeIds(p.ids)
		pc[p.nameStr] = pre

	}
	return pc
}

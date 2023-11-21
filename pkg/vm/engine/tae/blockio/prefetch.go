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

// PrefetchParams is the parameter of the executed IoPipeline.PrefetchParams, which
// provides the merge function, which can merge the PrefetchParams requests of
// multiple blocks in an object/file
type PrefetchParams struct {
	ids          map[uint16]*objectio.ReadBlockOptions
	fs           fileservice.FileService
	key          objectio.Location
	reader       *objectio.ObjectReader
	prefetchFile bool
}

func BuildPrefetchParams(service fileservice.FileService, key objectio.Location) (PrefetchParams, error) {
	pp := buildPrefetchParams(service, key)
	return pp, nil
}

func buildPrefetchParams(service fileservice.FileService, key objectio.Location) PrefetchParams {
	ids := make(map[uint16]*objectio.ReadBlockOptions)
	return PrefetchParams{
		ids: ids,
		fs:  service,
		key: key,
	}
}

func buildPrefetchParamsByReader(reader *BlockReader) PrefetchParams {
	ids := make(map[uint16]*objectio.ReadBlockOptions)
	return PrefetchParams{
		ids:    ids,
		reader: reader.GetObjectReader(),
	}
}

func (p *PrefetchParams) AddBlock(idxes []uint16, ids []uint16) {
	blocks := make(map[uint16]*objectio.ReadBlockOptions)
	columns := make(map[uint16]bool)
	for _, idx := range idxes {
		columns[idx] = true
	}
	for _, id := range ids {
		blocks[id] = &objectio.ReadBlockOptions{
			Id:       id,
			Idxes:    columns,
			DataType: uint16(objectio.SchemaData),
		}
	}
	p.mergeIds(blocks)
}
func (p *PrefetchParams) AddBlockWithType(idxes []uint16, ids []uint16, dataType uint16) {
	blocks := make(map[uint16]*objectio.ReadBlockOptions)
	columns := make(map[uint16]bool)
	for _, idx := range idxes {
		columns[idx] = true
	}
	for _, id := range ids {
		blocks[id] = &objectio.ReadBlockOptions{
			Id:       id,
			DataType: dataType,
			Idxes:    columns,
		}
	}
	p.mergeIds(blocks)
}

func (p *PrefetchParams) mergeIds(ids2 map[uint16]*objectio.ReadBlockOptions) {
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

func mergePrefetch(processes []PrefetchParams) map[string]PrefetchParams {
	pc := make(map[string]PrefetchParams)
	for _, p := range processes {
		var name string
		if p.reader != nil {
			name = p.reader.GetName()
		} else {
			name = p.key.Name().String()
		}
		old, ok := pc[name]
		if !ok {
			pc[name] = p
			continue
		}
		old.mergeIds(p.ids)
		pc[name] = old
	}
	return pc
}

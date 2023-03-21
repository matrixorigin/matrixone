package blockio

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
)

type prefetch struct {
	name   string
	meta   objectio.Extent
	ids    map[uint32]*objectio.ReadBlockOptions
	pool   *mpool.MPool
	reader objectio.Reader
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

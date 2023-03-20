package blockio

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type prefetchCtx struct {
	name   string
	meta   objectio.Extent
	ids    map[uint32]*objectio.ReadBlock
	pool   *mpool.MPool
	reader objectio.Reader
}

func mergePrefetch(processes []prefetchCtx) map[string]prefetchCtx {
	pc := make(map[string]prefetchCtx)
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

func (p *prefetchCtx) mergeIds(ids2 map[uint32]*objectio.ReadBlock) {
	for id, block := range ids2 {
		if p.ids[id] == nil {
			p.ids[id] = block
			continue
		}
		for index := range block.Idxes {
			if p.ids[id].Idxes[index] == false {
				p.ids[id].Idxes[index] = true
			}
		}
	}
	return
}

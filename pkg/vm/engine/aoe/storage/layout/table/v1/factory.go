package table

import (
	"matrixone/pkg/vm/engine/aoe/storage/db/factories/base"
	fb "matrixone/pkg/vm/engine/aoe/storage/db/factories/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

// type blockFactory struct{}

// func (f *blockFactory) CreateBlock(host iface.ISegment, meta *metadata.Block) (iface.IBlock, error) {
// 	return newBlock(host, meta)
// }

type altBlockFactory struct {
	nodeFactory base.NodeFactory
}

func newAltBlockFactory(mutFactory fb.MutFactory, tabledata iface.ITableData) *altBlockFactory {
	f := &altBlockFactory{
		nodeFactory: mutFactory.CreateNodeFactory(tabledata),
	}
	return f
}

func (af *altBlockFactory) CreateBlock(host iface.ISegment, meta *metadata.Block) (iface.IBlock, error) {
	if meta.DataState < metadata.FULL {
		return newTBlock(host, meta, af.nodeFactory, nil)
	}
	return newBlock(host, meta)
}

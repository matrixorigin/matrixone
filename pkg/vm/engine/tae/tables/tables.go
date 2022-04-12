package tables

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
)

type DataFactory struct {
	fileFactory  dataio.SegmentFileFactory
	appendBufMgr base.INodeManager
}

func NewDataFactory(fileFactory dataio.SegmentFileFactory, appendBufMgr base.INodeManager) *DataFactory {
	return &DataFactory{
		fileFactory:  fileFactory,
		appendBufMgr: appendBufMgr,
	}
}

func (factory *DataFactory) MakeTableFactory() catalog.TableDataFactory {
	return func(meta *catalog.TableEntry) data.Table {
		return newTable(meta, factory.fileFactory, factory.appendBufMgr)
	}
}

func (factory *DataFactory) MakeSegmentFactory() catalog.SegmentDataFactory {
	return func(meta *catalog.SegmentEntry) data.Segment {
		return newSegment(meta, factory.fileFactory, factory.appendBufMgr)
	}
}

func (factory *DataFactory) MakeBlockFactory(segFile dataio.SegmentFile) catalog.BlockDataFactory {
	return func(meta *catalog.BlockEntry) data.Block {
		return newBlock(meta, segFile, factory.appendBufMgr)
	}
}

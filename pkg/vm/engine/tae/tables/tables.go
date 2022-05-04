package tables

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type DataFactory struct {
	fileFactory  file.SegmentFileFactory
	appendBufMgr base.INodeManager
	scheduler    tasks.TaskScheduler
}

func NewDataFactory(fileFactory file.SegmentFileFactory, appendBufMgr base.INodeManager, scheduler tasks.TaskScheduler) *DataFactory {
	return &DataFactory{
		fileFactory:  fileFactory,
		appendBufMgr: appendBufMgr,
		scheduler:    scheduler,
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

func (factory *DataFactory) MakeBlockFactory(segFile file.Segment) catalog.BlockDataFactory {
	return func(meta *catalog.BlockEntry) data.Block {
		return newBlock(meta, segFile, factory.appendBufMgr, factory.scheduler)
	}
}

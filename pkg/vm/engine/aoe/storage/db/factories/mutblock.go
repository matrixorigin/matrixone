package factories

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mutation"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type mutNodeFactory struct {
	host  *mutFactory
	tdata iface.ITableData
}

func newMutNodeFactory(host *mutFactory, tdata iface.ITableData) *mutNodeFactory {
	f := &mutNodeFactory{
		tdata: tdata,
		host:  host,
	}
	return f
}

func (f *mutNodeFactory) GetManager() bb.INodeManager {
	return f.host.mgr
}

func (f *mutNodeFactory) CreateNode(segfile base.ISegmentFile, meta *metadata.Block) bb.INode {
	blkfile := dataio.NewTBlockFile(segfile, *meta.AsCommonID())
	f.tdata.Ref()
	n := mutation.NewMutableBlockNode(f.host.mgr, blkfile, f.tdata, meta, f.host.flusher)
	f.host.mgr.RegisterNode(n)
	return n
}

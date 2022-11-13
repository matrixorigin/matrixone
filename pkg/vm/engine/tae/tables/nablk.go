package tables

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type block struct {
	*baseBlock
}

func newNABlock(
	meta *catalog.BlockEntry,
	fs *objectio.ObjectFS,
	bufMgr base.INodeManager,
	scheduler tasks.TaskScheduler) *block {
	base := newBaseBlock(meta, bufMgr, fs, scheduler)
	blk := &block{
		baseBlock: base,
	}
	base.mvcc.SetDeletesListener(blk.OnApplyDelete)
	node := newPersistedNode(base)
	pinned := node.Pin()
	blk.storage.pnode = pinned
	return blk
}

func (blk *block) OnApplyDelete(
	deleted uint64,
	gen common.RowGen,
	ts types.TS) (err error) {
	blk.meta.GetSegment().GetTable().RemoveRows(deleted)
	return
}

func (blk *block) PrepareCompact() bool {
	return blk.meta.PrepareCompact()
}

func (blk *block) Pin() *common.PinnedItem[*block] {
	blk.Ref()
	return &common.PinnedItem[*block]{
		Val: blk,
	}
}

func (blk *block) GetColumnDataById(
	txn txnif.AsyncTxn,
	colIdx int,
	buffer *bytes.Buffer) (view *model.ColumnView, err error) {
	_, pnode := blk.PinNode()
	defer pnode.Close()
	return blk.ResolvePersistedColumnData(
		pnode.Item(),
		txn.GetStartTS(),
		colIdx,
		buffer,
		false)
}

func (blk *block) BatchDedup(
	txn txnif.AsyncTxn,
	keys containers.Vector,
	rowmask *roaring.Bitmap) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Infof("BatchDedup BLK-%d: %v", blk.meta.ID, err)
		}
	}()
	_, pnode := blk.PinNode()
	defer pnode.Close()
	return blk.PersistedBatchDedup(
		pnode.Item(),
		txn.GetStartTS(),
		keys,
		rowmask,
		blk.dedupClosure)
}

func (blk *block) dedupClosure(
	vec containers.Vector,
	mask *roaring.Bitmap,
	def *catalog.ColDef) func(any, int) error {
	return func(v any, _ int) (err error) {
		if _, existed := compute.GetOffsetByVal(vec, v, mask); existed {
			entry := common.TypeStringValue(vec.GetType(), v)
			return moerr.NewDuplicateEntry(entry, def.Name)
		}
		return nil
	}
}

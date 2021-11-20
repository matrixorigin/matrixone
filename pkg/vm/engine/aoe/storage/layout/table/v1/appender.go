package table

import (
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
)

type MutationHandle interface {
	io.Closer
	Append(bat *batch.Batch, index *shard.SliceIndex) (err error)
	Flush() error
	String() string
	GetMeta() *metadata.Table
}

type tableAppender struct {
	common.RefHelper
	data        iface.ITableData
	meta        *metadata.Table
	mu          *sync.RWMutex
	blkAppender iface.IMutBlock
	opts        *storage.Options
}

func newTableAppender(opts *storage.Options, data iface.ITableData) *tableAppender {
	c := &tableAppender{
		opts: opts,
		data: data,
		meta: data.GetMeta(),
		mu:   &sync.RWMutex{},
	}
	blkAppender := data.StrongRefLastBlock()
	if blkAppender != nil {
		if blkAppender.GetType() == base.TRANSIENT_BLK {
			c.blkAppender = blkAppender.(iface.IMutBlock)
		} else {
			blkAppender.Unref()
		}
	}
	c.Ref()
	c.OnZeroCB = c.close
	return c
}

func (c *tableAppender) GetMeta() *metadata.Table {
	return c.meta
}

func (c *tableAppender) close() {
	if c.blkAppender != nil {
		c.blkAppender.Unref()
	}
}

func (c *tableAppender) Flush() error {
	c.mu.RLock()
	if c.blkAppender == nil {
		c.mu.RUnlock()
		return nil
	}
	blkHandle := c.blkAppender.MakeHandle()
	c.mu.RUnlock()
	defer blkHandle.Close()
	blk := blkHandle.GetNode().(mb.IMutableBlock)
	return blk.Flush()
}

func (c *tableAppender) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.blkAppender.String()
}

func (c *tableAppender) onNoBlock() (meta *metadata.Block, data iface.IBlock, err error) {
	var prevMeta *metadata.Block
	if c.blkAppender != nil {
		prevMeta = c.blkAppender.GetMeta()
	}
	meta = c.meta.SimpleGetOrCreateNextBlock(prevMeta)
	data, err = c.opts.Scheduler.InstallBlock(meta, c.data)
	return
}

func (c *tableAppender) onNoMut() error {
	_, data, err := c.onNoBlock()
	if err != nil {
		return err
	}
	c.blkAppender = data.(iface.IMutBlock)
	return nil
}

func (c *tableAppender) onImmut() {
	c.opts.Scheduler.AsyncFlushBlock(c.blkAppender)
	c.onNoMut()
}

func (c *tableAppender) doAppend(mutblk mb.IMutableBlock, bat *batch.Batch, offset uint64, index *shard.SliceIndex) (n uint64, err error) {
	var na int
	meta := mutblk.GetMeta()
	data := mutblk.GetData()
	for idx, attr := range data.GetAttrs() {
		for i, a := range bat.Attrs {
			if a == meta.Segment.Table.Schema.ColDefs[idx].Name {
				vec, err := data.GetVectorByAttr(attr)
				if err != nil {
					return 0, err
				}
				if na, err = vec.AppendVector(bat.Vecs[i], int(offset)); err != nil {
					return n, err
				}
			}
		}
	}
	n = uint64(na)
	index.Count = n
	meta.Lock()
	defer meta.Unlock()
	if !index.IsApplied() && !index.IsSlice() {
		left := index.Capacity - index.Start - index.Count
		slices := 1 + left/meta.Segment.Table.Schema.BlockMaxRows
		if left%meta.Segment.Table.Schema.BlockMaxRows != 0 {
			slices += 1
		}
		index.Info = &shard.SliceInfo{
			Offset: 0,
			Size:   uint32(slices),
		}
	}
	if err = meta.SetIndexLocked(index); err != nil {
		return 0, err
	}
	// log.Infof("1. offset=%d, n=%d, cap=%d, index=%s, blkcnt=%d", offset, n, bat.Vecs[0].Length(), index.String(), mt.Meta.GetCount())
	if _, err = meta.AddCountLocked(n); err != nil {
		return 0, err
	}
	c.data.AddRows(n)
	// log.Infof("2. offset=%d, n=%d, cap=%d, index=%s, blkcnt=%d", offset, n, bat.Vecs[0].Length(), index.String(), mt.Meta.GetCount())
	return n, nil
}

func (c *tableAppender) Append(bat *batch.Batch, index *shard.SliceIndex) (err error) {
	logutil.Infof("Append logindex: %s", index.String())
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.blkAppender == nil {
		c.onNoMut()
	} else if c.blkAppender.GetMeta().HasMaxRowsLocked() {
		c.onImmut()
	}

	offset := index.Start
	blkHandle := c.blkAppender.MakeHandle()
	for {
		if c.blkAppender.GetMeta().HasMaxRowsLocked() {
			c.onImmut()
			blkHandle.Close()
			blkHandle = c.blkAppender.MakeHandle()
		}
		blk := blkHandle.GetNode().(mb.IMutableBlock)
		n, err := c.doAppend(blk, bat, offset, index)
		if err != nil {
			blkHandle.Close()
			return err
		}
		offset += n
		if offset == uint64(bat.Vecs[0].Length()) {
			break
		}
		index.Start += n
		index.Count = uint64(0)
		index.Info.Offset += 1
	}
	blkHandle.Close()

	return err
}

func (c *tableAppender) Close() error {
	c.Unref()
	c.data.Unref()
	return nil
}

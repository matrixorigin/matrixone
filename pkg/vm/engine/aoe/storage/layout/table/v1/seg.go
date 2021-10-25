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

package table

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	bmgrif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync"
	"sync/atomic"
)

type segment struct {
	sllnode
	typ  base.SegmentType
	tree struct {
		*sync.RWMutex
		blocks    []iface.IBlock
		helper    map[uint64]int
		blockids  []uint64
		blockcnt  uint32
		attrsizes map[string]uint64
	}
	host        iface.ITableData
	meta        *metadata.Segment
	indexHolder *index.SegmentHolder
	segFile     base.ISegmentFile
}

func newSegment(host iface.ITableData, meta *metadata.Segment) (iface.ISegment, error) {
	var err error
	segType := base.UNSORTED_SEG
	if meta.CommitInfo.Op == metadata.OpUpgradeSorted {
		segType = base.SORTED_SEG
	}
	mu := new(sync.RWMutex)
	seg := &segment{
		typ:     segType,
		host:    host,
		meta:    meta,
		sllnode: *common.NewSLLNode(mu),
	}

	fsMgr := seg.host.GetFsManager()
	segId := meta.AsCommonID().AsSegmentID()
	indexHolder := host.GetIndexHolder().RegisterSegment(segId, segType, nil)
	seg.indexHolder = indexHolder
	segFile := fsMgr.GetUnsortedFile(segId)
	if segType == base.UNSORTED_SEG {
		if segFile == nil {
			segFile, err = fsMgr.RegisterUnsortedFiles(segId)
			if err != nil {
				panic(err)
			}
		}
		seg.indexHolder.Init(segFile)
	} else {
		if segFile != nil {
			fsMgr.UpgradeFile(segId)
		} else {
			segFile = fsMgr.GetSortedFile(segId)
			if segFile == nil {
				segFile, err = fsMgr.RegisterSortedFiles(segId)
				if err != nil {
					panic(err)
				}
			}
		}
		seg.indexHolder.Init(segFile)
	}

	seg.tree.RWMutex = mu
	seg.tree.blocks = make([]iface.IBlock, 0)
	seg.tree.helper = make(map[uint64]int)
	seg.tree.blockids = make([]uint64, 0)
	seg.tree.attrsizes = make(map[string]uint64)
	seg.OnZeroCB = seg.close
	seg.segFile = segFile
	seg.segFile.Ref()
	seg.Ref()
	return seg, nil
}

func NewSimpleSegment(typ base.SegmentType, meta *metadata.Segment, indexHolder *index.SegmentHolder, segFile base.ISegmentFile) *segment {
	return &segment{
		typ:         typ,
		meta:        meta,
		indexHolder: indexHolder,
		segFile:     segFile,
		tree: struct {
			*sync.RWMutex
			blocks    []iface.IBlock
			helper    map[uint64]int
			blockids  []uint64
			blockcnt  uint32
			attrsizes map[string]uint64
		}{
			helper:  make(map[uint64]int),
			RWMutex: &sync.RWMutex{},
		},
	}
}

func (seg *segment) CanUpgrade() bool {
	if seg.typ == base.SORTED_SEG {
		return false
	}
	if len(seg.tree.blocks) < int(seg.meta.Table.Schema.SegmentMaxBlocks) {
		return false
	}
	for _, blk := range seg.tree.blocks {
		if blk.GetType() != base.PERSISTENT_BLK {
			return false
		}
	}
	return true
}

func (seg *segment) GetSegmentedIndex() (id uint64, ok bool) {
	ok = false
	if seg.typ == base.SORTED_SEG {
		for i := len(seg.tree.blocks) - 1; i >= 0; i-- {
			id, ok = seg.tree.blocks[i].GetSegmentedIndex()
			if ok {
				return id, ok
			}
		}
		return id, ok
	}
	blkCnt := atomic.LoadUint32(&seg.tree.blockcnt)
	for i := 0; i <= int(blkCnt)-1; i++ {
		seg.tree.RLock()
		blk := seg.tree.blocks[i]
		seg.tree.RUnlock()
		tmpId, tmpOk := blk.GetSegmentedIndex()
		if tmpOk {
			id, ok = tmpId, tmpOk
		} else if blk.GetType() > base.TRANSIENT_BLK {
			continue
		} else {
			break
		}
	}
	return id, ok
}

func (seg *segment) GetReplayIndex() *metadata.LogIndex {
	// if seg.tree.blockcnt == 0 {
	// 	return nil
	// }
	// var ctx *metadata.LogIndex
	// for blkIdx := int(seg.tree.blockcnt) - 1; blkIdx >= 0; blkIdx-- {
	// 	blk := seg.tree.blocks[blkIdx]
	// 	if ctx = blk.GetMeta().GetReplayIndex(); ctx != nil {
	// 		break
	// 	}
	// }
	// return ctx
	// TODO
	return nil
}

func (seg *segment) GetRowCount() uint64 {
	if seg.meta.CommitInfo.Op >= metadata.OpUpgradeClose {
		return seg.meta.Table.Schema.BlockMaxRows * seg.meta.Table.Schema.SegmentMaxBlocks
	}
	var ret uint64
	seg.tree.RLock()
	for _, blk := range seg.tree.blocks {
		ret += blk.GetRowCount()
	}
	seg.tree.RUnlock()
	return ret
}

func (seg *segment) Size(attr string) uint64 {
	if seg.typ >= base.SORTED_SEG {
		return seg.tree.attrsizes[attr]
	}
	size := uint64(0)
	blkCnt := atomic.LoadUint32(&seg.tree.blockcnt)
	var blk iface.IBlock
	for i := 0; i < int(blkCnt); i++ {
		seg.tree.RLock()
		blk = seg.tree.blocks[i]
		seg.tree.RUnlock()
		size += blk.Size(attr)
	}
	return size
}

func (seg *segment) BlockIds() []uint64 {
	if seg.typ == base.SORTED_SEG {
		return seg.tree.blockids
	}
	if atomic.LoadUint32(&seg.tree.blockcnt) == uint32(seg.meta.Table.Schema.SegmentMaxBlocks) {
		return seg.tree.blockids
	}
	seg.tree.RLock()
	ret := make([]uint64, 0, atomic.LoadUint32(&seg.tree.blockcnt))
	for _, blk := range seg.tree.blocks {
		ret = append(ret, blk.GetMeta().Id)
	}
	seg.tree.RUnlock()
	return ret
}

func (seg *segment) close() {
	segId := seg.meta.AsCommonID().AsSegmentID()
	if seg.indexHolder != nil {
		seg.indexHolder.Unref()
	}
	for _, blk := range seg.tree.blocks {
		blk.Unref()
		// log.Infof("blk refs=%d", blk.RefCount())
	}
	seg.sllnode.ReleaseNextNode()

	if seg.segFile != nil {
		seg.segFile.Unref()
	}
	if seg.typ == base.UNSORTED_SEG {
		seg.host.GetFsManager().UnregisterUnsortedFile(segId)
	} else {
		seg.host.GetFsManager().UnregisterSortedFile(segId)
	}
}

func (seg *segment) SetNext(next iface.ISegment) {
	seg.sllnode.SetNextNode(next)
}

func (seg *segment) GetNext() iface.ISegment {
	r := seg.sllnode.GetNextNode()
	if r == nil {
		return nil
	}
	return r.(iface.ISegment)
}

func (seg *segment) GetMeta() *metadata.Segment {
	return seg.meta
}

func (seg *segment) GetSegmentFile() base.ISegmentFile {
	return seg.segFile
}

func (seg *segment) GetType() base.SegmentType {
	return seg.typ
}

func (seg *segment) GetMTBufMgr() bmgrif.IBufferManager {
	return seg.host.GetMTBufMgr()
}

func (seg *segment) GetSSTBufMgr() bmgrif.IBufferManager {
	return seg.host.GetSSTBufMgr()
}

func (seg *segment) GetFsManager() base.IManager {
	return seg.host.GetFsManager()
}

func (seg *segment) GetIndexHolder() *index.SegmentHolder {
	return seg.indexHolder
}

func (seg *segment) String() string {
	seg.tree.RLock()
	defer seg.tree.RUnlock()
	s := fmt.Sprintf("<segment[%d]>(BlkCnt=%d)(RefCount=%d)(IndexRefs=%d)", seg.meta.Id, seg.tree.blockcnt, seg.RefCount(), seg.indexHolder.RefCount())
	for _, blk := range seg.tree.blocks {
		s = fmt.Sprintf("%s\n\t%s", s, blk.String())
		prev := blk.GetPrevVersion()
		v := 0
		for prev != nil {
			s = fmt.Sprintf("%s V%d", s, v)
			v++
			prev = prev.(iface.IBlock).GetPrevVersion()
		}
		s = fmt.Sprintf("%s V%d", s, v)
	}
	return s
}

func (seg *segment) StrongRefLastBlock() iface.IBlock {
	seg.tree.RLock()
	if len(seg.tree.blocks) == 0 {
		seg.tree.RUnlock()
		return nil
	}
	lastBlk := seg.tree.blocks[len(seg.tree.blocks)-1]
	seg.tree.RUnlock()
	lastBlk.Ref()
	return lastBlk
}

func (seg *segment) RegisterBlock(blkMeta *metadata.Block) (blk iface.IBlock, err error) {
	factory := seg.host.GetBlockFactory()
	if factory == nil {
		blk, err = newBlock(seg, blkMeta)
	} else {
		blk, err = factory.CreateBlock(seg, blkMeta)
	}
	if err != nil {
		return nil, err
	}
	seg.tree.Lock()
	defer seg.tree.Unlock()
	if len(seg.tree.blocks) > 0 {
		blk.Ref()
		seg.tree.blocks[len(seg.tree.blocks)-1].SetNext(blk)
	}

	seg.tree.blocks = append(seg.tree.blocks, blk)
	seg.tree.blockids = append(seg.tree.blockids, blk.GetMeta().Id)
	seg.tree.helper[blkMeta.Id] = int(seg.tree.blockcnt)
	atomic.AddUint32(&seg.tree.blockcnt, uint32(1))
	blk.Ref()
	return blk, err
}

func (seg *segment) WeakRefBlock(id uint64) iface.IBlock {
	seg.tree.RLock()
	defer seg.tree.RUnlock()
	idx, ok := seg.tree.helper[id]
	if !ok {
		return nil
	}
	return seg.tree.blocks[idx]
}

func (seg *segment) StrongRefBlock(id uint64) iface.IBlock {
	seg.tree.RLock()
	defer seg.tree.RUnlock()
	idx, ok := seg.tree.helper[id]
	if !ok {
		return nil
	}
	blk := seg.tree.blocks[idx]
	blk.Ref()
	return blk
}
func (seg *segment) CloneWithUpgrade(td iface.ITableData, meta *metadata.Segment) (iface.ISegment, error) {
	if seg.typ != base.UNSORTED_SEG {
		panic("logic error")
	}
	mu := new(sync.RWMutex)
	cloned := &segment{
		typ:     base.SORTED_SEG,
		host:    td,
		meta:    meta,
		sllnode: *common.NewSLLNode(mu),
	}
	cloned.tree.RWMutex = mu
	cloned.tree.blocks = make([]iface.IBlock, 0)
	cloned.tree.helper = make(map[uint64]int)
	cloned.tree.blockids = make([]uint64, 0)

	indexHolder := td.GetIndexHolder().StrongRefSegment(seg.meta.Id)
	if indexHolder == nil {
		panic("logic error")
	}

	id := seg.meta.AsCommonID().AsSegmentID()
	segFile := seg.host.GetFsManager().UpgradeFile(id)
	if segFile == nil {
		panic("logic error")
	}
	if indexHolder.Type == base.UNSORTED_SEG {
		indexHolder.Unref()
		indexHolder = td.GetIndexHolder().UpgradeSegment(seg.meta.Id, base.SORTED_SEG)
		seg.indexHolder.Init(segFile)
	}
	cloned.indexHolder = indexHolder
	cloned.segFile = segFile
	var prev iface.IBlock
	for _, blk := range seg.tree.blocks {
		newBlkMeta := cloned.meta.SimpleGetBlock(blk.GetMeta().Id)
		if newBlkMeta == nil {
			panic(metadata.BlockNotFoundErr)
		}
		cloned.Ref()
		cur, err := blk.CloneWithUpgrade(cloned, newBlkMeta)
		if err != nil {
			panic(err)
		}
		cloned.tree.helper[newBlkMeta.Id] = len(cloned.tree.blocks)
		cloned.tree.blocks = append(cloned.tree.blocks, cur)
		cloned.tree.blockids = append(cloned.tree.blockids, cur.GetMeta().Id)
		cloned.tree.blockcnt++
		if prev != nil {
			cur.Ref()
			prev.SetNext(cur)
		}
		prev = cur
	}

	cloned.segFile.Ref()
	cloned.Ref()
	cloned.OnZeroCB = cloned.close
	return cloned, nil
}

func (seg *segment) UpgradeBlock(meta *metadata.Block) (iface.IBlock, error) {
	if seg.typ != base.UNSORTED_SEG {
		panic("logic error")
	}
	if seg.meta.Id != meta.Segment.Id {
		panic("logic error")
	}
	idx, ok := seg.tree.helper[meta.Id]
	if !ok {
		logutil.Error("logic error")
		panic("logic error")
	}
	old := seg.tree.blocks[idx]
	seg.Ref()
	upgradeBlk, err := old.CloneWithUpgrade(seg, meta)
	if err != nil {
		return nil, err
	}
	var oldNext iface.IBlock
	if idx != len(seg.tree.blocks)-1 {
		oldNext = old.GetNext()
	}
	upgradeBlk.SetNext(oldNext)
	upgradeBlk.SetPrevVersion(old)

	seg.tree.Lock()
	defer seg.tree.Unlock()
	seg.tree.blocks[idx] = upgradeBlk
	if idx > 0 {
		upgradeBlk.Ref()
		seg.tree.blocks[idx-1].SetNext(upgradeBlk)
	}
	upgradeBlk.Ref()
	// old.SetNext(nil)
	old.Unref()
	return upgradeBlk, nil
}

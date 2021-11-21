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

package index

import (
	"errors"
	"fmt"
	roaring2 "github.com/RoaringBitmap/roaring"
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	mgrif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type PostCloseCB = func(interface{})

type SegmentHolder struct {
	common.RefHelper
	ID     common.ID
	BufMgr mgrif.IBufferManager
	Inited bool
	self   struct {
		sync.RWMutex
		Indices    []*Node
		ColIndices map[int][]int
		VersionMap map[int]uint64
	}
	tree struct {
		sync.RWMutex
		Blocks   []*BlockHolder
		IdMap    map[uint64]int
		BlockCnt int32
	}
	Type        base.SegmentType
	PostCloseCB PostCloseCB
}

func newSegmentHolder(bufMgr mgrif.IBufferManager, id common.ID, segType base.SegmentType, cb PostCloseCB) *SegmentHolder {
	holder := &SegmentHolder{ID: id, Type: segType, BufMgr: bufMgr}
	holder.tree.Blocks = make([]*BlockHolder, 0)
	holder.tree.IdMap = make(map[uint64]int)
	holder.self.ColIndices = make(map[int][]int)
	holder.self.Indices = make([]*Node, 0)
	holder.self.VersionMap = make(map[int]uint64)
	holder.OnZeroCB = holder.close
	holder.PostCloseCB = cb
	holder.Ref()
	return holder
}

func (holder *SegmentHolder) Init(segFile base.ISegmentFile) {
	holder.self.Lock()
	defer holder.self.Unlock()
	if holder.Inited {
		panic("logic error")
	}
	indicesMeta := segFile.GetIndicesMeta()
	if indicesMeta == nil {
		return
	}

	//holder.LoadIndex(segFile)

	// init embed index
	for _, meta := range indicesMeta.Data {
		vf := segFile.MakeVirtualIndexFile(meta)
		col := int(meta.Cols.ToArray()[0])
		var node *Node
		switch meta.Type {
		case base.ZoneMap:
			node = newNode(holder.BufMgr, vf, false, SegmentZoneMapIndexConstructor, meta.Cols, nil)
		//case base.NumBsi:
		//	node = newNode(holder.BufMgr, vf, false, NumericBsiIndexConstructor, meta.Cols, nil)
			//log.Info(node.GetManagedNode().DataNode.(*NumericBsiIndex).Get(uint64(39)))
			//node.Close()
		default:
			panic("unsupported index type")
		}
		idxes, ok := holder.self.ColIndices[col]
		if !ok {
			idxes = make([]int, 0)
			holder.self.ColIndices[col] = idxes
		}
		holder.self.ColIndices[col] = append(holder.self.ColIndices[col], len(holder.self.Indices))
		holder.self.Indices = append(holder.self.Indices, node)
	}
	holder.Inited = true
}

func (holder *SegmentHolder) LoadIndex(segFile base.ISegmentFile, filename string) {
	holder.self.Lock()
	defer holder.self.Unlock()
	// collect attached index files and make file for them
	if name, ok := common.ParseBitSlicedIndexFileName(filepath.Base(filename)); ok {
		version, tid, sid, col, ok := common.ParseBitSlicedIndexFileNameToInfo(name)
		if !ok {
			panic("unexpected error")
		}
		if sid != holder.ID.SegmentID || tid != holder.ID.TableID {
			panic("unexpected error")
		}
		build := false
		if v, ok := holder.self.VersionMap[int(col)]; !ok {
			holder.self.VersionMap[int(col)] = version
			build = true
		} else {
			if version > v {
				holder.self.VersionMap[int(col)] = version
				build = true
			} else {
				// stale index
				build = false
			}
		}
		if build {
			file, err := os.Open(filename)
			if err != nil {
				panic(err)
			}
			idxMeta, err := DefaultRWHelper.ReadIndicesMeta(*file)
			if err != nil {
				panic(err)
			}
			if idxMeta.Data == nil || len(idxMeta.Data) != 1 {
				panic("logic error")
			}
			col := int(idxMeta.Data[0].Cols.ToArray()[0])
			id := common.ID{}
			id.SegmentID = holder.ID.SegmentID
			id.Idx = uint16(idxMeta.Data[0].Cols.ToArray()[0])
			vf := segFile.MakeVirtualSeparateIndexFile(file, &id, idxMeta.Data[0])
			// TODO: str bsi
			node := newNode(holder.BufMgr, vf, false, NumericBsiIndexConstructor, idxMeta.Data[0].Cols, nil)
			idxes, ok := holder.self.ColIndices[col]
			if !ok {
				idxes = make([]int, 0)
				holder.self.ColIndices[col] = idxes
			}
			holder.self.ColIndices[col] = append(holder.self.ColIndices[col], len(holder.self.Indices))
			holder.self.Indices = append(holder.self.Indices, node)
			logutil.Infof("BSI load successfully | %s", filename)
			return
		}
		logutil.Warnf("stale index")
		return
	}
	panic("unexpected error")
}

func (holder *SegmentHolder) close() {
	for _, blk := range holder.tree.Blocks {
		blk.Unref()
	}

	for _, colIndex := range holder.self.Indices {
		colIndex.Unref()
	}
	if holder.PostCloseCB != nil {
		holder.PostCloseCB(holder)
	}
}

func (holder *SegmentHolder) EvalFilter(colIdx int, ctx *FilterCtx) error {
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		ctx.BoolRes = true
		return errors.New(fmt.Sprintf("index for column %d not found", colIdx))
	}
	var err error
	for _, idx := range idxes {
		node := holder.self.Indices[idx].GetManagedNode()
		err = node.DataNode.(Index).Eval(ctx)
		if err != nil {
			node.Close()
			return err
		}
		node.Close()
	}
	return nil
}

func (holder *SegmentHolder) CollectMinMax(colIdx int) (min []interface{}, max []interface{}, err error) {
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		return nil, nil, errors.New("no index found")
	}

	for _, idx := range idxes {
		node := holder.self.Indices[idx].GetManagedNode()
		if node.DataNode.(Index).Type() != base.ZoneMap {
			err = node.Close()
			if err != nil {
				return
			}
			continue
		}
		index := node.DataNode.(*SegmentZoneMapIndex)
		min = make([]interface{}, len(index.BlkMin))
		max = make([]interface{}, len(index.BlkMax))
		for i := 0; i < len(min); i++ {
			min[i] = index.BlkMin[i]
			max[i] = index.BlkMax[i]
		}
		err = node.Close()
		if err != nil {
			return
		}
	}
	return
}

func (holder *SegmentHolder) Count(colIdx int, filter *roaring.Bitmap) (uint64, error) {
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		return 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		node := holder.self.Indices[idx].GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			bm := roaring2.NewBitmap()
			if filter != nil {
				arr := filter.ToArray()
				for _, v := range arr {
					bm.Add(uint32(v))
				}
			} else {
				bm = nil
			}
			count := index.Count(bm)
			err := node.Close()
			if err != nil {
				return count, err
			}
			return count, nil
		}
		err := node.Close()
		if err != nil {
			return 0, err
		}
	}

	return 0, errors.New("bsi not found")
}

func (holder *SegmentHolder) NullCount(colIdx int, filter *roaring.Bitmap) (uint64, error) {
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		return 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		node := holder.self.Indices[idx].GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			bm := roaring2.NewBitmap()
			if filter != nil {
				arr := filter.ToArray()
				for _, v := range arr {
					bm.Add(uint32(v))
				}
			} else {
				bm = nil
			}
			count := index.NullCount(bm)
			err := node.Close()
			if err != nil {
				return count, err
			}
			return count, nil
		}
		err := node.Close()
		if err != nil {
			return 0, err
		}
	}

	return 0, errors.New("bsi not found")
}

func (holder *SegmentHolder) Min(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		return 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		node := holder.self.Indices[idx].GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			bm := roaring2.NewBitmap()
			if filter != nil {
				arr := filter.ToArray()
				for _, v := range arr {
					bm.Add(uint32(v))
				}
			} else {
				bm = nil
			}
			min, _ := index.Min(bm)
			err := node.Close()
			if err != nil {
				return min, err
			}
			return min, nil
		}
		err := node.Close()
		if err != nil {
			return 0, err
		}
	}
	return 0, errors.New("bsi not found")
}

func (holder *SegmentHolder) Max(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		return 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		node := holder.self.Indices[idx].GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			bm := roaring2.NewBitmap()
			if filter != nil {
				arr := filter.ToArray()
				for _, v := range arr {
					bm.Add(uint32(v))
				}
			} else {
				bm = nil
			}
			max, _ := index.Max(bm)
			err := node.Close()
			if err != nil {
				return max, err
			}
			return max, nil
		}
		err := node.Close()
		if err != nil {
			return 0, err
		}
	}
	return 0, errors.New("bsi not found")
}

func (holder *SegmentHolder) Sum(colIdx int, filter *roaring.Bitmap) (int64, uint64, error) {
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		return 0, 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		node := holder.self.Indices[idx].GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			bm := roaring2.NewBitmap()
			if filter != nil {
				arr := filter.ToArray()
				for _, v := range arr {
					bm.Add(uint32(v))
				}
			} else {
				bm = nil
			}
			sum, cnt := index.Sum(bm)
			res := int64(0)
			if res, ok = sum.(int64); ok {

			} else if ans, ok := sum.(uint64); ok {
				res = int64(ans)
			} else {
				return 0, 0, errors.New("invalid sum value type")
			}
			err := node.Close()
			if err != nil {
				return res, cnt, nil
			}
			return res, cnt, nil
		}
		err := node.Close()
		if err != nil {
			return 0, 0, err
		}
	}
	return 0, 0, errors.New("bsi not found")
}

func (holder *SegmentHolder) stringNoLock() string {
	s := fmt.Sprintf("<IndexSegmentHolder[%s]>[Ty=%v](Cnt=%d)(RefCount=%d)", holder.ID.SegmentString(), holder.Type,
		holder.tree.BlockCnt, holder.RefCount())
	for _, blk := range holder.tree.Blocks {
		s = fmt.Sprintf("%s\n\t%s", s, blk.stringNoLock())
	}
	return s
}

func (holder *SegmentHolder) StringIndicesRefsNoLock() string {
	s := fmt.Sprintf("<SEGHOLDER[%s]>[Indices cnt=%d]\n", holder.ID.SegmentString(), len(holder.self.Indices))
	for i, idx := range holder.self.Indices {
		s += fmt.Sprintf("<Index[%d]>[Ref cnt=%d]\n", i, idx.RefCount())
	}
	return s
}

func (holder *SegmentHolder) StrongRefBlock(id uint64) (blk *BlockHolder) {
	holder.tree.RLock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		holder.tree.RUnlock()
		return nil
	}
	blk = holder.tree.Blocks[idx]
	blk.Ref()
	holder.tree.RUnlock()
	return blk
}

func (holder *SegmentHolder) RegisterBlock(id common.ID, blkType base.BlockType, cb PostCloseCB) *BlockHolder {
	blk := newBlockHolder(holder.BufMgr, id, blkType, cb)
	holder.addBlock(blk)
	blk.Ref()
	return blk
}

func (holder *SegmentHolder) addBlock(blk *BlockHolder) {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	_, ok := holder.tree.IdMap[blk.ID.BlockID]
	if ok {
		panic(fmt.Sprintf("Duplicate blk %s for seg %s", blk.ID.BlockString(), holder.ID.SegmentString()))
	}
	holder.tree.IdMap[blk.ID.BlockID] = len(holder.tree.Blocks)
	holder.tree.Blocks = append(holder.tree.Blocks, blk)
	atomic.AddInt32(&holder.tree.BlockCnt, int32(1))
}

func (holder *SegmentHolder) DropBlock(id uint64) *BlockHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		panic(fmt.Sprintf("Specified blk %d not found in seg %d", id, holder.ID))
	}
	dropped := holder.tree.Blocks[idx]
	delete(holder.tree.IdMap, id)
	holder.tree.Blocks = append(holder.tree.Blocks[:idx], holder.tree.Blocks[idx+1:]...)
	atomic.AddInt32(&holder.tree.BlockCnt, int32(-1))
	return dropped
}

func (holder *SegmentHolder) GetBlockCount() int32 {
	return atomic.LoadInt32(&holder.tree.BlockCnt)
}

func (holder *SegmentHolder) UpgradeBlock(id uint64, blkType base.BlockType) *BlockHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		panic(fmt.Sprintf("specified blk %d not found in %d", id, holder.ID))
	}
	stale := holder.tree.Blocks[idx]
	if stale.Type >= blkType {
		panic(fmt.Sprintf("Cannot upgrade blk %d, type %d", id, blkType))
	}
	blk := newBlockHolder(holder.BufMgr, stale.ID, blkType, stale.PostCloseCB)
	holder.tree.Blocks[idx] = blk
	blk.Ref()
	stale.Unref()
	return blk
}

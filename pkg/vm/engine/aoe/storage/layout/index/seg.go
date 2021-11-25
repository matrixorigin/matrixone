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

type ColumnsAllocator struct {
	sync.RWMutex
	Allocators map[int]*common.IdAlloctor
}

type SegmentHolder struct {
	common.RefHelper
	ID     common.ID
	BufMgr mgrif.IBufferManager
	Inited bool
	VersionAllocator ColumnsAllocator
	self   struct {
		sync.RWMutex
		Indices    []*Node
		ColIndices     map[int][]int
		loadedVersion  map[int]uint64
		droppedVersion map[int]uint64
		FileHelper map[string]*Node
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
	holder.self.loadedVersion = make(map[int]uint64)
	holder.self.FileHelper = make(map[string]*Node)
	holder.self.droppedVersion = make(map[int]uint64)
	holder.VersionAllocator = ColumnsAllocator{
		RWMutex:    sync.RWMutex{},
		Allocators: make(map[int]*common.IdAlloctor),
	}
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

func (holder *SegmentHolder) AllocateVersion(colIdx int) uint64 {
	holder.VersionAllocator.Lock()
	defer holder.VersionAllocator.Unlock()
	if holder.VersionAllocator.Allocators[colIdx] == nil {
		holder.VersionAllocator.Allocators[colIdx] = common.NewIdAlloctor(uint64(1))
	}
	return holder.VersionAllocator.Allocators[colIdx].Alloc()
}

// DropIndex trying to drop the given index from holder. In detail, we only has 2 types
// of dropping:
// 1. explicit dropping by calling this function, remove the entry in `loadedVersion`,
//    and increase the `droppedVersion` to avoid scenario like: load 2 -> drop 2 ->
//    load1 => reversion. That is to say, only drop the newest version from this path.
//    If failed, i.e. the `loadedVersion` says the given version has been replaced by
//    a newer version, the given version should already been dropped via `LoadIndex`,
//    so that we can return directly saying dropped successfully.
// 2. implicit dropping by kicking out stale version during loading newer version.
//    By calling `LoadIndex`, if we detect stale version via versionMap, it would
//    be replaced by a newer version and dropped by the way.
//
// Notice that only when the index loaded(that is to say, resources have been allocated
// to the index) could it be added to the `loadedVersion`, so consider the following case:
// try generating and loading index 1, but not loaded yet -> try drop index 1, failed for
// dropping non-existent index. To solve this problem, we update `droppedVersion` anyway,
// so that when the "future" index comes, it would be cancelled.
func (holder *SegmentHolder) DropIndex(filename string) {
	holder.self.Lock()
	defer holder.self.Unlock()
	if name, ok := common.ParseBitSlicedIndexFileName(filepath.Base(filename)); ok {
		version, tid, sid, col, ok := common.ParseBitSlicedIndexFileNameToInfo(name)
		if !ok {
			panic("unexpected error")
		}
		if sid != holder.ID.SegmentID || tid != holder.ID.TableID {
			panic("unexpected error")
		}
		if v, ok := holder.self.loadedVersion[int(col)]; ok {
			if version > v {
				// speculative dropping
				if dropped, ok := holder.self.droppedVersion[int(col)]; ok {
					if dropped >= version {
						return
					}
				}
				holder.self.droppedVersion[int(col)] = version
				return
			} else if version < v {
				// stale index, has already been dropped
				return
			} else {
				// start explicit dropping
				staleName := common.MakeBitSlicedIndexFileName(version, tid, sid, col)
				//logutil.Infof("%s\n%+v", staleName, holder.self.FileHelper)
				node := holder.self.FileHelper[staleName]
				for _, idx := range holder.self.ColIndices[int(col)] {
					if holder.self.Indices[idx] == node {
						holder.self.Indices[idx] = nil
						break
					}
				}
				node.VFile.Unref()
				delete(holder.self.FileHelper, staleName)
				delete(holder.self.loadedVersion, int(col))
				holder.self.droppedVersion[int(col)] = version
				logutil.Infof("dropping newest index explicitly | version-%d", version)
				return
			}
		} else {
			if dv, ok := holder.self.droppedVersion[int(col)]; ok {
				if dv >= version {
					// already dropped explicitly
					return
				}
			}
			// drop "future" index, i.e. when the designated index comes, it
			// would be cancelled directly.
			holder.self.droppedVersion[int(col)] = version
			return
		}
	}
	panic("unexpected error")
}

// LoadIndex would not even make the VirtualFile if stale, so no need to worry about
// the resource leak like: load stale version 1 -> load fresher version -> load stale
// version 2, and 1 < 2 < fresh version -> loadedVersion updated -> stale version 2 never
// GCed. BTW, we always deal with Load/Drop in strict monotonically increase order, in
// detail we always explicitly drop only the newest version(that means only drop the
// version from loadedVersion), and remove the dropped version from loadedVersion. When
// stale version detected during loading, the stale version would be dropped implicitly
// and loadedVersion & droppedVersion would be refreshed as well.
// Notice: if given version < current newest version, no resource would be allocated, then
// no need to GC anything.
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
		isLatest := false
		if dropped, ok := holder.self.droppedVersion[int(col)]; ok {
			if dropped >= version {
				// newer version used to be loaded, but dropped explicitly
				// or dropped speculatively in the past.
				isLatest = false
				if err := os.Remove(filename); err != nil {
					panic(err)
				}
				logutil.Infof("detect stale index, version: %d, already dropped v-%d, file: %s", version, dropped, filepath.Base(filename))
				return
			}
		}
		if v, ok := holder.self.loadedVersion[int(col)]; !ok {
			holder.self.loadedVersion[int(col)] = version
			isLatest = true
		} else {
			if version > v {
				holder.self.loadedVersion[int(col)] = version
				// GC stale version
				staleName := common.MakeBitSlicedIndexFileName(v, tid, sid, col)
				node := holder.self.FileHelper[staleName]
				for _, idx := range holder.self.ColIndices[int(col)] {
					if holder.self.Indices[idx] == node {
						holder.self.Indices[idx] = nil
						break
					}
				}
				node.VFile.Unref()
				delete(holder.self.FileHelper, staleName)
				logutil.Infof("dropping stale index implicitly | version-%d", v)
				isLatest = true
			} else {
				// stale index, but not allocate resource yet, simply remove physical file
				isLatest = false
				if err := os.Remove(filename); err != nil {
					panic(err)
				}
				logutil.Infof("loading stale index | %s received, but v-%d already loaded", filename, v)
			}
		}
		if isLatest {
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
			holder.self.FileHelper[filepath.Base(filename)] = node
			logutil.Infof("BSI load successfully | %s", filename)
			return
		}
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
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		ctx.BoolRes = true
		return errors.New(fmt.Sprintf("index for column %d not found", colIdx))
	}
	var err error
	for _, idx := range idxes {
		if holder.self.Indices[idx] == nil {
			continue
		}
		node := holder.self.Indices[idx].GetManagedNode()
		index := node.DataNode.(Index)
		index.IndexFile().Ref()
		err = index.Eval(ctx)
		if err != nil {
			node.Close()
			index.IndexFile().Unref()
			return err
		}
		index.IndexFile().Unref()
		node.Close()
	}
	return nil
}

func (holder *SegmentHolder) CollectMinMax(colIdx int) (min []interface{}, max []interface{}, err error) {
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		return nil, nil, errors.New("no index found")
	}

	for _, idx := range idxes {
		if holder.self.Indices[idx] == nil {
			continue
		}
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
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		return 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		if holder.self.Indices[idx] == nil {
			continue
		}
		node := holder.self.Indices[idx].GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			index.IndexFile().Ref()
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
				index.IndexFile().Unref()
				return count, err
			}
			index.IndexFile().Unref()
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
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		return 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		if holder.self.Indices[idx] == nil {
			continue
		}
		node := holder.self.Indices[idx].GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			index.IndexFile().Ref()
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
				index.IndexFile().Unref()
				return count, err
			}
			index.IndexFile().Unref()
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
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		return 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		if holder.self.Indices[idx] == nil {
			continue
		}
		node := holder.self.Indices[idx].GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			index.IndexFile().Ref()
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
				index.IndexFile().Unref()
				return min, err
			}
			index.IndexFile().Unref()
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
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		return 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		if holder.self.Indices[idx] == nil {
			continue
		}
		node := holder.self.Indices[idx].GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			index.IndexFile().Ref()
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
				index.IndexFile().Unref()
				return max, err
			}
			index.IndexFile().Unref()
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
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		return 0, 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		if holder.self.Indices[idx] == nil {
			continue
		}
		node := holder.self.Indices[idx].GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			index.IndexFile().Ref()
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
				index.IndexFile().Unref()
				return res, cnt, nil
			}
			index.IndexFile().Unref()
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

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
)

type sortedSegmentHolder struct {
	common.RefHelper
	ID     common.ID
	BufMgr mgrif.IBufferManager
	Inited           bool
	versionAllocator ColumnsAllocator
	self             struct {
		sync.RWMutex
		colIndices    map[int][]*Node
		loadedVersion map[int]uint64
		droppedVersion map[int]uint64
		fileHelper     map[string]*Node
	}
	PostCloseCB PostCloseCB
}

func newSortedSegmentHolder(bufMgr mgrif.IBufferManager, id common.ID, cb PostCloseCB) SegmentIndexHolder {
	holder := &sortedSegmentHolder{ID: id, BufMgr: bufMgr, PostCloseCB: cb}
	holder.self.colIndices = make(map[int][]*Node)
	holder.self.loadedVersion = make(map[int]uint64)
	holder.self.fileHelper = make(map[string]*Node)
	holder.self.droppedVersion = make(map[int]uint64)
	holder.versionAllocator = ColumnsAllocator{
		RWMutex:    sync.RWMutex{},
		Allocators: make(map[int]*common.IdAlloctor),
	}
	holder.OnZeroCB = holder.close
	holder.Ref()
	return holder
}

func (holder *sortedSegmentHolder) HolderType() base.SegmentType {
	return base.SORTED_SEG
}

func (holder *sortedSegmentHolder) GetID() common.ID {
	return holder.ID
}

func (holder *sortedSegmentHolder) GetCB() PostCloseCB {
	return holder.PostCloseCB
}

func (holder *sortedSegmentHolder) Init(segFile base.ISegmentFile) {
	holder.self.Lock()
	defer holder.self.Unlock()
	if holder.Inited {
		panic("logic error")
	}
	indicesMeta := segFile.GetIndicesMeta()
	if indicesMeta == nil {
		return
	}

	// init embed segment indices
	for _, meta := range indicesMeta.Data {
		vf := segFile.MakeVirtualIndexFile(meta)
		col := int(meta.Cols.ToArray()[0])
		var node *Node
		switch meta.Type {
		case base.ZoneMap:
			node = newNode(holder.BufMgr, vf, false, SegmentZoneMapIndexConstructor, meta.Cols, nil)
		default:
			panic("unsupported embedded index type")
		}
		idxes, ok := holder.self.colIndices[col]
		if !ok {
			idxes = make([]*Node, 0)
			holder.self.colIndices[col] = idxes
		}
		holder.self.colIndices[col] = append(holder.self.colIndices[col], node)
		logutil.Infof("[SEG] Zone map load successfully, current indices count for column %d: %d | %s", col, len(holder.self.colIndices[col]), holder.ID.SegmentString())
	}
	holder.Inited = true
}

func (holder *sortedSegmentHolder) EvalFilter(colIdx int, ctx *FilterCtx) error {
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.colIndices[colIdx]
	if !ok || len(idxes) == 0 {
		ctx.BoolRes = true
		return errors.New(fmt.Sprintf("index for column %d not found", colIdx))
	}
	var err error
	hasBsi := false
	for _, idx := range idxes {
		node := idx.GetManagedNode()
		index := node.DataNode.(Index)
		if index.Type() != base.ZoneMap && !ctx.BsiRequired {
			if err := node.Close(); err != nil {
				return err
			}
			continue
		}
		if index.IndexFile().RefCount() == 0 {
			if err := node.Close(); err != nil {
				return err
			}
			continue
		}
		index.IndexFile().Ref()
		err = index.Eval(ctx)
		if err != nil {
			node.Close()
			index.IndexFile().Unref()
			return err
		}
		if index.Type() == base.NumBsi || index.Type() == base.FixStrBsi {
			hasBsi = true
		}
		index.IndexFile().Unref()
		node.Close()
	}
	if ctx.BsiRequired && !hasBsi {
		return errors.New("bsi not found for this column")
	}
	return nil
}

func (holder *sortedSegmentHolder) CollectMinMax(colIdx int) (min []interface{}, max []interface{}, err error) {
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.colIndices[colIdx]
	if !ok || len(idxes) == 0 {
		return nil, nil, errors.New("no index found")
	}

	for _, idx := range idxes {
		node := idx.GetManagedNode()
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

func (holder *sortedSegmentHolder) Count(colIdx int, filter *roaring.Bitmap) (uint64, error) {
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.colIndices[colIdx]
	if !ok || len(idxes) == 0 {
		return 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		node := idx.GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			if index.IndexFile().RefCount() == 0 {
				if err := node.Close(); err != nil {
					return 0, err
				}
				continue
			}
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

func (holder *sortedSegmentHolder) NullCount(colIdx int, max uint64, filter *roaring.Bitmap) (uint64, error) {
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.colIndices[colIdx]
	if !ok || len(idxes) == 0 {
		return 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		node := idx.GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			if index.IndexFile().RefCount() == 0 {
				if err := node.Close(); err != nil {
					return 0, err
				}
				continue
			}
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

func (holder *sortedSegmentHolder) Min(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.colIndices[colIdx]
	if !ok || len(idxes) == 0 {
		return 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		node := idx.GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			if index.IndexFile().RefCount() == 0 {
				if err := node.Close(); err != nil {
					return 0, err
				}
				continue
			}
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

func (holder *sortedSegmentHolder) Max(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.colIndices[colIdx]
	if !ok || len(idxes) == 0 {
		return 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		node := idx.GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			if index.IndexFile().RefCount() == 0 {
				if err := node.Close(); err != nil {
					return 0, err
				}
				continue
			}
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

func (holder *sortedSegmentHolder) Sum(colIdx int, filter *roaring.Bitmap) (int64, uint64, error) {
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.colIndices[colIdx]
	if !ok || len(idxes) == 0 {
		return 0, 0, errors.New("no index found")
	}

	for _, idx := range idxes {
		node := idx.GetManagedNode()
		if node.DataNode.(Index).Type() == base.NumBsi {
			index := node.DataNode.(*NumericBsiIndex)
			if index.IndexFile().RefCount() == 0 {
				if err := node.Close(); err != nil {
					return 0, 0, err
				}
				continue
			}
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

// StrongRefBlock is not supported in sortedSegmentHolder
func (holder *sortedSegmentHolder) StrongRefBlock(id uint64) *BlockIndexHolder {
	panic("unsupported")
}

// RegisterBlock is not supported in sortedSegmentHolder
func (holder *sortedSegmentHolder) RegisterBlock(id common.ID, blockType base.BlockType, cb PostCloseCB) *BlockIndexHolder {
	panic("unsupported")
}

// DropBlock is not supported in sortedSegmentHolder
func (holder *sortedSegmentHolder) DropBlock(id uint64) *BlockIndexHolder {
	panic("unsupported")
}

// GetBlockCount is not supported in sortedSegmentHolder
func (holder *sortedSegmentHolder) GetBlockCount() int32 {
	panic("unsupported")
}

// stringNoLock is not supported in sortedSegmentHolder
func (holder *sortedSegmentHolder) stringNoLock() string {
	panic("unsupported")
}

func (holder *sortedSegmentHolder) AllocateVersion(colIdx int) uint64 {
	holder.versionAllocator.Lock()
	defer holder.versionAllocator.Unlock()
	if holder.versionAllocator.Allocators[colIdx] == nil {
		holder.versionAllocator.Allocators[colIdx] = common.NewIdAlloctor(uint64(1))
	}
	return holder.versionAllocator.Allocators[colIdx].Alloc()
}

func (holder *sortedSegmentHolder) FetchCurrentVersion(col uint16, blkId uint64) uint64 {
	var currVersion uint64
	holder.versionAllocator.RLock()
	defer holder.versionAllocator.RUnlock()
	if alloc, ok := holder.versionAllocator.Allocators[int(col)]; ok {
		currVersion = alloc.Get()
	} else {
		currVersion = uint64(1)
	}
	return currVersion
}

func (holder *sortedSegmentHolder) IndicesCount() int {
	holder.self.RLock()
	defer holder.self.RUnlock()
	cnt := 0
	for _, nodes := range holder.self.colIndices {
		cnt += len(nodes)
	}
	return cnt
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
func (holder *sortedSegmentHolder) DropIndex(filename string) {
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
				//logutil.Infof("%s\n%+v", staleName, holder.self.fileHelper)
				node := holder.self.fileHelper[staleName]
				idxes := holder.self.colIndices[int(col)]
				for i, idx := range idxes {
					if idx == node {
						if i == len(idxes) - 1 {
							idxes = idxes[:len(idxes)-1]
						} else {
							idxes = append(idxes[:i], idxes[i+1:]...)
						}
						break
					}
				}
				node.VFile.Unref()
				delete(holder.self.fileHelper, staleName)
				delete(holder.self.loadedVersion, int(col))
				holder.self.droppedVersion[int(col)] = version
				logutil.Infof("[SEG] dropping newest index explicitly | version-%d", version)
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
func (holder *sortedSegmentHolder) LoadIndex(segFile base.ISegmentFile, filename string) {
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
				logutil.Infof("[SEG] detect stale index, version: %d, already dropped v-%d, file: %s", version, dropped, filepath.Base(filename))
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
				node := holder.self.fileHelper[staleName]
				idxes := holder.self.colIndices[int(col)]
				for i, idx := range idxes {
					if idx == node {
						if i == len(idxes) - 1 {
							idxes = idxes[:len(idxes) - 1]
						} else {
							idxes = append(idxes[:i], idxes[i+1:]...)
						}
						break
					}
				}
				node.VFile.Unref()
				delete(holder.self.fileHelper, staleName)
				logutil.Infof("[SEG] dropping stale index implicitly | version-%d", v)
				isLatest = true
			} else {
				// stale index, but not allocate resource yet, simply remove physical file
				isLatest = false
				if err := os.Remove(filename); err != nil {
					panic(err)
				}
				logutil.Infof("[SEG] loading stale index | %s received, but v-%d already loaded", filename, v)
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
			idxes, ok := holder.self.colIndices[col]
			if !ok {
				idxes = make([]*Node, 0)
				holder.self.colIndices[col] = idxes
			}
			holder.self.colIndices[col] = append(holder.self.colIndices[col], node)
			// holder.self.indexNodes = append(holder.self.indexNodes, node)
			holder.self.fileHelper[filepath.Base(filename)] = node
			logutil.Infof("[SEG] BSI load successfully, current indices count for column %d: %d | %s", col, len(holder.self.colIndices[col]), holder.ID.SegmentString())
			return
		}
		return
	}
	panic("unexpected error")
}

func (holder *sortedSegmentHolder) StringIndicesRefsNoLock() string {
	s := fmt.Sprintf("<SEGHOLDER[%s]>\n", holder.ID.SegmentString())
	for i, idxes := range holder.self.colIndices {
		s += fmt.Sprintf("<Column[%d]>", i)
		for j, idx := range idxes {
			s += fmt.Sprintf("<Index[%d]>[Ref cnt=%d]\n", j, idx.RefCount())
		}
	}
	return s
}

func (holder *sortedSegmentHolder) close() {
	for _, idxes := range holder.self.colIndices {
		for _, idx := range idxes {
			idx.Unref()
		}
	}
	if holder.PostCloseCB != nil {
		holder.PostCloseCB(holder)
	}
}


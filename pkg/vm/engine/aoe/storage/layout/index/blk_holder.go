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

type BlockIndexHolder struct {
	common.RefHelper
	ID common.ID
	versionAllocator ColumnsAllocator
	self struct {
		sync.RWMutex
		colIndices    map[int][]*Node
		loadedVersion map[int]uint64
		droppedVersion map[int]uint64
		fileHelper     map[string]*Node
    }
	BufMgr      mgrif.IBufferManager
	Inited      bool
	PostCloseCB PostCloseCB
}

func newBlockIndexHolder(bufMgr mgrif.IBufferManager, id common.ID, t base.BlockType, cb PostCloseCB) *BlockIndexHolder {
	holder := &BlockIndexHolder{
		ID:          id,
		BufMgr:      bufMgr,
		PostCloseCB: cb,
	}
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

func (holder *BlockIndexHolder) Init(segFile base.ISegmentFile) {
	holder.self.Lock()
	defer holder.self.Unlock()
	if holder.Inited {
		panic("logic error")
	}
	indicesMeta := segFile.GetBlockIndicesMeta(holder.ID)
	if indicesMeta == nil || len(indicesMeta.Data) == 0 {
		return
	}
	// init embed segment indices
	for _, meta := range indicesMeta.Data {
		vf := segFile.MakeVirtualBlkIndexFile(holder.ID.AsBlockID(), meta)
		col := int(meta.Cols.ToArray()[0])
		var node *Node
		switch meta.Type {
		case base.ZoneMap:
			node = newNode(holder.BufMgr, vf, false, BlockZoneMapIndexConstructor, meta.Cols, nil)
		default:
			panic("unsupported embedded index type")
		}
		idxes, ok := holder.self.colIndices[col]
		if !ok {
			idxes = make([]*Node, 0)
			holder.self.colIndices[col] = idxes
		}
		holder.self.colIndices[col] = append(holder.self.colIndices[col], node)
		logutil.Infof("[BLK] Zone map load successfully, current indices count for column %d: %d | %s", col, len(holder.self.colIndices[col]), holder.ID.BlockString())
	}
	holder.Inited = true
}

func (holder *BlockIndexHolder) EvalFilter(colIdx int, ctx *FilterCtx) error {
	holder.self.RLock()
	defer holder.self.RUnlock()
	idxes, ok := holder.self.colIndices[colIdx]
	if !ok {
		// no indices found, just skip the block.
		ctx.BoolRes = true
		return nil
	}
	var err error
	for _, idx := range idxes {
		node := idx.GetManagedNode()
		index := node.DataNode.(Index)
		if !ctx.BsiRequired {
			if index.Type() != base.ZoneMap {
				node.Close()
				continue
			}
		} else {
			if index.Type() != base.NumBsi {
				node.Close()
				continue
			}
		}
		err = index.Eval(ctx)
		if err != nil {
			node.Close()
			return err
		}
		node.Close()
	}
	return nil
}

func (holder *BlockIndexHolder) close() {
	for _, idxes := range holder.self.colIndices {
		for _, idx := range idxes {
			idx.Unref()
		}
	}
	if holder.PostCloseCB != nil {
		holder.PostCloseCB(holder)
	}
}

func (holder *BlockIndexHolder) stringNoLock() string {
	//s := fmt.Sprintf("<IndexBlkHolder[%s]>[Ty=%v](Cnt=%d)(RefCount=%d)", holder.ID.BlockString(), holder.Type, len(holder.Indices), holder.RefCount())
	//for _, i := range holder.Indices {
	//	s = fmt.Sprintf("%s\n\tIndex: [RefCount=%d]", s, i.RefCount())
	//}
	// s = fmt.Sprintf("%s\n%vs, holder.colIndices)
	// TODO(zzl)
	return ""
}

func (holder *BlockIndexHolder) AllocateVersion(colIdx int) uint64 {
	holder.versionAllocator.Lock()
	defer holder.versionAllocator.Unlock()
	if holder.versionAllocator.Allocators[colIdx] == nil {
		holder.versionAllocator.Allocators[colIdx] = common.NewIdAlloctor(uint64(1))
	}
	return holder.versionAllocator.Allocators[colIdx].Alloc()
}

func (holder *BlockIndexHolder) fetchCurrentVersion(col uint16) uint64 {
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

func (holder *BlockIndexHolder) IndicesCount() int {
	holder.self.RLock()
	defer holder.self.RUnlock()
	cnt := 0
	for _, nodes := range holder.self.colIndices {
		cnt += len(nodes)
	}
	return cnt
}

func (holder *BlockIndexHolder) DropIndex(filename string) {
	holder.self.Lock()
	defer holder.self.Unlock()
	name, _ := common.ParseBlockBitSlicedIndexFileName(filepath.Base(filename))
	version, tid, sid, bid, col, _ := common.ParseBlockBitSlicedIndexFileNameToInfo(name)
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
			staleName := common.MakeBlockBitSlicedIndexFileName(version, tid, sid, bid, col)
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
			logutil.Infof("[BLK] dropping newest index explicitly | version-%d", version)
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

func (holder *BlockIndexHolder) LoadIndex(segFile base.ISegmentFile, filename string) {
	holder.self.Lock()
	defer holder.self.Unlock()
	name, _ := common.ParseBlockBitSlicedIndexFileName(filepath.Base(filename))
	version, tid, sid, bid, col, _ := common.ParseBlockBitSlicedIndexFileNameToInfo(name)

	isLatest := false
	if dropped, ok := holder.self.droppedVersion[int(col)]; ok {
		if dropped >= version {
			// newer version used to be loaded, but dropped explicitly
			// or dropped speculatively in the past.
			isLatest = false
			if err := os.Remove(filename); err != nil {
				panic(err)
			}
			logutil.Infof("[BLK] detect stale index, version: %d, already dropped v-%d, file: %s", version, dropped, filepath.Base(filename))
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
			staleName := common.MakeBlockBitSlicedIndexFileName(v, tid, sid, bid, col)
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
			logutil.Infof("[BLK] dropping stale index implicitly | version-%d", v)
			isLatest = true
		} else {
			// stale index, but not allocate resource yet, simply remove physical file
			isLatest = false
			if err := os.Remove(filename); err != nil {
				panic(err)
			}
			logutil.Infof("[BLK] loading stale index | %s received, but v-%d already loaded", filename, v)
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
		id.BlockID = bid
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
		logutil.Infof("[BLK] BSI load successfully, current indices count for column %d: %d | %s", col, len(holder.self.colIndices[col]), holder.ID.SegmentString())
		return
	}
}

func (holder *BlockIndexHolder) StringIndicesRefsNoLock() string {
	// TODO(zzl)
	return ""
}

func (holder *BlockIndexHolder) Count(colIdx int, filter *roaring.Bitmap) (uint64, error) {
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

func (holder *BlockIndexHolder) NullCount(colIdx int, maxRows uint64, filter *roaring.Bitmap) (uint64, error) {
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
			count = maxRows - count
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

func (holder *BlockIndexHolder) Min(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
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

func (holder *BlockIndexHolder) Max(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
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

func (holder *BlockIndexHolder) Sum(colIdx int, filter *roaring.Bitmap) (int64, uint64, error) {
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


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
	"fmt"
	mgrif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"sync"
	"sync/atomic"
	// log "github.com/sirupsen/logrus"
)

type TableHolder struct {
	common.RefHelper
	ID     uint64
	BufMgr mgrif.IBufferManager
	tree   struct {
		sync.RWMutex
		Segments   []SegmentIndexHolder
		IdMap      map[uint64]int
		SegmentCnt int64
	}
}

func NewTableHolder(bufMgr mgrif.IBufferManager, id uint64) *TableHolder {
	holder := &TableHolder{ID: id, BufMgr: bufMgr}
	holder.tree.Segments = make([]SegmentIndexHolder, 0)
	holder.tree.IdMap = make(map[uint64]int)
	holder.OnZeroCB = holder.close
	holder.Ref()
	return holder
}

func (holder *TableHolder) RegisterSegment(id common.ID, segType base.SegmentType, cb PostCloseCB) SegmentIndexHolder {
	//segHolder := newSegmentHolder(holder.BufMgr, id, segType, cb)
	var segHolder SegmentIndexHolder
	if segType == base.SORTED_SEG {
		segHolder = newSortedSegmentHolder(holder.BufMgr, id, cb)
	} else if segType == base.UNSORTED_SEG {
		segHolder = newUnsortedSegmentHolder(holder.BufMgr, id, cb)
	} else {
		panic("logic error")
	}
	holder.addSegment(segHolder)
	segHolder.Ref()
	return segHolder
}

func (holder *TableHolder) addSegment(seg SegmentIndexHolder) {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	_, ok := holder.tree.IdMap[seg.GetID().SegmentID]
	if ok {
		panic("duplicate segment")
		//panic(fmt.Sprintf("Duplicate seg %s", seg.GetID().SegmentString()))
	}
	holder.tree.IdMap[seg.GetID().SegmentID] = len(holder.tree.Segments)
	holder.tree.Segments = append(holder.tree.Segments, seg)
	atomic.AddInt64(&holder.tree.SegmentCnt, int64(1))
}

func (holder *TableHolder) DropSegment(id uint64) SegmentIndexHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		panic(fmt.Sprintf("Specified seg %d not found", id))
	}
	dropped := holder.tree.Segments[idx]
	delete(holder.tree.IdMap, id)
	holder.tree.Segments = append(holder.tree.Segments[:idx], holder.tree.Segments[idx+1:]...)
	atomic.AddInt64(&holder.tree.SegmentCnt, int64(-1))
	return dropped
}

func (holder *TableHolder) GetSegmentCount() int64 {
	return atomic.LoadInt64(&holder.tree.SegmentCnt)
}

func (holder *TableHolder) String() string {
	holder.tree.RLock()
	defer holder.tree.RUnlock()
	s := fmt.Sprintf("<IndexTableHolder[%d]>[Cnt=%d](RefCount=%d)", holder.ID, holder.tree.SegmentCnt, holder.RefCount())
	for _, seg := range holder.tree.Segments {
		s = fmt.Sprintf("%s\n\t%s", s, seg.stringNoLock())
	}
	return s
}

func (holder *TableHolder) StringIndicesRefs() string {
	holder.tree.RLock()
	defer holder.tree.RUnlock()
	s := ""
	for _, seg := range holder.tree.Segments {
		if seg.HolderType() == base.UNSORTED_SEG {
			continue
		}
		s += seg.StringIndicesRefsNoLock()
	}
	return s
}

func (holder *TableHolder) close() {
	for _, seg := range holder.tree.Segments {
		seg.Unref()
	}
}

func (holder *TableHolder) UpgradeSegment(id uint64, segType base.SegmentType) SegmentIndexHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		panic(fmt.Sprintf("specified seg %d not found in %d", id, holder.ID))
	}
	stale := holder.tree.Segments[idx]
	if stale.HolderType() >= segType {
		panic(fmt.Sprintf("Cannot upgrade segment %d, type %d", id, segType))
	}
	//newSeg := newSegmentHolder(holder.BufMgr, stale.ID, segType, stale.PostCloseCB)
	//holder.tree.Segments[idx] = newSeg
	//newSeg.Ref()
	newHolder := newSortedSegmentHolder(holder.BufMgr, stale.GetID(), stale.GetCB())
	holder.tree.Segments[idx] = newHolder
	newHolder.Ref()
	stale.Unref()
	return newHolder
}

func (holder *TableHolder) StrongRefSegment(id uint64) (seg SegmentIndexHolder) {
	holder.tree.RLock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		holder.tree.RUnlock()
		return nil
	}
	seg = holder.tree.Segments[idx]
	holder.tree.RUnlock()
	seg.Ref()
	return seg
}

// func (holder *TableHolder) Close() error {
// 	holder.tree.Lock()
// 	defer holder.tree.Unlock()
// 	for _, seg := range holder.tree.Segments {
// 		seg.Unref()
// 	}
// 	return nil
// }

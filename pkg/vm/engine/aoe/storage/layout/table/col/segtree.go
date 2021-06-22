package col

import (
	"errors"
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/index"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"runtime"

	"sync"

	log "github.com/sirupsen/logrus"
)

type ISegmentTree interface {
	// sync.Locker
	// RLock()
	// RUnlock()
	String() string
	ToString(depth uint64) string
	GetRoot() IColumnSegment
	GetTail() IColumnSegment
	Depth() uint64
	GetSegment(common.ID) IColumnSegment
	// ReferenceOther(other ISegmentTree)

	// Modifier
	Append(seg IColumnSegment) error
	UpgradeBlock(*md.Block) IColumnBlock
	UpgradeSegment(segID common.ID) IColumnSegment
	DropSegment(id common.ID) (seg IColumnSegment, err error)
}

type SegmentTree struct {
	IndexHolder *index.TableHolder
	data        struct {
		sync.RWMutex
		Segments []IColumnSegment
		Helper   map[common.ID]int
	}
}

func NewSegmentTree(indexHolder *index.TableHolder) ISegmentTree {
	tree := &SegmentTree{IndexHolder: indexHolder}
	tree.data.Segments = make([]IColumnSegment, 0)
	tree.data.Helper = make(map[common.ID]int)
	runtime.SetFinalizer(tree, func(o *SegmentTree) {
		log.Infof("[GC]: SegmentTree: %s", o.String())
		o.data.Segments = nil
	})
	return tree
}

func (tree *SegmentTree) DropSegment(id common.ID) (seg IColumnSegment, err error) {
	idx, ok := tree.data.Helper[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Specified seg %s not found", id.SegmentString()))
	}
	seg = tree.data.Segments[idx]
	tree.data.Lock()
	defer tree.data.Unlock()
	if idx > 0 {
		prev := tree.data.Segments[idx-1]
		prev.SetNext(seg.GetNext())
	}
	delete(tree.data.Helper, id)
	tree.data.Segments = append(tree.data.Segments[:idx], tree.data.Segments[idx+1:]...)
	for idx, segment := range tree.data.Segments {
		tree.data.Helper[segment.GetID()] = idx
	}
	return seg, nil
}

func (tree *SegmentTree) Depth() uint64 {
	tree.data.RLock()
	defer tree.data.RUnlock()
	return uint64(len(tree.data.Segments))
}

func (tree *SegmentTree) GetRoot() IColumnSegment {
	tree.data.RLock()
	defer tree.data.RUnlock()
	if len(tree.data.Segments) == 0 {
		return nil
	}
	return tree.data.Segments[0].Ref()
}

func (tree *SegmentTree) GetTail() IColumnSegment {
	tree.data.RLock()
	defer tree.data.RUnlock()
	if len(tree.data.Segments) == 0 {
		return nil
	}
	return tree.data.Segments[len(tree.data.Segments)-1].Ref()
}

func (tree *SegmentTree) GetSegment(segID common.ID) IColumnSegment {
	tree.data.RLock()
	idx, ok := tree.data.Helper[segID]
	defer tree.data.RUnlock()
	if !ok {
		return nil
	}
	return tree.data.Segments[idx].Ref()
}

func (tree *SegmentTree) UpgradeBlock(newMeta *md.Block) IColumnBlock {
	blkId := newMeta.AsCommonID()
	idx, ok := tree.data.Helper[blkId.AsSegmentID()]
	if !ok {
		panic("logic error")
	}
	seg := tree.data.Segments[idx]
	blk, err := seg.UpgradeBlock(newMeta)
	if err != nil {
		panic(fmt.Sprintf("logic error: %s", err))
	}
	return blk
}

func (tree *SegmentTree) UpgradeSegment(segID common.ID) IColumnSegment {
	idx, ok := tree.data.Helper[segID]
	if !ok {
		panic("logic error")
	}
	seg := tree.data.Segments[idx]

	if seg.GetSegmentType() != base.UNSORTED_SEG {
		panic("logic error")
	}
	if !segID.IsSameSegment(seg.GetID()) {
		panic("logic error")
	}

	upgradeSeg := seg.CloneWithUpgrade(seg.GetMeta(), tree.IndexHolder)
	if upgradeSeg == nil {
		panic(fmt.Sprintf("Cannot upgrade seg: %s", segID.SegmentString()))
	}
	var old_next IColumnSegment
	if idx != len(tree.data.Segments)-1 {
		old_next = seg.GetNext()
	}
	upgradeSeg.SetNext(old_next)
	tree.data.Lock()
	defer tree.data.Unlock()
	tree.data.Segments[idx] = upgradeSeg
	// if old_next != nil {
	// 	seg.SetNext(nil)
	// }
	if idx > 0 {
		tree.data.Segments[idx-1].SetNext(upgradeSeg.Ref())
	}
	seg.UnRef()
	return upgradeSeg.Ref()
}

func (tree *SegmentTree) Append(seg IColumnSegment) error {
	tree.data.Lock()
	defer tree.data.Unlock()
	_, ok := tree.data.Helper[seg.GetID()]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate seg %v in tree", seg.GetID()))
	}
	if len(tree.data.Segments) != 0 {
		tree.data.Segments[len(tree.data.Segments)-1].SetNext(seg.Ref())
	}
	tree.data.Segments = append(tree.data.Segments, seg)
	tree.data.Helper[seg.GetID()] = len(tree.data.Segments) - 1
	return nil
}

func (tree *SegmentTree) String() string {
	depth := tree.Depth()
	if depth > 10 {
		depth = 10
	}
	return tree.ToString(depth)
}

func (tree *SegmentTree) ToString(depth uint64) string {
	if depth > tree.Depth() {
		depth = tree.Depth()
	}
	ret := fmt.Sprintf("SegTree (%v/%v) [", depth, tree.Depth())
	for i := uint64(0); i < depth; i++ {
		ret = fmt.Sprintf("%s\n%s", ret, tree.data.Segments[i].ToString(true))
	}

	ret = fmt.Sprintf("%s\n]", ret)

	return ret
}

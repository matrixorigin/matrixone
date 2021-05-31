package md

import (
	"errors"
	"fmt"
)

const (
	SEGMENT_BLOCK_COUNT = 4
)

func NewSegment(info *MetaInfo, table_id, id uint64) *Segment {
	seg := &Segment{
		ID:            id,
		TableID:       table_id,
		Blocks:        make(map[uint64]*Block),
		TimeStamp:     *NewTimeStamp(),
		MaxBlockCount: SEGMENT_BLOCK_COUNT,
		Info:          info,
	}
	return seg
}

func (seg *Segment) GetTableID() uint64 {
	return seg.TableID
}

func (seg *Segment) GetID() uint64 {
	return seg.ID
}

func (seg *Segment) BlockIDs(args ...interface{}) map[uint64]uint64 {
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0].(int64)
	}
	ids := make(map[uint64]uint64)
	seg.RLock()
	defer seg.RUnlock()
	for _, blk := range seg.Blocks {
		if !blk.Select(ts) {
			continue
		}
		ids[blk.ID] = blk.ID
	}
	return ids
}

func (seg *Segment) CreateBlock() (blk *Block, err error) {
	blk = NewBlock(seg.TableID, seg.ID, seg.Info.Sequence.GetBlockID(), seg.Info.Conf.BlockMaxRows)
	return blk, err
}

func (seg *Segment) String() string {
	s := fmt.Sprintf("Seg(%d-%d)", seg.TableID, seg.ID)
	s += "["
	pos := 0
	for _, blk := range seg.Blocks {
		if pos != 0 {
			s += "<-->"
		}
		s += blk.String()
		pos++
	}
	s += "]"
	return s
}

func (seg *Segment) CloneBlock(id uint64) (blk *Block, err error) {
	seg.RLock()
	defer seg.RUnlock()
	rblk, ok := seg.Blocks[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("block %d not found in segment %d", id, seg.ID))
	}
	blk = rblk.Copy()
	err = blk.Detach()
	return blk, err
}

func (seg *Segment) ReferenceBlock(id uint64) (blk *Block, err error) {
	seg.RLock()
	defer seg.RUnlock()
	blk, ok := seg.Blocks[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("block %d not found in segment %d", id, seg.ID))
	}
	return blk, nil
}

func (seg *Segment) SetInfo(info *MetaInfo) error {
	if seg.Info == nil {
		seg.Info = info
		// for _, blk := range seg.Blocks {
		// 	if blk.Info == nil {
		// 		blk.Info = info
		// 	}
		// }
	}

	return nil
}

func (seg *Segment) RegisterBlock(blk *Block) error {
	if blk.TableID != seg.TableID {
		return errors.New(fmt.Sprintf("table id mismatch %d:%d", seg.TableID, blk.TableID))
	}
	if blk.GetSegmentID() != seg.GetID() {
		return errors.New(fmt.Sprintf("segment id mismatch %d:%d", seg.GetID(), blk.GetSegmentID()))
	}
	seg.Lock()
	defer seg.Unlock()

	err := blk.Attach()
	if err != nil {
		return err
	}
	if len(seg.Blocks) == int(seg.MaxBlockCount) {
		return errors.New(fmt.Sprintf("Cannot add block into full segment %d", seg.ID))
	}
	_, ok := seg.Blocks[blk.ID]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate block %d found in segment %d", blk.GetID(), seg.ID))
	}
	seg.Blocks[blk.GetID()] = blk
	if len(seg.Blocks) == int(seg.MaxBlockCount) {
		if blk.IsFull() {
			seg.DataState = CLOSED
		} else {
			seg.DataState = FULL
		}
	} else {
		seg.DataState = PARTIAL
	}
	return nil
}

func (seg *Segment) TryClose() bool {
	seg.Lock()
	defer seg.Unlock()
	if seg.DataState == CLOSED || seg.DataState == SORTED {
		return true
	}
	if seg.DataState == FULL || len(seg.Blocks) == int(seg.MaxBlockCount) {
		for _, blk := range seg.Blocks {
			if !blk.IsFull() {
				return false
			}
		}
		return true
	}
	return false
}

func (seg *Segment) GetMaxBlkID() uint64 {
	blkid := uint64(0)
	for bid, _ := range seg.Blocks {
		if bid > blkid {
			blkid = bid
		}
	}

	return blkid
}

func (seg *Segment) Copy(ts ...int64) *Segment {
	var t int64
	if len(ts) == 0 {
		t = NowMicro()
	} else {
		t = ts[0]
	}
	new_seg := NewSegment(seg.Info, seg.TableID, seg.ID)
	new_seg.TimeStamp = seg.TimeStamp
	new_seg.MaxBlockCount = seg.MaxBlockCount
	new_seg.DataState = seg.DataState
	for k, v := range seg.Blocks {
		if !v.Select(t) {
			continue
		}
		blk, _ := seg.CloneBlock(v.ID)
		new_seg.Blocks[k] = blk
	}

	return new_seg
}

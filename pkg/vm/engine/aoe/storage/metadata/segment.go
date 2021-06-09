package md

import (
	"errors"
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage/common"
)

const (
	SEGMENT_BLOCK_COUNT = 4
)

func NewSegment(info *MetaInfo, table_id, id uint64, schema *Schema) *Segment {
	seg := &Segment{
		ID:            id,
		TableID:       table_id,
		Blocks:        make([]*Block, 0),
		IdMap:         make(map[uint64]int),
		TimeStamp:     *NewTimeStamp(),
		MaxBlockCount: info.Conf.SegmentMaxBlocks,
		Info:          info,
		Schema:        schema,
	}
	return seg
}

func (seg *Segment) GetTableID() uint64 {
	return seg.TableID
}

func (seg *Segment) AsCommonID() *common.ID {
	return &common.ID{
		TableID:   seg.TableID,
		SegmentID: seg.ID,
	}
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
	blk = NewBlock(seg.Info.Sequence.GetBlockID(), seg)
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
	idx, ok := seg.IdMap[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("block %d not found in segment %d", id, seg.ID))
	}
	blk = seg.Blocks[idx].Copy()
	err = blk.Detach()
	return blk, err
}

func (seg *Segment) ReferenceBlock(id uint64) (blk *Block, err error) {
	seg.RLock()
	defer seg.RUnlock()
	idx, ok := seg.IdMap[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("block %d not found in segment %d", id, seg.ID))
	}
	return seg.Blocks[idx], nil
}

func (seg *Segment) SetInfo(info *MetaInfo) error {
	if seg.Info == nil {
		seg.Info = info
	}

	return nil
}

func (seg *Segment) RegisterBlock(blk *Block) error {
	if blk.Segment.TableID != seg.TableID {
		return errors.New(fmt.Sprintf("table id mismatch %d:%d", seg.TableID, blk.Segment.TableID))
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
	_, ok := seg.IdMap[blk.ID]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate block %d found in segment %d", blk.GetID(), seg.ID))
	}
	seg.IdMap[blk.GetID()] = len(seg.Blocks)
	seg.Blocks = append(seg.Blocks, blk)
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
	for bid, _ := range seg.IdMap {
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
	new_seg := NewSegment(seg.Info, seg.TableID, seg.ID, seg.Schema)
	new_seg.TimeStamp = seg.TimeStamp
	new_seg.MaxBlockCount = seg.MaxBlockCount
	new_seg.DataState = seg.DataState
	for _, v := range seg.Blocks {
		if !v.Select(t) {
			continue
		}
		blk, _ := seg.CloneBlock(v.ID)
		new_seg.IdMap[v.GetID()] = len(new_seg.Blocks)
		new_seg.Blocks = append(new_seg.Blocks, blk)
	}

	return new_seg
}

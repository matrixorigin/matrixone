package md

import (
	"errors"
	"fmt"
	// log "github.com/sirupsen/logrus"
)

func NewTable(info *MetaInfo, ids ...uint64) *Table {
	var id uint64
	if len(ids) == 0 {
		id = info.Sequence.GetTableID()
	} else {
		id = ids[0]
	}
	tbl := &Table{
		ID:        id,
		Segments:  make(map[uint64]*Segment),
		TimeStamp: *NewTimeStamp(),
		Info:      info,
	}
	return tbl
}

func (tbl *Table) GetID() uint64 {
	return tbl.ID
}

func (tbl *Table) CloneSegment(segment_id uint64, ts ...int64) (seg *Segment, err error) {
	tbl.RLock()
	seg, err = tbl.referenceSegmentNoLock(segment_id)
	if err != nil {
		tbl.RUnlock()
		return nil, err
	}
	tbl.RUnlock()
	seg = seg.Copy(ts...)
	err = seg.Detach()
	return seg, err
}

func (tbl *Table) ReferenceBlock(segment_id, block_id uint64) (blk *Block, err error) {
	tbl.RLock()
	seg, err := tbl.referenceSegmentNoLock(segment_id)
	if err != nil {
		tbl.RUnlock()
		return nil, err
	}
	tbl.RUnlock()

	blk, err = seg.ReferenceBlock(block_id)

	return blk, err
}

func (tbl *Table) ReferenceSegment(segment_id uint64) (seg *Segment, err error) {
	tbl.RLock()
	defer tbl.RUnlock()
	seg, err = tbl.referenceSegmentNoLock(segment_id)
	return seg, err
}

func (tbl *Table) referenceSegmentNoLock(segment_id uint64) (seg *Segment, err error) {
	seg, ok := tbl.Segments[segment_id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("specified segment %d not found in table %d", segment_id, tbl.ID))
	}
	return seg, nil
}

func (tbl *Table) GetSegmentBlockIDs(segment_id uint64, args ...int64) map[uint64]uint64 {
	tbl.RLock()
	seg, err := tbl.referenceSegmentNoLock(segment_id)
	tbl.RUnlock()
	if err == nil {
		return make(map[uint64]uint64, 0)
	}
	return seg.BlockIDs(args)
}

func (tbl *Table) SegmentIDs(args ...int64) map[uint64]uint64 {
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0]
	}
	ids := make(map[uint64]uint64)
	tbl.RLock()
	defer tbl.RUnlock()
	for _, seg := range tbl.Segments {
		if !seg.Select(ts) {
			continue
		}
		ids[seg.ID] = seg.ID
	}
	return ids
}

func (tbl *Table) SetInfo(info *MetaInfo) error {
	if tbl.Info == nil {
		tbl.Info = info
		for _, seg := range tbl.Segments {
			err := seg.SetInfo(info)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (tbl *Table) CreateSegment() (seg *Segment, err error) {
	seg = NewSegment(tbl.Info, tbl.ID, tbl.Info.Sequence.GetSegmentID())
	return seg, err
}

func (tbl *Table) GetInfullSegment() (seg *Segment, err error) {
	tbl.RLock()
	defer tbl.RUnlock()
	for _, seg := range tbl.Segments {
		if seg.DataState == EMPTY || seg.DataState == PARTIAL {
			return seg, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("no infull segment found in table %d", tbl.ID))
}

func (tbl *Table) String() string {
	s := fmt.Sprintf("Tbl(%d)", tbl.ID)
	s += "["
	for i, seg := range tbl.Segments {
		if i != 0 {
			s += "\n"
		}
		s += seg.String()
	}
	if len(tbl.Segments) > 0 {
		s += "\n"
	}
	s += "]"
	return s
}

func (tbl *Table) RegisterSegment(seg *Segment) error {
	if tbl.ID != seg.GetTableID() {
		return errors.New(fmt.Sprintf("table id mismatch %d:%d", tbl.ID, seg.GetTableID()))
	}
	tbl.Lock()
	defer tbl.Unlock()

	err := seg.Attach()
	if err != nil {
		return err
	}

	_, ok := tbl.Segments[seg.ID]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate segment %d found in table %d", seg.GetID(), tbl.ID))
	}
	tbl.Segments[seg.GetID()] = seg
	return nil
}

func (tbl *Table) GetMaxSegIDAndBlkID() (uint64, uint64) {
	blkid := uint64(0)
	segid := uint64(0)
	for sid, seg := range tbl.Segments {
		max_blkid := seg.GetMaxBlkID()
		if max_blkid > blkid {
			blkid = max_blkid
		}
		if sid > segid {
			segid = sid
		}
	}

	return segid, blkid
}

func (tbl *Table) Copy(ts ...int64) *Table {
	var t int64
	if len(ts) == 0 {
		t = NowMicro()
	} else {
		t = ts[0]
	}
	new_tbl := NewTable(tbl.Info, tbl.ID)
	new_tbl.TimeStamp = tbl.TimeStamp
	new_tbl.BoundSate = tbl.BoundSate
	for k, v := range tbl.Segments {
		if !v.Select(t) {
			continue
		}
		seg, _ := tbl.CloneSegment(v.ID)
		new_tbl.Segments[k] = seg
	}

	return new_tbl
}

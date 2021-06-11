package md

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync/atomic"
)

func NewTable(info *MetaInfo, schema *Schema, ids ...uint64) *Table {
	var id uint64
	if len(ids) == 0 {
		id = info.Sequence.GetTableID()
	} else {
		id = ids[0]
	}
	tbl := &Table{
		ID:        id,
		Segments:  make([]*Segment, 0),
		IdMap:     make(map[uint64]int),
		TimeStamp: *NewTimeStamp(),
		Info:      info,
		Schema:    schema,
	}
	return tbl
}

func (tbl *Table) GetID() uint64 {
	return tbl.ID
}

func (tbl *Table) CloneSegment(segment_id uint64, ctx CopyCtx) (seg *Segment, err error) {
	tbl.RLock()
	seg, err = tbl.referenceSegmentNoLock(segment_id)
	if err != nil {
		tbl.RUnlock()
		return nil, err
	}
	tbl.RUnlock()
	segCpy := seg.Copy(ctx)
	if !ctx.Attached {
		err = segCpy.Detach()
	}
	return segCpy, err
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
	idx, ok := tbl.IdMap[segment_id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("specified segment %d not found in table %d", segment_id, tbl.ID))
	}
	seg = tbl.Segments[idx]
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
	seg = NewSegment(tbl.Info, tbl.ID, tbl.Info.Sequence.GetSegmentID(), tbl.Schema)
	return seg, err
}

func (tbl *Table) NextActiveSegment() *Segment {
	var seg *Segment
	if tbl.ActiveSegment >= len(tbl.Segments) {
		return seg
	}
	tbl.ActiveSegment++
	return tbl.GetActiveSegment()
	// if tbl.ActiveSegment <= len(tbl.Segments)-1 {
	// 	seg = tbl.Segments[tbl.ActiveSegment]
	// }
	// return seg
}

func (tbl *Table) GetActiveSegment() *Segment {
	if tbl.ActiveSegment >= len(tbl.Segments) {
		return nil
	}
	seg := tbl.Segments[tbl.ActiveSegment]
	blk := seg.GetActiveBlk()
	if blk == nil && uint64(len(seg.Blocks)) == tbl.Info.Conf.SegmentMaxBlocks {
		return nil
	}
	return seg
	// for i := len(tbl.Segments) - 1; i >= 0; i-- {
	// 	seg := tbl.Segments[i]
	// 	if seg.DataState >= CLOSED {
	// 		break
	// 	} else if seg.DataState == EMPTY {
	// 		active = seg
	// 	} else if seg.DataState == PARTIAL {
	// 		active = seg
	// 		break
	// 	} else if seg.DataState == FULL {
	// 		activeBlk := seg.GetActiveBlock()
	// 		if activeBlk != nil {
	// 			active = seg
	// 		}
	// 		break
	// 	}
	// }
	// return active
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
	s := fmt.Sprintf("Tbl(%d) %d", tbl.ID, tbl.ActiveSegment)
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

	_, ok := tbl.IdMap[seg.ID]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate segment %d found in table %d", seg.GetID(), tbl.ID))
	}
	tbl.IdMap[seg.GetID()] = len(tbl.Segments)
	tbl.Segments = append(tbl.Segments, seg)
	atomic.StoreUint64(&tbl.SegmentCnt, uint64(len(tbl.Segments)))
	return nil
}

func (tbl *Table) GetSegmentCount() uint64 {
	return atomic.LoadUint64(&tbl.SegmentCnt)
}

func (tbl *Table) GetMaxSegIDAndBlkID() (uint64, uint64) {
	blkid := uint64(0)
	segid := uint64(0)
	for _, seg := range tbl.Segments {
		sid := seg.GetID()
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

func (tbl *Table) Copy(ctx CopyCtx) *Table {
	if ctx.Ts == 0 {
		ctx.Ts = NowMicro()
	}
	new_tbl := NewTable(tbl.Info, tbl.Schema, tbl.ID)
	new_tbl.TimeStamp = tbl.TimeStamp
	new_tbl.BoundSate = tbl.BoundSate
	for _, v := range tbl.Segments {
		if !v.Select(ctx.Ts) {
			continue
		}
		seg, _ := tbl.CloneSegment(v.ID, ctx)
		new_tbl.IdMap[seg.GetID()] = len(new_tbl.Segments)
		new_tbl.Segments = append(new_tbl.Segments, seg)
	}

	return new_tbl
}

func MockTable(info *MetaInfo, schema *Schema, blkCnt uint64) *Table {
	tbl, _ := info.CreateTable(schema)
	info.RegisterTable(tbl)
	var activeSeg *Segment
	for i := uint64(0); i < blkCnt; i++ {
		if activeSeg == nil {
			activeSeg, _ = tbl.CreateSegment()
			tbl.RegisterSegment(activeSeg)
		}
		blk, _ := activeSeg.CreateBlock()
		err := activeSeg.RegisterBlock(blk)
		if err != nil {
			log.Errorf("seg blks = %d, maxBlks = %d", len(activeSeg.Blocks), activeSeg.MaxBlockCount)
			panic(err)
		}
		if len(activeSeg.Blocks) == int(info.Conf.SegmentMaxBlocks) {
			activeSeg = nil
		}
	}
	return tbl
}

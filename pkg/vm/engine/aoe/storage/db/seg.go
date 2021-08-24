package db

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/process"
	"strconv"
	"sync/atomic"
)

type Segment struct {
	Data iface.ISegment
	Ids  *atomic.Value
}

func (seg *Segment) ID() string {
	id := seg.Data.GetMeta().GetID()
	return strconv.FormatUint(id, 10)
}

func (seg *Segment) Blocks() []string {
	if ids := seg.Ids.Load(); ids != nil {
		return ids.([]string)
	}
	ids := seg.Data.BlockIds()
	strs := make([]string, len(ids))
	for idx, id := range ids {
		strs[idx] = strconv.FormatUint(id, 10)
	}
	seg.Ids.Store(strs)
	return strs
}

func (seg *Segment) Block(id string, proc *process.Process) engine.Block {
	iid, _ := strconv.ParseUint(id, 10, 64)
	data := seg.Data.WeakRefBlock(iid)
	if data == nil {
		return nil
	}
	blk := &Block{
		StrId: id,
		Id:    iid,
		Host:  seg,
	}
	return blk
}

func (seg *Segment) NewFilter() engine.Filter {
	return NewSegmentFilter(seg)
}

func (seg *Segment) NewSummarizer() engine.Summarizer {
	return nil
}

func (seg *Segment) NewSparseFilter() engine.SparseFilter {
	return NewSegmentSparseFilter(seg)
}

func (seg *Segment) Rows() int64 {
	return int64(seg.Data.GetRowCount())
}

func (seg *Segment) Size(attr string) int64 {
	return int64(seg.Data.Size(attr))
}

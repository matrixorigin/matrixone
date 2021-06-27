package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	// "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
)

var (
	EmptySegmentIt = new(SegmentIt)
)

var (
	_ dbi.ISegmentIt = (*SegmentIt)(nil)
)

type SegmentIt struct {
	OnCloseCB CloseSegmentItCB
	Snapshot  *Snapshot
	Pos       int
}

func NewSegmentIt(ss *Snapshot) dbi.ISegmentIt {
	it := &SegmentIt{
		Snapshot:  ss,
		OnCloseCB: ss.removeIt,
	}
	return it
}

func (it *SegmentIt) Next() {
	it.Pos++
}

func (it *SegmentIt) Valid() bool {
	if it.Snapshot == nil {
		return false
	}
	if it.Snapshot.Ids == nil {
		return false
	}
	if it.Pos >= len(it.Snapshot.Ids) {
		return false
	}
	return true
}

func (it *SegmentIt) GetHandle() dbi.ISegment {
	seg := &Segment{
		Data: it.Snapshot.TableData.WeakRefSegment(it.Snapshot.Ids[it.Pos]),
		Attr: it.Snapshot.Attr,
	}
	return seg
}

func (it *SegmentIt) Close() error {
	if it.OnCloseCB != nil {
		it.OnCloseCB(it)
		it.OnCloseCB = nil
	}
	return nil
}

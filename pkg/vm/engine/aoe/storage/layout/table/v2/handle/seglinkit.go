package handle

import (
	hif "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/handle/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
)

type SegmentLinkIt struct {
	OnCloseCB CloseSegmentItCB
	Snapshot  *Snapshot
	Cursor    iface.ISegment
}

func NewSegmentLinkIt(ss *Snapshot) hif.ISegmentIt {
	if ss == nil {
		return &SegmentLinkIt{}
	}
	it := &SegmentLinkIt{
		Snapshot:  ss,
		OnCloseCB: ss.removeIt,
	}
	it.Cursor = ss.TableData.StongRefRoot()
	return it
}

func (it *SegmentLinkIt) Close() error {
	if it.Cursor != nil {
		it.Cursor.Unref()
		it.Cursor = nil
	}
	if it.OnCloseCB != nil {
		it.OnCloseCB(it)
		it.OnCloseCB = nil
	}
	return nil
}

func (it *SegmentLinkIt) Next() {
	if it.Cursor != nil {
		cursor := it.Cursor
		it.Cursor = it.Cursor.GetNext()
		cursor.Unref()
	}
}

func (it *SegmentLinkIt) Valid() bool {
	if it.Cursor != nil {
		return true
	}
	return false
}

func (it *SegmentLinkIt) GetHandle() hif.ISegment {
	seg := &Segment{
		Data: it.Snapshot.TableData.WeakRefSegment(it.Cursor.GetMeta().ID),
		Attr: it.Snapshot.Attr,
	}
	return seg
}

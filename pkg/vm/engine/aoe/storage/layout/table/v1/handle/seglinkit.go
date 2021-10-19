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

package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
)

var (
	_ dbi.ISegmentIt = (*SegmentLinkIt)(nil)
)

type SegmentLinkIt struct {
	OnCloseCB CloseSegmentItCB
	Snapshot  *Snapshot
	Cursor    iface.ISegment
}

func NewSegmentLinkIt(ss *Snapshot) dbi.ISegmentIt {
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

func (it *SegmentLinkIt) GetHandle() dbi.ISegment {
	seg := &Segment{
		Data: it.Snapshot.TableData.WeakRefSegment(it.Cursor.GetMeta().Id),
		Attr: it.Snapshot.Attr,
	}
	return seg
}

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
	seg := it.Snapshot.GetSegment(it.Snapshot.Ids[it.Pos])
	return seg
}

func (it *SegmentIt) Close() error {
	if it.OnCloseCB != nil {
		it.OnCloseCB(it)
		it.OnCloseCB = nil
	}
	return nil
}

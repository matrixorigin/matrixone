// Copyright 2024 Matrix Origin
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

package vector

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// DetachedBuffer transfers one owned Vector backing allocation through the
// pipeline spool. The allocation keeps its own charge (its MPool metadata),
// so reattaching it never relabels memory. A non-empty value must be attached
// or freed exactly once.
type DetachedBuffer struct {
	data []byte
	kind DetachedBufferKind
}

type DetachedBufferKind uint8

const (
	DetachedDataBuffer DetachedBufferKind = iota
	DetachedAreaBuffer
)

func DetachVectorData(v *Vector) DetachedBuffer {
	if v == nil || !v.CanDetach(BackingData) {
		return DetachedBuffer{}
	}
	buffer := DetachedBuffer{
		data: v.data,
	}
	v.data = nil
	return buffer
}

func DetachVectorArea(v *Vector) DetachedBuffer {
	if v == nil || !v.CanDetach(BackingArea) {
		return DetachedBuffer{}
	}
	buffer := DetachedBuffer{
		data: v.area,
		kind: DetachedAreaBuffer,
	}
	v.area = nil
	return buffer
}

func (b *DetachedBuffer) Capacity() int {
	if b == nil {
		return 0
	}
	return cap(b.data)
}

// CanAttachTo reports whether the buffer can serve v's data or area backing.
// Vectors carry no allocation account and an accounted view has one site for
// data and area, so any owned buffer can serve either backing.
func (b *DetachedBuffer) CanAttachTo(
	v *Vector,
	kind DetachedBufferKind,
) bool {
	return b != nil && v != nil && cap(b.data) != 0 && kind <= DetachedAreaBuffer
}

func (b *DetachedBuffer) AttachTo(
	v *Vector,
	kind DetachedBufferKind,
) error {
	if !b.CanAttachTo(v, kind) {
		return moerr.NewInternalErrorNoCtx(
			"detached vector buffer provenance mismatch",
		)
	}
	if kind == DetachedAreaBuffer {
		if cap(v.area) != 0 {
			return moerr.NewInternalErrorNoCtx(
				"vector area already has backing storage",
			)
		}
		v.area = b.data
	} else {
		if cap(v.data) != 0 {
			return moerr.NewInternalErrorNoCtx(
				"vector data already has backing storage",
			)
		}
		v.data = b.data[:cap(b.data)]
	}
	b.clear()
	return nil
}

func (b *DetachedBuffer) Free(mp *mpool.MPool) {
	if b == nil {
		return
	}
	if cap(b.data) != 0 {
		mp.Free(b.data)
	}
	b.clear()
}

func (b *DetachedBuffer) clear() {
	b.data = nil
	b.kind = DetachedDataBuffer
}

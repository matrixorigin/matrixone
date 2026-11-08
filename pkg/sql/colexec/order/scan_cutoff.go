// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package order

import (
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

// SupportsScanCutoffType admits only domains whose SQL ordering and zone-map
// ordering are known to agree. A non-null leading key is essential: zone maps
// cannot prove that an unread block has no NULLs which sort before the cutoff.
func SupportsScanCutoffType(typ plan.Type) bool {
	if !typ.NotNullable {
		return false
	}
	switch types.T(typ.Id) {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_year,
		types.T_decimal64, types.T_decimal128, types.T_uuid:
		return true
	case types.T_varchar, types.T_text, types.T_binary, types.T_varbinary, types.T_blob:
		// UTF8MB4Bin still has PAD SPACE semantics. Unknown identities must
		// also fail closed rather than being mistaken for bytewise ordering.
		return !typ.PadSpace && (typ.Charset == uint32(types.CharsetLegacy) || typ.Charset == uint32(types.CharsetBinary))
	default:
		return false
	}
}

// ScanCutoff belongs to one scan-scope execution. Its workers contribute only
// batches that Order has already accepted. It publishes at most one cutoff.
type ScanCutoff struct {
	mu          sync.Mutex
	done        atomic.Bool
	limit       uint64
	seen        uint64
	typ         types.Type
	desc        bool
	initialized bool
	bound       [index.ZMSize]byte
}

func NewScanCutoff(limit uint64, typ types.Type, desc bool) *ScanCutoff {
	c := &ScanCutoff{limit: limit, typ: typ, desc: desc}
	if limit == 0 {
		c.done.Store(true)
	}
	return c
}

func (c *ScanCutoff) Done() bool {
	return c == nil || c.done.Load()
}

// Observe returns an owned point zone map exactly once after at least limit
// distinct accepted rows have been witnessed. Invalid representations disable
// this optional optimization instead of changing query behavior.
func (c *ScanCutoff) Observe(v *vector.Vector, rows int) (objectio.ZoneMap, bool) {
	if c.Done() {
		return nil, false
	}
	var batchBound [index.ZMSize]byte
	if !c.batchBound(v, rows, &batchBound) {
		c.mu.Lock()
		c.done.Store(true)
		c.mu.Unlock()
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done.Load() || c.limit == 0 {
		return nil, false
	}
	bound := index.ZM(c.bound[:])
	incoming := index.ZM(batchBound[:])
	if !c.initialized {
		copy(c.bound[:], batchBound[:])
		c.initialized = true
	} else if (c.desc && incoming.CompareMin(bound) < 0) || (!c.desc && incoming.CompareMax(bound) > 0) {
		copy(c.bound[:], batchBound[:])
	}
	remaining := c.limit - c.seen
	if uint64(rows) < remaining {
		c.seen += uint64(rows)
		return nil, false
	}
	c.seen = c.limit
	c.done.Store(true)
	return objectio.ZoneMap(c.bound[:]).Clone(), true
}

// batchBound stores a point at an outward bound of every key in the batch.
// Long byte strings need an upper-rounded maximum for ASC and a lower-rounded
// minimum for DESC; a maximum with no finite upper bound cannot be counted.
func (c *ScanCutoff) batchBound(v *vector.Vector, rows int, out *[index.ZMSize]byte) bool {
	if v == nil || rows <= 0 || v.Length() != rows || v.HasNull() || v.HasGrouping() ||
		v.GetType().Oid != c.typ.Oid || v.GetType().Scale != c.typ.Scale {
		return false
	}
	var min, max []byte
	if v.IsConst() {
		min = v.GetRawBytesAt(0)
		max = min
	} else if v.GetSorted() {
		// Reader metadata describes ascending values, including when the
		// query requests DESC. Admitted types share SQL/storage ordering.
		min = v.GetRawBytesAt(0)
		max = v.GetRawBytesAt(rows - 1)
	} else {
		var ok bool
		ok, min, max = v.GetMinMaxValue()
		if !ok {
			return false
		}
	}
	var raw [index.ZMSize]byte
	zm := index.ZM(raw[:])
	zm.SetType(c.typ.Oid)
	zm.SetScale(c.typ.Scale)
	if c.desc {
		index.UpdateZM(zm, min)
	} else {
		index.UpdateZM(zm, max)
	}
	if !zm.IsInited() || (!c.desc && zm.MaxTruncated()) {
		return false
	}
	point := index.ZM(out[:])
	point.SetType(c.typ.Oid)
	point.SetScale(c.typ.Scale)
	if c.desc {
		index.UpdateZM(point, zm.GetMinBuf())
	} else {
		index.UpdateZM(point, zm.GetMaxBuf())
	}
	return point.IsInited()
}

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

// SupportsScanCutoffType admits domains whose SQL ordering and zone-map
// ordering are known to agree. NULL ordering is carried separately: readers
// only skip a NULL-leading placement on blocks whose physical column null
// count is zero, and publication uses the certified-witness rules in Observe,
// so nullability itself is not an admission constraint there.
func SupportsScanCutoffType(typ plan.Type) bool {
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
// batches that Order has already accepted. It publishes at most one cutoff,
// and only once at least limit witnesses have been certified relative to the
// candidate bound (see Observe).
type ScanCutoff struct {
	mu          sync.Mutex
	done        atomic.Bool
	limit       uint64
	certified   uint64
	typ         types.Type
	desc        bool
	nullsLead   bool
	initialized bool
	bound       [index.ZMSize]byte
}

func NewScanCutoff(limit uint64, typ types.Type, desc bool, nullsLead bool) *ScanCutoff {
	c := &ScanCutoff{limit: limit, typ: typ, desc: desc, nullsLead: nullsLead}
	if limit == 0 {
		c.done.Store(true)
	}
	return c
}

func (c *ScanCutoff) Done() bool {
	return c == nil || c.done.Load()
}

// Observe returns an owned point zone map once the stream has certified at
// least limit witnesses: rows whose rank is provably at or head-ward of the
// published bound. Batches fall into three classes:
//
//   - usable: the outward extremum materializes (the base refusal
//     predicates below). Every NULL row certifies structurally when the
//     effective placement leads the head with NULLs, because a NULL ranks
//     no later than every row of the stream; non-NULL rows certify through
//     their batch's conservative extremum.
//   - all-NULL: no non-NULL extremum exists. The same structural
//     certification covers its NULL rows under a leading placement only.
//   - unusable non-NULL: non-NULL rows exist but no conservative outward
//     extremum can be built for them (representation-truncated maxima,
//     uninitializable zone maps, length mismatch). Their non-NULL rows can
//     never be certified against anything and never count.
//
// Unusable and all-NULL batches contribute no bound and no longer disable the
// summary for later usable batches. A structurally invalid key vector (nil,
// grouping, type or scale mismatch) still disables the cutoff outright, as
// does any stream whose certification never reaches limit (fail open); the
// sort itself consumes every row of every batch regardless of class.
func (c *ScanCutoff) Observe(v *vector.Vector, rows int) (objectio.ZoneMap, bool) {
	if c.Done() {
		return nil, false
	}
	if rows <= 0 {
		// Empty batches witness nothing and disprove nothing.
		return nil, false
	}
	if v == nil || v.HasGrouping() ||
		v.GetType().Oid != c.typ.Oid || v.GetType().Scale != c.typ.Scale {
		c.mu.Lock()
		c.done.Store(true)
		c.mu.Unlock()
		return nil, false
	}
	nulls := observedNullCount(v, rows)
	var batchBound [index.ZMSize]byte
	usable := nulls < uint64(rows) && c.batchBound(v, rows, &batchBound)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done.Load() || c.limit == 0 {
		return nil, false
	}
	// Certified witnesses per class: usable batches count all rows under a
	// leading placement and only non-NULL rows under a trailing one; the
	// other classes always reduce to their NULL rows, which certify only
	// when NULLs may lead the head.
	var witnessed uint64
	switch {
	case usable:
		witnessed = uint64(rows) - nulls
		if c.nullsLead {
			witnessed = uint64(rows)
		}
	case c.nullsLead:
		witnessed = nulls
	}
	c.certified = certifyUpTo(c.certified, witnessed, c.limit)
	if usable {
		bound := index.ZM(c.bound[:])
		incoming := index.ZM(batchBound[:])
		if !c.initialized {
			copy(c.bound[:], batchBound[:])
			c.initialized = true
		} else if (c.desc && incoming.CompareMin(bound) < 0) || (!c.desc && incoming.CompareMax(bound) > 0) {
			copy(c.bound[:], batchBound[:])
		}
	}
	if c.initialized && c.certified >= c.limit {
		c.done.Store(true)
		return objectio.ZoneMap(c.bound[:]).Clone(), true
	}
	return nil, false
}

// certifyUpTo accumulates witnesses without ever passing the limit the
// publication cursor cares about, so adversarial LIMIT values cannot wrap
// the counter.
func certifyUpTo(certified, witnessed, limit uint64) uint64 {
	if witnessed > limit-certified {
		return limit
	}
	return certified + witnessed
}

// observedNullCount returns how many of the batch's logical rows are NULL.
// Const-null vectors carry no bitmap, so they map to every row; const
// non-null vectors map to none.
func observedNullCount(v *vector.Vector, rows int) uint64 {
	if v.IsConstNull() {
		return uint64(rows)
	}
	if v.IsConst() {
		return 0
	}
	length := uint64(v.Length())
	if uint64(rows) < length {
		length = uint64(rows)
	}
	return uint64(v.GetNulls().CountRange(0, length))
}

// batchBound stores a point at an outward bound of every non-NULL key in the
// batch. Long byte strings need an upper-rounded maximum for ASC and a
// lower-rounded minimum for DESC; a maximum with no finite upper bound, or a
// zone map that cannot initialize, cannot be counted. Sorted and const fast
// paths read raw slot bytes that a NULL row does not own, so they stay
// NULL-free only.
func (c *ScanCutoff) batchBound(v *vector.Vector, rows int, out *[index.ZMSize]byte) bool {
	if v.Length() != rows {
		return false
	}
	var min, max []byte
	if v.HasNull() {
		// Reader-sorted and const metadata describes value rows; a leading
		// NULL row would alias raw bytes that do not belong to it. Only the
		// NULL-skipping extremum scan stays sound here.
		var ok bool
		ok, min, max = v.GetMinMaxValue()
		if !ok {
			return false
		}
	} else if v.IsConst() {
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

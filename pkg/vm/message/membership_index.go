// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import (
	"bytes"
	"context"
	"fmt"
	"hash/maphash"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// MembershipIndex is an immutable single-column membership set. Keys are
// borrowed from batches owned by its JoinMap; Free must precede batch cleanup.
// Only the pointer-free slot array grows. Neither build nor probe encodes or
// copies keys, including long strings, into unaccounted iterator buffers.
type MembershipIndex struct {
	slots   []uint64
	batches []*batch.Batch
	col     int
	seed    maphash.Seed
	mp      *mpool.MPool
	hasRows bool
	hasNull bool
}

// SupportsMembershipType admits only domains whose same-type SQL equality is
// exactly byte equality. In particular, floating point and scaled decimals
// must not enter this path merely because their vectors have fixed width.
func SupportsMembershipType(typ types.Type) bool {
	if typ.IsIntOrUint() {
		return true
	}
	switch typ.Oid {
	case types.T_bool, types.T_char, types.T_varchar, types.T_text, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_datalink:
		return true
	default:
		return false
	}
}

func MembershipVectorMatches(vec *vector.Vector, typ types.Type) bool {
	t := vec.GetType()
	return t.Oid == typ.Oid && t.Scale == typ.Scale && t.Width == typ.Width && !vec.GetGrouping().Any()
}

// NewMembershipIndex returns nil without error when optional indexing cannot
// be admitted. The caller keeps all RHS batches for the evaluator fallback.
func NewMembershipIndex(ctx context.Context, batches []*batch.Batch, col int, typ types.Type, mp *mpool.MPool) (*MembershipIndex, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !SupportsMembershipType(typ) || col < 0 {
		return nil, nil
	}
	var rows uint64
	for i, bat := range batches {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if uint64(i) > math.MaxUint32 || uint64(bat.RowCount()) >= math.MaxUint32 ||
			col >= len(bat.Vecs) || !MembershipVectorMatches(bat.Vecs[col], typ) {
			return nil, nil
		}
		rows += uint64(bat.RowCount())
		// This also bounds the additions and subsequent power-of-two rounding.
		if rows > (64<<20)/16 {
			return nil, nil
		}
	}
	index := &MembershipIndex{batches: batches, col: col, seed: maphash.MakeSeed(), mp: mp, hasRows: rows != 0}
	if rows == 0 {
		return index, nil
	}
	slots := uint64(2)
	for slots < rows*2 {
		slots *= 2
	}
	if int64(slots*8) > min(int64(64<<20), mp.Cap()/8) {
		return nil, nil
	}
	var err error
	index.slots, err = mpool.MakeSlice[uint64](int(slots), mp, true)
	if err != nil {
		if canceled := ctx.Err(); canceled != nil {
			return nil, canceled
		}
		if membershipAllocationPressure(err, int64(slots*8), mp.Cap()) {
			return nil, nil
		}
		return nil, err
	}
	clear(index.slots)
	for b, bat := range batches {
		vec := bat.Vecs[col]
		for r := 0; r < bat.RowCount(); r++ {
			if r&127 == 0 {
				if err := ctx.Err(); err != nil {
					index.Free()
					return nil, err
				}
			}
			if membershipNullAt(vec, r) {
				index.hasNull = true
				continue
			}
			pos, err := index.slot(ctx, vec.GetRawBytesAt(r))
			if err != nil {
				index.Free()
				return nil, err
			}
			if index.slots[pos] == 0 {
				index.slots[pos] = uint64(b)<<32 | uint64(r+1)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		index.Free()
		return nil, err
	}
	return index, nil
}

// The release branch reports the native per-pool cap as ErrInternal, unlike
// its global ErrOOM. Recognize that exact allocation error only, at the sole
// optional allocation boundary; unrelated internal/evaluation errors propagate.
func membershipAllocationPressure(err error, size, capacity int64) bool {
	return moerr.IsMoErrCode(err, moerr.ErrOOM) ||
		(moerr.IsMoErrCode(err, moerr.ErrInternal) &&
			err.Error() == fmt.Sprintf("internal error: mpool out of space, alloc %d bytes, cap %d", size, capacity))
}

func membershipNullAt(vec *vector.Vector, row int) bool {
	if vec.IsConstNull() {
		return true
	}
	if vec.IsConst() {
		row = 0
	}
	return vec.GetNulls().Contains(uint64(row))
}

func (index *MembershipIndex) slot(ctx context.Context, key []byte) (uint64, error) {
	mask := uint64(len(index.slots) - 1)
	pos := maphash.Bytes(index.seed, key) & mask
	for steps := uint64(0); ; steps++ {
		if steps&127 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		ref := index.slots[pos]
		if ref == 0 {
			return pos, nil
		}
		vec := index.batches[ref>>32].Vecs[index.col]
		if bytes.Equal(key, vec.GetRawBytesAt(int(uint32(ref)-1))) {
			return pos, nil
		}
		pos = (pos + 1) & mask
	}
}

// Probe requires a vector accepted by MembershipVectorMatches. It has no
// per-row/key scratch allocation; output-vector errors remain the caller's.
func (index *MembershipIndex) Probe(ctx context.Context, vec *vector.Vector, row int) (matched, unknown bool, err error) {
	if err = ctx.Err(); err != nil {
		return false, false, err
	}
	if !index.hasRows {
		return false, false, nil
	}
	if membershipNullAt(vec, row) {
		return false, true, nil
	}
	pos, err := index.slot(ctx, vec.GetRawBytesAt(row))
	if err != nil {
		return false, false, err
	}
	matched = index.slots[pos] != 0
	return matched, !matched && index.hasNull, nil
}

func (index *MembershipIndex) Size() int64 {
	return int64(len(index.slots)) * 8
}

func (index *MembershipIndex) Free() {
	mpool.FreeSlice(index.mp, index.slots)
	index.slots = nil
	index.batches = nil
}

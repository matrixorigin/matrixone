// Copyright 2026 Matrix Origin
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

package aggexec

import (
	"bytes"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const maxByVarlenaCompactionSlack = 1 << 20

// maxByExec keeps value, order and tie in three serialized state vectors. The
// comparison is a total order: order, then tie, then canonical value bytes.
// The final component makes merge commutative even if callers provide duplicate
// tie keys. NULL order rows are ignored and NULL tie sorts below non-NULL tie.
type maxByExec struct {
	aggExec
	nonNullValue bool
}

func makeMaxByExec(mp *mpool.MPool, id int64, nonNullValue bool, params []types.Type) AggFuncExec {
	if len(params) != 3 {
		panic(moerr.NewInternalErrorNoCtx("max_by requires value, order, and tie arguments"))
	}
	exec := &maxByExec{nonNullValue: nonNullValue}
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      id,
		isDistinct: false,
		argTypes:   append([]types.Type(nil), params...),
		retType:    params[0],
		stateTypes: append([]types.Type(nil), params...),
		emptyNull:  true,
	}
	return exec
}

func (exec *maxByExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *maxByExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if len(vectors) != 3 {
		return moerr.NewInternalErrorNoCtx("max_by requires three input vectors")
	}
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *maxByExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if len(vectors) != 3 {
		return moerr.NewInternalErrorNoCtx("max_by requires three input vectors")
	}
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		rows := [3]int{offset + i, offset + i, offset + i}
		for column := range rows {
			if vectors[column].IsConst() {
				rows[column] = 0
			}
		}
		if vectors[1].IsNull(uint64(rows[1])) || exec.nonNullValue && vectors[0].IsNull(uint64(rows[0])) {
			continue
		}
		x, y := exec.getXY(group - 1)
		state := &exec.state[x]
		if state.vecs[1].IsNull(uint64(y)) || candidateWins(vectors, rows, state.vecs, int(y), exec.argTypes) {
			if err := exec.copyWinner(state.vecs, int(y), vectors, rows); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *maxByExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *maxByExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other, ok := next.(*maxByExec)
	if !ok || other.nonNullValue != exec.nonNullValue || !slices.Equal(other.argTypes, exec.argTypes) {
		return moerr.NewInternalErrorNoCtx("cannot merge incompatible max_by states")
	}
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x1, y1 := exec.getXY(group - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		candidate := other.state[x2].vecs
		if candidate[1].IsNull(uint64(y2)) {
			continue
		}
		current := exec.state[x1].vecs
		rows := [3]int{int(y2), int(y2), int(y2)}
		if current[1].IsNull(uint64(y1)) || candidateWins(candidate, rows, current, int(y1), exec.argTypes) {
			if err := exec.copyWinner(current, int(y1), candidate, rows); err != nil {
				return err
			}
		}
	}
	return nil
}

func candidateWins(candidate []*vector.Vector, candidateRows [3]int, current []*vector.Vector, currentRow int, typs []types.Type) bool {
	if cmp := compareVectorValue(candidate[1], candidateRows[1], current[1], currentRow, typs[1]); cmp != 0 {
		return cmp > 0
	}
	if cmp := compareNullableVectorValue(candidate[2], candidateRows[2], current[2], currentRow, typs[2]); cmp != 0 {
		return cmp > 0
	}
	// A caller-supplied tie should normally be unique. The value bytes are a
	// final deterministic fallback so partial aggregation merge order cannot
	// change the result when it is not.
	return compareNullableRaw(candidate[0], candidateRows[0], current[0], currentRow) > 0
}

func compareNullableVectorValue(a *vector.Vector, ai int, b *vector.Vector, bi int, typ types.Type) int {
	an, bn := a.IsNull(uint64(ai)), b.IsNull(uint64(bi))
	if an || bn {
		switch {
		case an && bn:
			return 0
		case an:
			return -1
		default:
			return 1
		}
	}
	return compareVectorValue(a, ai, b, bi, typ)
}

func compareNullableRaw(a *vector.Vector, ai int, b *vector.Vector, bi int) int {
	an, bn := a.IsNull(uint64(ai)), b.IsNull(uint64(bi))
	if an || bn {
		switch {
		case an && bn:
			return 0
		case an:
			return -1
		default:
			return 1
		}
	}
	return bytes.Compare(a.GetRawBytesAt(ai), b.GetRawBytesAt(bi))
}

func compareVectorValue(a *vector.Vector, ai int, b *vector.Vector, bi int, typ types.Type) int {
	x, y := a.GetRawBytesAt(ai), b.GetRawBytesAt(bi)
	switch typ.Oid {
	case types.T_bool:
		return types.BoolAscCompare(types.DecodeBool(x), types.DecodeBool(y))
	case types.T_int8:
		return types.GenericAscCompare(types.DecodeInt8(x), types.DecodeInt8(y))
	case types.T_int16:
		return types.GenericAscCompare(types.DecodeInt16(x), types.DecodeInt16(y))
	case types.T_int32:
		return types.GenericAscCompare(types.DecodeInt32(x), types.DecodeInt32(y))
	case types.T_int64:
		return types.GenericAscCompare(types.DecodeInt64(x), types.DecodeInt64(y))
	case types.T_uint8:
		return types.GenericAscCompare(types.DecodeUint8(x), types.DecodeUint8(y))
	case types.T_uint16:
		return types.GenericAscCompare(types.DecodeUint16(x), types.DecodeUint16(y))
	case types.T_uint32:
		return types.GenericAscCompare(types.DecodeUint32(x), types.DecodeUint32(y))
	case types.T_uint64, types.T_bit:
		return types.GenericAscCompare(types.DecodeUint64(x), types.DecodeUint64(y))
	case types.T_float32:
		return compareFloat64(float64(types.DecodeFloat32(x)), float64(types.DecodeFloat32(y)))
	case types.T_float64:
		return compareFloat64(types.DecodeFloat64(x), types.DecodeFloat64(y))
	case types.T_date:
		return types.GenericAscCompare(types.DecodeDate(x), types.DecodeDate(y))
	case types.T_datetime:
		return types.GenericAscCompare(types.DecodeDatetime(x), types.DecodeDatetime(y))
	case types.T_timestamp:
		return types.GenericAscCompare(types.DecodeTimestamp(x), types.DecodeTimestamp(y))
	case types.T_time:
		return types.GenericAscCompare(types.DecodeFixed[types.Time](x), types.DecodeFixed[types.Time](y))
	case types.T_year:
		return types.GenericAscCompare(types.DecodeFixed[types.MoYear](x), types.DecodeFixed[types.MoYear](y))
	case types.T_decimal64:
		return types.DecodeDecimal64(x).Compare(types.DecodeDecimal64(y))
	case types.T_decimal128:
		return types.DecodeDecimal128(x).Compare(types.DecodeDecimal128(y))
	case types.T_decimal256:
		return types.DecodeDecimal256(x).Compare(types.DecodeDecimal256(y))
	case types.T_uuid:
		return types.DecodeUuid(x).Compare(types.DecodeUuid(y))
	default:
		return bytes.Compare(x, y)
	}
}

func compareFloat64(a, b float64) int {
	aNaN, bNaN := math.IsNaN(a), math.IsNaN(b)
	if aNaN || bNaN {
		switch {
		case aNaN && bNaN:
			return types.GenericAscCompare(math.Float64bits(a), math.Float64bits(b))
		case aNaN:
			return 1
		default:
			return -1
		}
	}
	return types.GenericAscCompare(a, b)
}

func (exec *maxByExec) copyWinner(dst []*vector.Vector, dstRow int, src []*vector.Vector, srcRows [3]int) error {
	// Reserve every fallible varlen allocation before mutating any of the three
	// correlated state vectors. Without this preflight, an OOM after copying the
	// value but before copying order/tie would publish a mixed winner. Growing
	// capacity is harmless if a later reservation fails; the logical state stays
	// byte-for-byte unchanged and remains safe to serialize or free.
	for i := range dst {
		if src[i].IsNull(uint64(srcRows[i])) || !dst[i].GetType().IsVarlen() {
			continue
		}
		valueBytes := len(src[i].GetRawBytesAt(srcRows[i]))
		if valueBytes <= types.VarlenaInlineSize {
			continue
		}
		if err := dst[i].PreExtendWithArea(0, valueBytes, exec.mp); err != nil {
			return err
		}
	}
	for i := range dst {
		if src[i].IsNull(uint64(srcRows[i])) {
			dst[i].SetNull(uint64(dstRow))
			continue
		}
		if err := dst[i].SetRawBytesAt(dstRow, src[i].GetRawBytesAt(srcRows[i]), exec.mp); err != nil {
			return err
		}
		dst[i].UnsetNull(uint64(dstRow))
	}
	for _, vec := range dst {
		// Compaction is an optional bound on stale varlen area. A failed compact
		// clone leaves the valid original untouched, so memory pressure must not
		// turn a fully copied winner into an aggregate error with ambiguous state.
		_ = compactMaxByStateVector(vec, exec.mp)
	}
	return nil
}

func compactMaxByStateVector(vec *vector.Vector, mp *mpool.MPool) error {
	if vec == nil || !vec.GetType().IsVarlen() {
		return nil
	}
	areaCapacity := cap(vec.GetArea())
	if areaCapacity <= maxByVarlenaCompactionSlack {
		return nil
	}
	liveBytes := 0
	for row := 0; row < vec.Length(); row++ {
		if vec.IsNull(uint64(row)) {
			continue
		}
		valueBytes := len(vec.GetRawBytesAt(row))
		if valueBytes > types.VarlenaInlineSize {
			liveBytes += valueBytes
		}
	}
	if areaCapacity <= 2*liveBytes+maxByVarlenaCompactionSlack {
		return nil
	}
	var (
		compact *vector.Vector
		err     error
	)
	if selection := vec.AllocationAccountSelection(); selection != nil {
		compact, err = vec.CloneToFlatCompactWithAllocation(mp, selection)
	} else {
		compact, err = vec.CloneToFlatCompact(mp)
	}
	if err != nil {
		return err
	}
	vec.Free(mp)
	*vec = *compact
	return nil
}

func (exec *maxByExec) SetExtraInformation(any, int) error { return nil }

func (exec *maxByExec) Flush() ([]*vector.Vector, error) {
	result := make([]*vector.Vector, len(exec.state))
	for i := range exec.state {
		result[i] = exec.state[i].vecs[0]
		exec.state[i].vecs[0] = nil
		for j := 1; j < len(exec.state[i].vecs); j++ {
			exec.state[i].vecs[j].Free(exec.mp)
			exec.state[i].vecs[j] = nil
		}
		exec.state[i].length = 0
		exec.state[i].capacity = 0
	}
	return result, nil
}

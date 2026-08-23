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
	"io"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
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
	varlenaUsage [][]maxByVarlenaUsage
}

type maxByVarlenaUsage struct {
	liveBytes  int
	staleBytes int
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
	if len(vectors) != 3 {
		return moerr.NewInternalErrorNoCtx("max_by requires three input vectors")
	}
	if err := exec.fillRow(groupIndex, row, vectors); err != nil {
		return err
	}
	exec.compactChunk(exec.getChunk(groupIndex))
	return nil
}

func (exec *maxByExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if len(vectors) != 3 {
		return moerr.NewInternalErrorNoCtx("max_by requires three input vectors")
	}
	for row := 0; row < vectors[0].Length(); row++ {
		if err := exec.fillRow(groupIndex, row, vectors); err != nil {
			return err
		}
	}
	exec.compactChunk(exec.getChunk(groupIndex))
	return nil
}

func (exec *maxByExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if len(vectors) != 3 {
		return moerr.NewInternalErrorNoCtx("max_by requires three input vectors")
	}
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		if err := exec.fillRow(int(group-1), offset+i, vectors); err != nil {
			return err
		}
	}
	exec.compactBatchGroups(groups)
	return nil
}

func (exec *maxByExec) fillRow(
	groupIndex int,
	row int,
	vectors []*vector.Vector,
) error {
	rows := [3]int{row, row, row}
	for column := range rows {
		if vectors[column].IsConst() {
			rows[column] = 0
		}
	}
	if vectors[1].IsNull(uint64(rows[1])) ||
		exec.nonNullValue && vectors[0].IsNull(uint64(rows[0])) {
		return nil
	}
	x, y := exec.getXY(uint64(groupIndex))
	state := &exec.state[x]
	if state.vecs[1].IsNull(uint64(y)) ||
		candidateWins(vectors, rows, state.vecs, int(y), exec.argTypes) {
		return exec.copyWinner(x, state.vecs, int(y), vectors, rows)
	}
	if candidateEquals(vectors, rows, state.vecs, int(y), exec.argTypes) {
		return mergeEqualRuntimeStringDomain(state.vecs[0], int(y), vectors[0], rows[0], exec.mp)
	}
	return nil
}

func (exec *maxByExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	if err := exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)}); err != nil {
		return err
	}
	return nil
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
			if err := exec.copyWinner(x1, current, int(y1), candidate, rows); err != nil {
				return err
			}
		} else if candidateEquals(candidate, rows, current, int(y1), exec.argTypes) {
			if err := mergeEqualRuntimeStringDomain(current[0], int(y1), candidate[0], rows[0], exec.mp); err != nil {
				return err
			}
		}
	}
	exec.compactBatchGroups(groups)
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

func candidateEquals(candidate []*vector.Vector, candidateRows [3]int, current []*vector.Vector, currentRow int, typs []types.Type) bool {
	return compareVectorValue(candidate[1], candidateRows[1], current[1], currentRow, typs[1]) == 0 &&
		compareNullableVectorValue(candidate[2], candidateRows[2], current[2], currentRow, typs[2]) == 0 &&
		compareNullableRaw(candidate[0], candidateRows[0], current[0], currentRow) == 0
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

func (exec *maxByExec) copyWinner(
	chunk int,
	dst []*vector.Vector,
	dstRow int,
	src []*vector.Vector,
	srcRows [3]int,
) error {
	usage := exec.ensureVarlenaUsage(chunk)
	var oldLive, newLive [3]int
	// Reserve every fallible varlen allocation before mutating any of the three
	// correlated state vectors. Without this preflight, an OOM after copying the
	// value but before copying order/tie would publish a mixed winner. Growing
	// capacity is harmless if a later reservation fails; the logical state stays
	// byte-for-byte unchanged and remains safe to serialize or free.
	for i := range dst {
		if !dst[i].GetType().IsVarlen() {
			continue
		}
		if !dst[i].IsNull(uint64(dstRow)) {
			oldLive[i] = maxByAreaBytes(dst[i].GetRawBytesAt(dstRow))
		}
		if src[i].IsNull(uint64(srcRows[i])) {
			continue
		}
		valueBytes := len(src[i].GetRawBytesAt(srcRows[i]))
		newLive[i] = maxByAreaBytes(src[i].GetRawBytesAt(srcRows[i]))
		if newLive[i] == 0 {
			continue
		}
		if err := dst[i].PreExtendWithArea(0, valueBytes, exec.mp); err != nil {
			return err
		}
	}
	valueSource := src[0].GetStringSourceAt(srcRows[0])
	if err := dst[0].PreflightSetStringSourceAt(dstRow, valueSource, exec.mp); err != nil {
		return err
	}
	if !src[0].IsNull(uint64(srcRows[0])) {
		if err := dst[0].PreflightSetPrepareParamKindAt(
			dstRow,
			src[0].GetPrepareParamKindAt(srcRows[0]),
			exec.mp,
		); err != nil {
			return err
		}
	}
	for i := range dst {
		if src[i].IsNull(uint64(srcRows[i])) {
			if i == 0 {
				dst[i].SetNullPreservingPrepareParamCapacity(uint64(dstRow))
				if err := dst[i].SetStringSourceAtWithMP(dstRow, valueSource, exec.mp); err != nil {
					return err
				}
			} else {
				dst[i].SetNull(uint64(dstRow))
			}
			continue
		}
		if i == 0 {
			if err := dst[i].SetRawBytesAtFromAndUnsetNull(dstRow, src[i], srcRows[i], exec.mp); err != nil {
				return err
			}
		} else {
			if err := dst[i].SetRawBytesAtFrom(dstRow, src[i], srcRows[i], exec.mp); err != nil {
				return err
			}
			dst[i].UnsetNull(uint64(dstRow))
		}
	}
	for i, vec := range dst {
		if vec.GetType().IsVarlen() {
			usage[i].liveBytes += newLive[i] - oldLive[i]
			usage[i].staleBytes += oldLive[i]
		}
	}
	return nil
}

func (exec *maxByExec) getChunk(groupIndex int) int {
	x, _ := exec.getXY(uint64(groupIndex))
	return x
}

func (exec *maxByExec) compactChunk(chunk int) {
	if chunk < 0 || chunk >= len(exec.state) {
		return
	}
	if len(exec.state[chunk].vecs) != 0 {
		exec.state[chunk].vecs[0].NormalizePrepareParamKinds()
	}
	usage := exec.ensureVarlenaUsage(chunk)
	for i, vec := range exec.state[chunk].vecs {
		// Compaction is an optional bound on stale varlen area. A failed compact
		// clone leaves the valid original untouched, so memory pressure must not
		// turn a fully copied winner into an aggregate error with ambiguous state.
		_ = compactMaxByStateVector(vec, &usage[i], exec.mp)
	}
}

func (exec *maxByExec) compactBatchGroups(groups []uint64) {
	var chunks [hashmap.UnitLimit]int
	chunkCount := 0
	for _, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		chunk := exec.getChunk(int(group - 1))
		seen := false
		for i := 0; i < chunkCount; i++ {
			if chunks[i] == chunk {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		if chunkCount == len(chunks) {
			// Normal Group work units are bounded by UnitLimit. Keep the public
			// aggregate API correct for a larger caller without allocating a map.
			exec.compactChunk(chunk)
			continue
		}
		chunks[chunkCount] = chunk
		chunkCount++
	}
	for i := 0; i < chunkCount; i++ {
		exec.compactChunk(chunks[i])
	}
}

func maxByAreaBytes(value []byte) int {
	if len(value) <= types.VarlenaInlineSize {
		return 0
	}
	return len(value)
}

func (exec *maxByExec) ensureVarlenaUsage(chunk int) []maxByVarlenaUsage {
	for len(exec.varlenaUsage) < len(exec.state) {
		exec.varlenaUsage = append(exec.varlenaUsage, nil)
	}
	if exec.varlenaUsage[chunk] != nil {
		return exec.varlenaUsage[chunk]
	}
	usage := make([]maxByVarlenaUsage, len(exec.state[chunk].vecs))
	for i, vec := range exec.state[chunk].vecs {
		if vec == nil || !vec.GetType().IsVarlen() {
			continue
		}
		for row := 0; row < vec.Length(); row++ {
			if !vec.IsNull(uint64(row)) {
				usage[i].liveBytes += maxByAreaBytes(vec.GetRawBytesAt(row))
			}
		}
		usage[i].staleBytes = max(0, len(vec.GetArea())-usage[i].liveBytes)
	}
	exec.varlenaUsage[chunk] = usage
	return usage
}

func compactMaxByStateVector(vec *vector.Vector, usage *maxByVarlenaUsage, mp *mpool.MPool) error {
	if vec == nil || !vec.GetType().IsVarlen() {
		return nil
	}
	fixedCapacity := vec.Capacity() * vec.GetType().TypeSize()
	areaCapacity := vec.Allocated() - fixedCapacity
	if areaCapacity <= maxByVarlenaCompactionSlack ||
		usage.staleBytes <= usage.liveBytes+maxByVarlenaCompactionSlack {
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
	usage.staleBytes = 0
	return nil
}

func (exec *maxByExec) GroupGrow(more int) error {
	oldChunks := len(exec.state)
	if err := exec.aggExec.GroupGrow(more); err != nil {
		return err
	}
	if exec.chunkSize == 1 {
		// The single-group fast path replaces state[0] instead of appending a
		// chunk, so any accounting derived from the prior vector is invalid.
		exec.varlenaUsage = nil
		oldChunks = 0
	}
	for len(exec.varlenaUsage) < len(exec.state) {
		exec.varlenaUsage = append(exec.varlenaUsage, nil)
	}
	for chunk := oldChunks; chunk < len(exec.state); chunk++ {
		exec.varlenaUsage[chunk] = make([]maxByVarlenaUsage, len(exec.state[chunk].vecs))
	}
	return nil
}

func (exec *maxByExec) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
	err := exec.aggExec.UnmarshalFromReader(reader, mp)
	exec.varlenaUsage = nil
	return err
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
	exec.varlenaUsage = nil
	return result, nil
}

func (exec *maxByExec) Free() {
	exec.varlenaUsage = nil
	exec.aggExec.Free()
}

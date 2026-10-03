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

package compile

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Leave room for pool rounding and descriptors in the 64 MiB native window.
// This is a slice target, not a Go coalescing buffer. Sirius coalesces the
// published slices under its own byte/count admission.
const siriusBatchTarget = 32 << 20

func publishSiriusBatch(ctx context.Context, input SiriusInput, bat *batch.Batch, columns []SiriusReadColumn) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if len(bat.Vecs) != len(columns) || len(columns) == 0 {
		return moerr.NewInvalidInput(ctx, "Sirius reader batch does not match its binding")
	}
	for i, v := range bat.Vecs {
		if v == nil || v.Length() != bat.RowCount() || int32(v.GetType().Oid) != columns[i].Type.Id {
			return moerr.NewInvalidInput(ctx, "Sirius reader column does not match its binding")
		}
		width, err := siriusElementSize(v.GetType().Oid)
		if err != nil {
			return err
		}
		physicalRows := bat.RowCount()
		if v.IsConst() {
			physicalRows = 1
		}
		if !v.IsConstNull() && uint64(len(v.GetData())) < uint64(physicalRows)*uint64(width) {
			return moerr.NewInvalidInput(ctx, "truncated Sirius reader column")
		}
		if v.GetType().Oid.IsDecimal() && (v.GetType().Width != columns[i].Type.Width || v.GetType().Scale != columns[i].Type.Scale) {
			return moerr.NewInvalidInput(ctx, "Sirius reader decimal type does not match its binding")
		}
		if columns[i].Type.NotNullable && v.HasNull() {
			return moerr.NewInvalidInput(ctx, "NULL in nonnullable Sirius reader column")
		}
	}
	if _, fits := siriusWholeBatchSize(bat); fits {
		return publishSiriusSlice(ctx, input, bat, columns, 0, bat.RowCount())
	}
	for begin := 0; begin < bat.RowCount(); {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		// Inspect borrowed reader bytes only. No outgoing allocation is allowed
		// until the lease below has reserved native credit.
		end, err := siriusSliceEnd(bat, columns, begin, siriusBatchTarget)
		if err != nil {
			return err
		}
		if err := publishSiriusSlice(ctx, input, bat, columns, begin, end); err != nil {
			return err
		}
		begin = end
	}
	return nil
}

func siriusElementSize(oid types.T) (int, error) {
	switch oid {
	case types.T_bool, types.T_int8, types.T_uint8:
		return 1, nil
	case types.T_int16, types.T_uint16:
		return 2, nil
	case types.T_int32, types.T_uint32, types.T_float32, types.T_date:
		return 4, nil
	case types.T_int64, types.T_uint64, types.T_float64, types.T_decimal64, types.T_timestamp:
		return 8, nil
	case types.T_decimal128:
		return 16, nil
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary:
		return types.VarlenaSize, nil
	default:
		return 0, moerr.NewNotSupportedNoCtx("unsupported Sirius native column type")
	}
}

// The expanded size bounds constants too: a small physical constant cannot
// hide a large logical expansion. A source batch is never scanned beyond the
// next slice, so this work is linear in the rows actually published.
func siriusSliceEnd(bat *batch.Batch, columns []SiriusReadColumn, begin, target int) (int, error) {
	bytes, fixedRowBytes, strings := len(columns)*8, 0, false
	for _, v := range bat.Vecs {
		width, err := siriusElementSize(v.GetType().Oid)
		if err != nil {
			return begin, err
		}
		fixedRowBytes += width + 1
		strings = strings || v.GetType().IsVarlen()
	}
	if !strings && fixedRowBytes <= target-bytes {
		return min(bat.RowCount(), begin+(target-bytes)/fixedRowBytes), nil
	}
	end := begin
	for end < bat.RowCount() {
		rowBytes := fixedRowBytes
		for _, v := range bat.Vecs {
			if !v.GetType().IsVarlen() {
				continue
			}
			row := end
			if v.IsConst() {
				row = 0
			}
			isNull := v.IsConstNull() || v.IsNull(uint64(row))
			if !isNull {
				rowBytes += len(v.GetBytesAt(row))
			}
		}
		if rowBytes > target-bytes {
			if end == begin {
				// A single row may exceed the coalescing target. Leave a MiB
				// plus descriptors for physical-pool rounding; native Acquire
				// remains authoritative for the configured pool's charge.
				if rowBytes+bytes > (63<<20)-len(columns)*68 {
					return begin, moerr.NewInvalidInputNoCtx("Sirius reader row exceeds bounded slice capacity")
				}
				return end + 1, nil
			}
			break
		}
		bytes += rowBytes
		end++
	}
	return end, nil
}

// Ordinary reader batches need neither per-row sizing nor varlena rebasing.
// The disjoint-area proof bounds logical expansion by the physical area.
func siriusWholeBatchSize(bat *batch.Batch) (uint64, bool) {
	var physical, expanded uint64
	for _, v := range bat.Vecs {
		width, _ := siriusElementSize(v.GetType().Oid)
		rows := bat.RowCount()
		expanded += uint64(rows*(width+1) + 8)
		if v.IsConstNull() {
			continue
		}
		if v.IsConst() {
			rows = 1
		}
		physical += uint64(rows * width)
		if v.HasNull() {
			physical += uint64((rows + 63) / 64 * 8)
		}
		if v.GetType().IsVarlen() {
			if v.IsConst() || !v.VarlenaAreaIsDisjoint() {
				return 0, false
			}
			physical += uint64(len(v.GetArea()))
			expanded += uint64(len(v.GetArea()))
		}
		if physical > siriusBatchTarget || expanded > siriusBatchTarget {
			return 0, false
		}
	}
	return physical, expanded <= siriusBatchTarget
}

func siriusSliceBytes(bat *batch.Batch, begin, end int) uint64 {
	if begin == 0 && end == bat.RowCount() {
		if bytes, fits := siriusWholeBatchSize(bat); fits {
			return bytes
		}
	}
	var total uint64
	for _, v := range bat.Vecs {
		if v.IsConstNull() {
			continue
		}
		lo, hi := begin, end
		if v.IsConst() {
			lo, hi = 0, 1
		}
		width, _ := siriusElementSize(v.GetType().Oid)
		total += uint64((hi - lo) * width)
		if v.HasNull() {
			total += uint64((hi - lo + 63) / 64 * 8)
		}
		if v.GetType().IsVarlen() {
			for row := lo; row < hi; row++ {
				if !v.IsNull(uint64(row)) {
					value := v.GetBytesAt(row)
					if len(value) > types.VarlenaInlineSize {
						total += uint64(len(value))
					}
				}
			}
		}
	}
	return total
}

func publishSiriusSlice(ctx context.Context, input SiriusInput, bat *batch.Batch, columns []SiriusReadColumn, begin, end int) (err error) {
	lease, err := input.Acquire(ctx, siriusSliceBytes(bat, begin, end))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lease.Release()) }()
	vs := make([]SiriusInputVector, len(columns))
	_, whole := siriusWholeBatchSize(bat)
	for i, v := range bat.Vecs {
		out := &vs[i]
		if v.IsConstNull() {
			out.Class = 2
			continue
		}
		lo, hi := begin, end
		if v.IsConst() {
			out.Class = 1
			lo, hi = 0, 1
		}
		if v.HasNull() {
			out.Nulls = make([]byte, (hi-lo+63)/64*8)
			for row := lo; row < hi; row++ {
				if v.IsNull(uint64(row)) {
					out.Nulls[(row-lo)/8] |= 1 << uint((row-lo)%8)
				}
			}
		}
		width, _ := siriusElementSize(v.GetType().Oid)
		if !v.GetType().IsVarlen() || (whole && begin == 0 && end == bat.RowCount()) {
			out.Data = v.GetData()[lo*width : hi*width]
			if v.GetType().IsVarlen() {
				out.Area = v.GetArea()
			}
			continue
		}
		out.Data = make([]byte, (hi-lo)*types.VarlenaSize)
		areaBytes := 0
		for row := lo; row < hi; row++ {
			if !v.IsNull(uint64(row)) && len(v.GetBytesAt(row)) > types.VarlenaInlineSize {
				areaBytes += len(v.GetBytesAt(row))
			}
		}
		out.Area = make([]byte, areaBytes)
		position := 0
		for row := lo; row < hi; row++ {
			if v.IsNull(uint64(row)) {
				continue
			}
			value := v.GetBytesAt(row)
			descriptor := out.Data[(row-lo)*types.VarlenaSize : (row-lo+1)*types.VarlenaSize]
			if len(value) <= types.VarlenaInlineSize {
				descriptor[0] = byte(len(value))
				copy(descriptor[1:], value)
			} else {
				binary.LittleEndian.PutUint32(descriptor, types.VarlenaBigHdr)
				binary.LittleEndian.PutUint32(descriptor[4:], uint32(position))
				binary.LittleEndian.PutUint32(descriptor[8:], uint32(len(value)))
				copy(out.Area[position:], value)
				position += len(value)
			}
		}
	}
	return lease.Publish(ctx, uint32(end-begin), vs)
}

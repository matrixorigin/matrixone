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

package ioutil

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func addRunBytes(a, b int) int {
	if b > math.MaxInt-a {
		return math.MaxInt
	}
	return a + b
}

func multiplyRunBytes(a, b int) int {
	if a > 0 && b > math.MaxInt/a {
		return math.MaxInt
	}
	return a * b
}

// materializedBatchSize bounds both the staged area and the size after merge
// expands shared values. Use a per-vector maximum: different columns may have
// dead area and shared descriptors, respectively. The bound travels with the
// staged batch, including when sorting clears its disjointness flag.
func materializedBatchSize(bat *batch.Batch) int {
	size, _ := materializedBatchSizeWithPayloadSizes(bat, nil)
	return size
}

// payloadSizes, when provided, receives exact live payload sizes for
// non-constant columns that required a scan. The values are valid only while
// this batch remains unchanged and can be reused by the same full-window copy.
func materializedBatchSizeWithPayloadSizes(bat *batch.Batch, payloadSizes []int) (int, bool) {
	size := 0
	hasPayloadSize := false
	for i, vec := range bat.Vecs {
		if payloadSizes != nil {
			payloadSizes[i] = -1
		}
		rows := vec.Length()
		if vec.IsConst() {
			// A constant may have one physical/logical slot while the batch
			// broadcasts it across every row.
			rows = bat.RowCount()
		}
		size = addRunBytes(size, multiplyRunBytes(rows, vec.GetType().TypeSize()))
		if !vec.GetType().IsVarlen() || len(vec.GetArea()) == 0 {
			continue
		}
		if vec.VarlenaAreaIsDisjoint() {
			size = addRunBytes(size, len(vec.GetArea()))
			continue
		}
		payload := 0
		if vec.IsConstNull() {
			size = addRunBytes(size, len(vec.GetArea()))
			continue
		}
		values, _ := vector.MustVarlenaRawData(vec)
		if vec.IsConst() {
			if !values[0].IsSmall() {
				_, n := values[0].OffsetLen()
				if uint64(n) > uint64(math.MaxInt) {
					return math.MaxInt, false
				}
				payload = multiplyRunBytes(rows, int(n))
			}
		} else {
			hasNull := !vec.GetNulls().EmptyByFlag()
			// A varlena length is uint32. For ordinary vector sizes the
			// entire sum is provably representable, so no per-row saturation
			// check is needed. Retain the checked path for extreme inputs.
			if uint64(rows) <= uint64(math.MaxInt)/uint64(math.MaxUint32) {
				for i := 0; i < rows; i++ {
					if hasNull && vec.GetNulls().Contains(uint64(i)) {
						continue
					}
					if !values[i].IsSmall() {
						_, n := values[i].OffsetLen()
						payload += int(n)
					}
				}
			} else {
				for i := 0; i < rows; i++ {
					if hasNull && vec.GetNulls().Contains(uint64(i)) {
						continue
					}
					if !values[i].IsSmall() {
						_, n := values[i].OffsetLen()
						if uint64(n) > uint64(math.MaxInt) {
							return math.MaxInt, false
						}
						payload = addRunBytes(payload, int(n))
					}
				}
			}
		}
		if payloadSizes != nil && !vec.IsConst() && rows == bat.RowCount() && payload != math.MaxInt {
			payloadSizes[i] = payload
			hasPayloadSize = true
		}
		size = addRunBytes(size, max(len(vec.GetArea()), payload))
	}
	return size, hasPayloadSize
}

// UnionWindow stores a constant varlen payload once and broadcasts its
// descriptor. A later sorted merge may materialize it once per row.
func constantRunExpansion(data *batch.Batch, rows int) int {
	if rows <= 1 {
		return 0
	}
	size := 0
	for _, vec := range data.Vecs {
		if !vec.IsConst() || vec.IsConstNull() || !vec.GetType().IsVarlen() || len(vec.GetArea()) == 0 {
			continue
		}
		n := len(vec.GetBytesAt(0))
		if n > types.VarlenaInlineSize {
			size = addRunBytes(size, multiplyRunBytes(rows-1, n))
		}
	}
	return size
}

// boundedRunRows selects the prefix whose materialized size fits the remaining
// budget. The caller handles a zero-length prefix, including oversized rows.
func boundedRunRows(data *batch.Batch, offset, rows, remaining int) (int, int) {
	size := 0
	for row := offset; row < offset+rows; row++ {
		previous := size
		for _, vec := range data.Vecs {
			size = addRunBytes(size, vec.GetType().TypeSize())
			if !vec.GetType().IsVarlen() || len(vec.GetArea()) == 0 || vec.IsConstNull() {
				continue
			}
			if !vec.IsConst() && vec.GetNulls().Contains(uint64(row)) {
				continue
			}
			n := len(vec.GetBytesAt(row))
			if n > types.VarlenaInlineSize {
				size = addRunBytes(size, n)
			}
		}
		if size > remaining {
			return row - offset, previous
		}
		if size == remaining {
			return row - offset + 1, size
		}
	}
	return rows, size
}

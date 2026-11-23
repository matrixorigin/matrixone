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
	if a < 0 || b < 0 || b > math.MaxInt-a {
		return math.MaxInt
	}
	return a + b
}

func multiplyRunBytes(a, b int) int {
	if a < 0 || b < 0 || (a > 0 && b > math.MaxInt/a) {
		return math.MaxInt
	}
	return a * b
}

// fastBatchAdmission proves a whole input fits without scanning aliased
// descriptors. Its deliberately loose bound is never carried after the copy.
func fastBatchAdmission(bat *batch.Batch, budget int) (int, bool, bool) {
	size, hasAlias := 0, false
	for _, vec := range bat.Vecs {
		rows := vec.Length()
		if vec.IsConst() {
			rows = bat.RowCount()
		}
		size = addRunBytes(size, multiplyRunBytes(rows, vec.GetType().TypeSize()))
		area := len(vec.GetArea())
		if area == 0 || !vec.GetType().IsVarlen() {
			continue
		}
		payload := area
		if vec.IsConst() && !vec.IsConstNull() {
			n := len(vec.GetBytesAt(0))
			if n > types.VarlenaInlineSize {
				payload = max(area, multiplyRunBytes(rows, n))
			}
		} else if !vec.IsConst() && !vec.VarlenaAreaIsDisjoint() {
			hasAlias = true
			payload = max(area, multiplyRunBytes(rows, area))
		}
		size = addRunBytes(size, payload)
		if size == math.MaxInt || size > budget {
			return 0, false, false
		}
	}
	return size, size != math.MaxInt && size <= budget, hasAlias
}

// copiedRunBytes charges only the newly copied slots and area, and counts the
// later expansion of a constant's broadcast payload. Earlier bounds stay with
// the current batch and must not be recomputed from its physical size.
func copiedRunBytes(dst, src *batch.Batch, oldAreas []int, rows int) (int, bool) {
	size := 0
	for i, vec := range dst.Vecs {
		area := len(vec.GetArea())
		if area < oldAreas[i] {
			return 0, false
		}
		size = addRunBytes(size, multiplyRunBytes(rows, vec.GetType().TypeSize()))
		size = addRunBytes(size, area-oldAreas[i])
	}
	size = addRunBytes(size, constantRunExpansion(src, rows))
	return size, size != math.MaxInt
}

// materializedBatchSize bounds both the staged area and the size after merge
// expands shared values. Use a per-vector maximum: different columns may have
// dead area and shared descriptors, respectively. The bound travels with the
// staged batch, including when sorting clears its disjointness flag.
func materializedBatchSize(bat *batch.Batch) int {
	size := 0
	for _, vec := range bat.Vecs {
		rows := vec.Length()
		if vec.IsConst() {
			rows = bat.RowCount()
		}
		size = addRunBytes(size, multiplyRunBytes(rows, vec.GetType().TypeSize()))
		if !vec.GetType().IsVarlen() || len(vec.GetArea()) == 0 {
			continue
		}
		if vec.VarlenaAreaIsDisjoint() || vec.IsConstNull() {
			size = addRunBytes(size, len(vec.GetArea()))
			continue
		}
		values, _ := vector.MustVarlenaRawData(vec)
		payload := 0
		if vec.IsConst() {
			if !values[0].IsSmall() {
				_, n := values[0].OffsetLen()
				payload = multiplyRunBytes(rows, int(n))
			}
		} else {
			for i := 0; i < rows; i++ {
				if !vec.GetNulls().Contains(uint64(i)) && !values[i].IsSmall() {
					_, n := values[i].OffsetLen()
					payload = addRunBytes(payload, int(n))
				}
			}
		}
		size = addRunBytes(size, max(len(vec.GetArea()), payload))
	}
	return size
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

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

package vector

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// unionDictionaryBatch runs after ordinary destination preflight/admission.
// It gathers directly from the immutable backing into the existing destination;
// source metadata remains indexed by logical row, including repeated values.
func (v *Vector) unionDictionaryBatch(w *Vector, offset int64, count int, flags []uint8,
	overrides []types.StringSource, added int, areaWasDisjoint bool, mp *mpool.MPool) error {
	backing := w.dictionaryState()
	base, rows := backing.base.vec, backing.rows.storageFixed()
	oldLength := v.length
	v.setLengthAfterExtend(oldLength + added)
	appendBatchGrouping(v, w, oldLength, offset, count, flags)
	width := v.typ.TypeSize()
	var dstValues, srcValues []types.Varlena
	if v.typ.IsVarlen() {
		dstValues = toSliceOfLengthNoTypeCheck[types.Varlena](v, v.length)
		if !base.IsConstNull() {
			physicalLength := base.length
			if base.IsConst() {
				physicalLength = 1
			}
			srcValues = toSliceOfLengthNoTypeCheck[types.Varlena](base, physicalLength)
		}
	}
	sourceCount := count
	if flags != nil {
		sourceCount = len(flags)
	}
	next := oldLength
	for row := 0; row < sourceCount; row++ {
		if flags != nil && flags[row] == 0 {
			continue
		}
		logical := int(offset) + row
		if w.IsNull(uint64(logical)) {
			nulls.Add(&v.nsp, uint64(next))
			if v.typ.IsVarlen() {
				dstValues[next] = types.Varlena{}
			}
		} else {
			physical := int(rows[logical])
			if base.IsConst() {
				physical = 0
			}
			if v.typ.IsVarlen() {
				if err := BuildVarlenaFromVarlena(v, &dstValues[next], &srcValues[physical], &base.area, mp); err != nil {
					return err
				}
			} else {
				copy(v.data[next*width:(next+1)*width], base.data[physical*width:(physical+1)*width])
			}
		}
		next++
	}
	if err := v.propagatePrepareParamKindsBatch(w, oldLength, offset, count, flags, overrides, mp); err != nil {
		return err
	}
	if err := v.propagateBinaryStringBatch(w, oldLength, offset, count, flags, mp); err != nil {
		return err
	}
	if v.typ.IsVarlen() {
		v.areaDisjoint = areaWasDisjoint
	}
	return nil
}

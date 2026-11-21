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

package aggexec

import (
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Selected counterparts retain each flat kernel's accumulation, overflow and
// publication order. Only physical input lookup changes; flat loops stay intact.

func (exec *sumDecimal64FastExec) bulkFillSingleGroupDictionary(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConstNull() {
		return nil
	}
	n := vec.Length()
	if n == 0 {
		return nil
	}

	view := vector.ResolveFixedReadView[types.Decimal64](vec)
	vals := view.Values
	hasNull := vec.HasNull()
	var np *bitmap.Bitmap
	if hasNull {
		np = vec.GetNulls().GetBitmap()
	}

	g := uint64(groupIndex)
	x := int(g >> aggBatchSizeShift)
	y := g & aggBatchSizeMask
	sums := chunkArr[types.Decimal128](exec.state[x].vecs[0])
	cnts := vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])

	var localSum types.Decimal128
	var localCnt int64
	for i := 0; i < n; i++ {
		if hasNull && np.Contains(uint64(i)) {
			continue
		}
		physical := int(view.Selection[i])
		if view.Constant {
			physical = 0
		}
		raw := vals[physical]
		val := types.Decimal128{B0_63: uint64(raw), B64_127: uint64(int64(raw) >> 63)}
		if localCnt == 0 {
			localSum = val
		} else {
			localSum = localSum.Add128Unchecked(val)
		}
		localCnt++
	}
	if localCnt == 0 {
		return nil
	}
	sums[y] = sums[y].Add128Unchecked(localSum)
	cnts[y] += localCnt
	return nil
}

func (exec *sumDecimal64FastExec) batchFillDictionary(offset int, groups []uint64, vectors []*vector.Vector) error {
	if len(exec.state) == 1 && exec.state[0].length <= 256 {
		return exec.batchFillDenseDictionary(offset, groups, vectors[0])
	}
	vec := vectors[0]
	view := vector.ResolveFixedReadView[types.Decimal64](vec)
	vals := view.Values
	hasNull := vec.HasNull()
	var np *bitmap.Bitmap
	if hasNull {
		np = vec.GetNulls().GetBitmap()
	}

	const maxSlots = 255
	var slotOf [256]uint8
	var localSums [maxSlots]types.Decimal128
	var localCnts [maxSlots]int64
	var localGrps [maxSlots]uint64
	nSlots := 0
	for i := 0; i < len(slotOf); i += 8 {
		binary.LittleEndian.PutUint64(slotOf[i:i+8], ^uint64(0))
	}
	fallbackLastX := -1
	var fallbackSums []types.Decimal128
	var fallbackCnts []int64

	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		idx := i + offset
		if hasNull && np.Contains(uint64(idx)) {
			continue
		}
		g := grp - 1
		physical := int(view.Selection[idx])
		if view.Constant {
			physical = 0
		}
		raw := vals[physical]
		val := types.Decimal128{B0_63: uint64(raw), B64_127: uint64(int64(raw) >> 63)}
		// Once the local table is full, probing it can scan nearly all 256
		// slots per row. Direct updates still compose with the deferred scatter
		// below, including when the group already has a local entry.
		if nSlots >= maxSlots {
			x := int(g >> aggBatchSizeShift)
			y := g & aggBatchSizeMask
			if x != fallbackLastX {
				fallbackLastX = x
				fallbackSums = chunkArr[types.Decimal128](exec.state[x].vecs[0])
				fallbackCnts = vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])
			}
			fallbackSums[y] = fallbackSums[y].Add128Unchecked(val)
			fallbackCnts[y]++
			continue
		}

		for h := uint8(g) ^ uint8(g>>8); ; h++ {
			s := slotOf[h]
			if s == 0xFF {
				slotOf[h] = uint8(nSlots)
				localGrps[nSlots] = g
				localSums[nSlots] = val
				localCnts[nSlots] = 1
				nSlots++
				break
			}
			if localGrps[s] == g {
				localSums[s] = localSums[s].Add128Unchecked(val)
				localCnts[s]++
				break
			}
		}
	}

	lastX := -1
	var sums []types.Decimal128
	var cnts []int64
	for s := 0; s < nSlots; s++ {
		g := localGrps[s]
		x := int(g >> aggBatchSizeShift)
		if x != lastX {
			lastX = x
			sums = chunkArr[types.Decimal128](exec.state[x].vecs[0])
			cnts = vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])
		}
		y := g & aggBatchSizeMask
		sums[y] = sums[y].Add128Unchecked(localSums[s])
		cnts[y] += localCnts[s]
	}
	return nil
}

func (exec *sumDecimal64FastExec) batchFillDenseGroupsDictionary(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	view := vector.ResolveFixedReadView[types.Decimal64](vec)
	vals := view.Values
	var np *bitmap.Bitmap
	if vec.HasNull() {
		np = vec.GetNulls().GetBitmap()
	}

	var localSums [256]types.Decimal128
	var localCnts [256]int64
	var touched [256]uint8
	nTouched := 0
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		idx := i + offset
		if np != nil && np.Contains(uint64(idx)) {
			continue
		}
		g := grp - 1
		physical := int(view.Selection[idx])
		if view.Constant {
			physical = 0
		}
		raw := vals[physical]
		value := types.Decimal128{B0_63: uint64(raw), B64_127: uint64(int64(raw) >> 63)}
		if localCnts[g] == 0 {
			touched[nTouched] = uint8(g)
			nTouched++
			localSums[g] = value
		} else {
			localSums[g] = localSums[g].Add128Unchecked(value)
		}
		localCnts[g]++
	}

	sums := chunkArr[types.Decimal128](exec.state[0].vecs[0])
	cnts := vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[1])
	for _, g := range touched[:nTouched] {
		sums[g] = sums[g].Add128Unchecked(localSums[g])
		cnts[g] += localCnts[g]
	}
	return nil
}

func (exec *sumDecimal128FastExec) bulkFillSingleGroupDictionary(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConstNull() {
		return nil
	}
	n := vec.Length()
	if n == 0 {
		return nil
	}

	view := vector.ResolveFixedReadView[types.Decimal128](vec)
	vals := view.Values
	hasNull := vec.HasNull()
	var np *bitmap.Bitmap
	if hasNull {
		np = vec.GetNulls().GetBitmap()
	}
	checked := exec.overflowCheck

	g := uint64(groupIndex)
	x := int(g >> aggBatchSizeShift)
	y := g & aggBatchSizeMask
	sums := chunkArr[types.Decimal128](exec.state[x].vecs[0])
	cnts := vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])

	var localSum types.Decimal128
	var localCnt int64
	for i := 0; i < n; i++ {
		if hasNull && np.Contains(uint64(i)) {
			continue
		}
		physical := int(view.Selection[i])
		if view.Constant {
			physical = 0
		}
		val := vals[physical]
		if localCnt == 0 {
			localSum = val
		} else {
			if checked {
				var err error
				if localSum, err = localSum.Add128(val); err != nil {
					return err
				}
			} else {
				localSum = localSum.Add128Unchecked(val)
			}
		}
		localCnt++
	}
	if localCnt == 0 {
		return nil
	}
	if checked {
		var err error
		if sums[y], err = sums[y].Add128(localSum); err != nil {
			return err
		}
	} else {
		sums[y] = sums[y].Add128Unchecked(localSum)
	}
	cnts[y] += localCnt
	return nil
}

func (exec *sumDecimal128FastExec) batchFillDictionary(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConstNull() {
		return nil
	}
	view := vector.ResolveFixedReadView[types.Decimal128](vec)
	vals := view.Values
	hasNull := vec.HasNull()
	var np *bitmap.Bitmap
	if hasNull {
		np = vec.GetNulls().GetBitmap()
	}
	checked := exec.overflowCheck

	const maxSlots = 255
	var slotOf [256]uint8
	var localSums [maxSlots]types.Decimal128
	var localCnts [maxSlots]int64
	var localGrps [maxSlots]uint64
	nSlots := 0
	for i := range slotOf {
		slotOf[i] = 0xFF
	}

	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		idx := i + offset
		if hasNull && np.Contains(uint64(idx)) {
			continue
		}
		g := grp - 1
		physical := int(view.Selection[idx])
		if view.Constant {
			physical = 0
		}
		val := vals[physical]

		for h := uint8(g) ^ uint8(g>>8); ; h++ {
			s := slotOf[h]
			if s == 0xFF {
				if nSlots >= maxSlots {
					x := int(g >> aggBatchSizeShift)
					y := g & aggBatchSizeMask
					sums := chunkArr[types.Decimal128](exec.state[x].vecs[0])
					if checked {
						var err error
						if sums[y], err = sums[y].Add128(val); err != nil {
							return err
						}
					} else {
						sums[y] = sums[y].Add128Unchecked(val)
					}
					vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])[y]++
					break
				}
				slotOf[h] = uint8(nSlots)
				localGrps[nSlots] = g
				localSums[nSlots] = val
				localCnts[nSlots] = 1
				nSlots++
				break
			}
			if localGrps[s] == g {
				if checked {
					var err error
					if localSums[s], err = localSums[s].Add128(val); err != nil {
						return err
					}
				} else {
					localSums[s] = localSums[s].Add128Unchecked(val)
				}
				localCnts[s]++
				break
			}
		}
	}

	lastX := -1
	var sums []types.Decimal128
	var cnts []int64
	for s := 0; s < nSlots; s++ {
		g := localGrps[s]
		x := int(g >> aggBatchSizeShift)
		if x != lastX {
			lastX = x
			sums = chunkArr[types.Decimal128](exec.state[x].vecs[0])
			cnts = vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])
		}
		y := g & aggBatchSizeMask
		if checked {
			var err error
			if sums[y], err = sums[y].Add128(localSums[s]); err != nil {
				return err
			}
		} else {
			sums[y] = sums[y].Add128Unchecked(localSums[s])
		}
		cnts[y] += localCnts[s]
	}
	return nil
}

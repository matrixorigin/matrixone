// Copyright 2021 Matrix Origin
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

package index

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/index/bsi"
)

func BuildBitSlicedIndex(data []*vector.Vector, t types.Type, colIdx int16, startPos int) (bsi.BitSlicedIndex, error) {
	switch t.Oid {
	case types.T_int8:
		bsiIdx := NewNumericBsiIndex(t, 8, colIdx)
		row := startPos
		for _, part := range data {
			column := part.Col.([]int8)
			for _, val := range column {
				if nulls.Contains(part.Nsp, uint64(row-startPos)) {
					row++
					continue
				}
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_int16:
		bsiIdx := NewNumericBsiIndex(t, 16, colIdx)
		row := startPos
		for _, part := range data {
			column := part.Col.([]int16)
			for _, val := range column {
				if nulls.Contains(part.Nsp, uint64(row-startPos)) {
					row++
					continue
				}
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_int32:
		bsiIdx := NewNumericBsiIndex(t, 32, colIdx)
		row := startPos
		for _, part := range data {
			column := part.Col.([]int32)
			for _, val := range column {
				if nulls.Contains(part.Nsp, uint64(row-startPos)) {
					row++
					continue
				}
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_int64:
		bsiIdx := NewNumericBsiIndex(t, 64, colIdx)
		row := startPos
		for _, part := range data {
			column := part.Col.([]int64)
			for _, val := range column {
				if nulls.Contains(part.Nsp, uint64(row-startPos)) {
					row++
					continue
				}
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_uint8:
		bsiIdx := NewNumericBsiIndex(t, 8, colIdx)
		row := startPos
		for _, part := range data {
			column := part.Col.([]uint8)
			for _, val := range column {
				if nulls.Contains(part.Nsp, uint64(row-startPos)) {
					row++
					continue
				}
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_uint16:
		bsiIdx := NewNumericBsiIndex(t, 16, colIdx)
		row := startPos
		for _, part := range data {
			column := part.Col.([]uint16)
			for _, val := range column {
				if nulls.Contains(part.Nsp, uint64(row-startPos)) {
					row++
					continue
				}
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_uint32:
		bsiIdx := NewNumericBsiIndex(t, 32, colIdx)
		row := startPos
		for _, part := range data {
			column := part.Col.([]uint32)
			for _, val := range column {
				if nulls.Contains(part.Nsp, uint64(row-startPos)) {
					row++
					continue
				}
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_uint64:
		bsiIdx := NewNumericBsiIndex(t, 64, colIdx)
		row := startPos
		for _, part := range data {
			column := part.Col.([]uint64)
			for _, val := range column {
				if nulls.Contains(part.Nsp, uint64(row-startPos)) {
					row++
					continue
				}
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_float32:
		bsiIdx := NewNumericBsiIndex(t, 32, colIdx)
		row := startPos
		for _, part := range data {
			column := part.Col.([]float32)
			for _, val := range column {
				if nulls.Contains(part.Nsp, uint64(row-startPos)) {
					row++
					continue
				}
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_float64:
		bsiIdx := NewNumericBsiIndex(t, 64, colIdx)
		row := startPos
		for _, part := range data {
			column := part.Col.([]float64)
			for _, val := range column {
				if nulls.Contains(part.Nsp, uint64(row-startPos)) {
					row++
					continue
				}
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_datetime:
		bsiIdx := NewNumericBsiIndex(t, 64, colIdx)
		row := startPos
		for _, part := range data {
			column := part.Col.([]types.Datetime)
			for _, val := range column {
				if nulls.Contains(part.Nsp, uint64(row-startPos)) {
					row++
					continue
				}
				if err := bsiIdx.Set(uint64(row), int64(val)); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_date:
		bsiIdx := NewNumericBsiIndex(t, 32, colIdx)
		row := startPos
		for _, part := range data {
			column := part.Col.([]types.Date)
			for _, val := range column {
				if nulls.Contains(part.Nsp, uint64(row-startPos)) {
					row++
					continue
				}
				if err := bsiIdx.Set(uint64(row), int32(val)); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_char:
		bsiIdx := NewStringBsiIndex(t, colIdx)
		row := startPos
		for _, part := range data {
			column := part.Col.(*types.Bytes)
			for i := 0; i < len(column.Lengths); i++ {
					if nulls.Contains(part.Nsp, uint64(row-startPos)) {
						row++
						continue
					}
					val := column.Get(int64(i))
					if err := bsiIdx.Set(uint64(row), val); err != nil {
						return nil, err
					}
					row++
			}
		}
		return bsiIdx, nil
	default:
		panic("unsupported type")
	}
}

func BuildSegmentZoneMapIndex(data []*vector.Vector, t types.Type, colIdx int16, isSorted bool) (Index, error) {
	switch t.Oid {
	case types.T_int8:
		var globalMin, globalMax int8
		var partMins, partMaxs []interface{}
		if isSorted {
			for i, vec := range data {
				part := vec.Col.([]int8)
				partMins = append(partMins, part[0])
				partMaxs = append(partMaxs, part[len(part) - 1])
				if i == 0 {
					globalMin = part[0]
				}
				if i == len(data) - 1 {
					globalMax = part[len(part) - 1]
				}
			}
		} else {
			var minv, maxv int8
			for i, vec := range data {
				part := vec.Col.([]int8)
				if i == 0 {
					globalMin = part[i]
					globalMax = part[i]
				}
				for j, v := range part {
					if j == 0 {
						minv = v
						maxv = v
					}
					if v > globalMax {
						globalMax = v
					}
					if v < globalMin {
						globalMin = v
					}
					if v > maxv {
						maxv = v
					}
					if v < minv {
						minv = v
					}
				}
				partMaxs = append(partMaxs, maxv)
				partMins = append(partMins, minv)
			}
		}
		zmi := NewSegmentZoneMap(t, globalMin, globalMax, colIdx, partMins, partMaxs)
		return zmi, nil
	case types.T_int16:
		var globalMin, globalMax int16
		var partMins, partMaxs []interface{}
		if isSorted {
			for i, vec := range data {
				part := vec.Col.([]int16)
				partMins = append(partMins, part[0])
				partMaxs = append(partMaxs, part[len(part) - 1])
				if i == 0 {
					globalMin = part[0]
				}
				if i == len(data) - 1 {
					globalMax = part[len(part) - 1]
				}
			}
		} else {
			var minv, maxv int16
			for i, vec := range data {
				part := vec.Col.([]int16)
				if i == 0 {
					globalMin = part[i]
					globalMax = part[i]
				}
				for j, v := range part {
					if j == 0 {
						minv = v
						maxv = v
					}
					if v > globalMax {
						globalMax = v
					}
					if v < globalMin {
						globalMin = v
					}
					if v > maxv {
						maxv = v
					}
					if v < minv {
						minv = v
					}
				}
				partMaxs = append(partMaxs, maxv)
				partMins = append(partMins, minv)
			}
		}
		zmi := NewSegmentZoneMap(t, globalMin, globalMax, colIdx, partMins, partMaxs)
		return zmi, nil
	case types.T_int32:
		var globalMin, globalMax int32
		var partMins, partMaxs []interface{}
		if isSorted {
			for i, vec := range data {
				part := vec.Col.([]int32)
				partMins = append(partMins, part[0])
				partMaxs = append(partMaxs, part[len(part) - 1])
				if i == 0 {
					globalMin = part[0]
				}
				if i == len(data) - 1 {
					globalMax = part[len(part) - 1]
				}
			}
		} else {
			var minv, maxv int32
			for i, vec := range data {
				part := vec.Col.([]int32)
				if i == 0 {
					globalMin = part[i]
					globalMax = part[i]
				}
				for j, v := range part {
					if j == 0 {
						minv = v
						maxv = v
					}
					if v > globalMax {
						globalMax = v
					}
					if v < globalMin {
						globalMin = v
					}
					if v > maxv {
						maxv = v
					}
					if v < minv {
						minv = v
					}
				}
				partMaxs = append(partMaxs, maxv)
				partMins = append(partMins, minv)
			}
		}
		zmi := NewSegmentZoneMap(t, globalMin, globalMax, colIdx, partMins, partMaxs)
		return zmi, nil
	case types.T_int64:
		var globalMin, globalMax int64
		var partMins, partMaxs []interface{}
		if isSorted {
			for i, vec := range data {
				part := vec.Col.([]int64)
				partMins = append(partMins, part[0])
				partMaxs = append(partMaxs, part[len(part) - 1])
				if i == 0 {
					globalMin = part[0]
				}
				if i == len(data) - 1 {
					globalMax = part[len(part) - 1]
				}
			}
		} else {
			var minv, maxv int64
			for i, vec := range data {
				part := vec.Col.([]int64)
				if i == 0 {
					globalMin = part[i]
					globalMax = part[i]
				}
				for j, v := range part {
					if j == 0 {
						minv = v
						maxv = v
					}
					if v > globalMax {
						globalMax = v
					}
					if v < globalMin {
						globalMin = v
					}
					if v > maxv {
						maxv = v
					}
					if v < minv {
						minv = v
					}
				}
				partMaxs = append(partMaxs, maxv)
				partMins = append(partMins, minv)
			}
		}
		zmi := NewSegmentZoneMap(t, globalMin, globalMax, colIdx, partMins, partMaxs)
		return zmi, nil
	case types.T_uint8:
		var globalMin, globalMax uint8
		var partMins, partMaxs []interface{}
		if isSorted {
			for i, vec := range data {
				part := vec.Col.([]uint8)
				partMins = append(partMins, part[0])
				partMaxs = append(partMaxs, part[len(part) - 1])
				if i == 0 {
					globalMin = part[0]
				}
				if i == len(data) - 1 {
					globalMax = part[len(part) - 1]
				}
			}
		} else {
			var minv, maxv uint8
			for i, vec := range data {
				part := vec.Col.([]uint8)
				if i == 0 {
					globalMin = part[i]
					globalMax = part[i]
				}
				for j, v := range part {
					if j == 0 {
						minv = v
						maxv = v
					}
					if v > globalMax {
						globalMax = v
					}
					if v < globalMin {
						globalMin = v
					}
					if v > maxv {
						maxv = v
					}
					if v < minv {
						minv = v
					}
				}
				partMaxs = append(partMaxs, maxv)
				partMins = append(partMins, minv)
			}
		}
		zmi := NewSegmentZoneMap(t, globalMin, globalMax, colIdx, partMins, partMaxs)
		return zmi, nil
	case types.T_uint16:
		var globalMin, globalMax uint16
		var partMins, partMaxs []interface{}
		if isSorted {
			for i, vec := range data {
				part := vec.Col.([]uint16)
				partMins = append(partMins, part[0])
				partMaxs = append(partMaxs, part[len(part) - 1])
				if i == 0 {
					globalMin = part[0]
				}
				if i == len(data) - 1 {
					globalMax = part[len(part) - 1]
				}
			}
		} else {
			var minv, maxv uint16
			for i, vec := range data {
				part := vec.Col.([]uint16)
				if i == 0 {
					globalMin = part[i]
					globalMax = part[i]
				}
				for j, v := range part {
					if j == 0 {
						minv = v
						maxv = v
					}
					if v > globalMax {
						globalMax = v
					}
					if v < globalMin {
						globalMin = v
					}
					if v > maxv {
						maxv = v
					}
					if v < minv {
						minv = v
					}
				}
				partMaxs = append(partMaxs, maxv)
				partMins = append(partMins, minv)
			}
		}
		zmi := NewSegmentZoneMap(t, globalMin, globalMax, colIdx, partMins, partMaxs)
		return zmi, nil
	case types.T_uint32:
		var globalMin, globalMax uint32
		var partMins, partMaxs []interface{}
		if isSorted {
			for i, vec := range data {
				part := vec.Col.([]uint32)
				partMins = append(partMins, part[0])
				partMaxs = append(partMaxs, part[len(part) - 1])
				if i == 0 {
					globalMin = part[0]
				}
				if i == len(data) - 1 {
					globalMax = part[len(part) - 1]
				}
			}
		} else {
			var minv, maxv uint32
			for i, vec := range data {
				part := vec.Col.([]uint32)
				if i == 0 {
					globalMin = part[i]
					globalMax = part[i]
				}
				for j, v := range part {
					if j == 0 {
						minv = v
						maxv = v
					}
					if v > globalMax {
						globalMax = v
					}
					if v < globalMin {
						globalMin = v
					}
					if v > maxv {
						maxv = v
					}
					if v < minv {
						minv = v
					}
				}
				partMaxs = append(partMaxs, maxv)
				partMins = append(partMins, minv)
			}
		}
		zmi := NewSegmentZoneMap(t, globalMin, globalMax, colIdx, partMins, partMaxs)
		return zmi, nil
	case types.T_uint64:
		var globalMin, globalMax uint64
		var partMins, partMaxs []interface{}
		if isSorted {
			for i, vec := range data {
				part := vec.Col.([]uint64)
				partMins = append(partMins, part[0])
				partMaxs = append(partMaxs, part[len(part) - 1])
				if i == 0 {
					globalMin = part[0]
				}
				if i == len(data) - 1 {
					globalMax = part[len(part) - 1]
				}
			}
		} else {
			var minv, maxv uint64
			for i, vec := range data {
				part := vec.Col.([]uint64)
				if i == 0 {
					globalMin = part[i]
					globalMax = part[i]
				}
				for j, v := range part {
					if j == 0 {
						minv = v
						maxv = v
					}
					if v > globalMax {
						globalMax = v
					}
					if v < globalMin {
						globalMin = v
					}
					if v > maxv {
						maxv = v
					}
					if v < minv {
						minv = v
					}
				}
				partMaxs = append(partMaxs, maxv)
				partMins = append(partMins, minv)
			}
		}
		zmi := NewSegmentZoneMap(t, globalMin, globalMax, colIdx, partMins, partMaxs)
		return zmi, nil
	case types.T_float32:
		var globalMin, globalMax float32
		var partMins, partMaxs []interface{}
		if isSorted {
			for i, vec := range data {
				part := vec.Col.([]float32)
				partMins = append(partMins, part[0])
				partMaxs = append(partMaxs, part[len(part) - 1])
				if i == 0 {
					globalMin = part[0]
				}
				if i == len(data) - 1 {
					globalMax = part[len(part) - 1]
				}
			}
		} else {
			var minv, maxv float32
			for i, vec := range data {
				part := vec.Col.([]float32)
				if i == 0 {
					globalMin = part[i]
					globalMax = part[i]
				}
				for j, v := range part {
					if j == 0 {
						minv = v
						maxv = v
					}
					if v > globalMax {
						globalMax = v
					}
					if v < globalMin {
						globalMin = v
					}
					if v > maxv {
						maxv = v
					}
					if v < minv {
						minv = v
					}
				}
				partMaxs = append(partMaxs, maxv)
				partMins = append(partMins, minv)
			}
		}
		zmi := NewSegmentZoneMap(t, globalMin, globalMax, colIdx, partMins, partMaxs)
		return zmi, nil
	case types.T_float64:
		var globalMin, globalMax float64
		var partMins, partMaxs []interface{}
		if isSorted {
			for i, vec := range data {
				part := vec.Col.([]float64)
				partMins = append(partMins, part[0])
				partMaxs = append(partMaxs, part[len(part) - 1])
				if i == 0 {
					globalMin = part[0]
				}
				if i == len(data) - 1 {
					globalMax = part[len(part) - 1]
				}
			}
		} else {
			var minv, maxv float64
			for i, vec := range data {
				part := vec.Col.([]float64)
				if i == 0 {
					globalMin = part[i]
					globalMax = part[i]
				}
				for j, v := range part {
					if j == 0 {
						minv = v
						maxv = v
					}
					if v > globalMax {
						globalMax = v
					}
					if v < globalMin {
						globalMin = v
					}
					if v > maxv {
						maxv = v
					}
					if v < minv {
						minv = v
					}
				}
				partMaxs = append(partMaxs, maxv)
				partMins = append(partMins, minv)
			}
		}
		zmi := NewSegmentZoneMap(t, globalMin, globalMax, colIdx, partMins, partMaxs)
		return zmi, nil
	case types.T_datetime:
		var globalMin, globalMax types.Datetime
		var partMins, partMaxs []interface{}
		if isSorted {
			for i, vec := range data {
				part := vec.Col.([]types.Datetime)
				partMins = append(partMins, part[0])
				partMaxs = append(partMaxs, part[len(part) - 1])
				if i == 0 {
					globalMin = part[0]
				}
				if i == len(data) - 1 {
					globalMax = part[len(part) - 1]
				}
			}
		} else {
			var minv, maxv types.Datetime
			for i, vec := range data {
				part := vec.Col.([]types.Datetime)
				if i == 0 {
					globalMin = part[i]
					globalMax = part[i]
				}
				for j, v := range part {
					if j == 0 {
						minv = v
						maxv = v
					}
					if v > globalMax {
						globalMax = v
					}
					if v < globalMin {
						globalMin = v
					}
					if v > maxv {
						maxv = v
					}
					if v < minv {
						minv = v
					}
				}
				partMaxs = append(partMaxs, maxv)
				partMins = append(partMins, minv)
			}
		}
		zmi := NewSegmentZoneMap(t, globalMin, globalMax, colIdx, partMins, partMaxs)
		return zmi, nil
	case types.T_date:
		var globalMin, globalMax types.Date
		var partMins, partMaxs []interface{}
		if isSorted {
			for i, vec := range data {
				part := vec.Col.([]types.Date)
				partMins = append(partMins, part[0])
				partMaxs = append(partMaxs, part[len(part) - 1])
				if i == 0 {
					globalMin = part[0]
				}
				if i == len(data) - 1 {
					globalMax = part[len(part) - 1]
				}
			}
		} else {
			var minv, maxv types.Date
			for i, vec := range data {
				part := vec.Col.([]types.Date)
				if i == 0 {
					globalMin = part[i]
					globalMax = part[i]
				}
				for j, v := range part {
					if j == 0 {
						minv = v
						maxv = v
					}
					if v > globalMax {
						globalMax = v
					}
					if v < globalMin {
						globalMin = v
					}
					if v > maxv {
						maxv = v
					}
					if v < minv {
						minv = v
					}
				}
				partMaxs = append(partMaxs, maxv)
				partMins = append(partMins, minv)
			}
		}
		zmi := NewSegmentZoneMap(t, globalMin, globalMax, colIdx, partMins, partMaxs)
		return zmi, nil
	case types.T_char, types.T_varchar, types.T_json:
		var globalMin, globalMax []byte
		var partMins, partMaxs []interface{}
		if isSorted {
			for i, vec := range data {
				part := vec.Col.(*types.Bytes)
				partMins = append(partMins, part.Get(0))
				partMaxs = append(partMaxs, part.Get(int64(len(part.Offsets)-1)))
				if i == 0 {
					globalMin = part.Get(0)
				}
				if i == len(data) - 1 {
					globalMax = part.Get(int64(len(part.Offsets)-1))
				}
			}
		} else {
			var minv, maxv []byte
			for i, vec := range data {
				part := vec.Col.(*types.Bytes)
				if i == 0 {
					globalMin = part.Get(0)
					globalMax = part.Get(0)
				}
				for j := int64(0); j < int64(len(part.Offsets)); j++ {
					v := part.Get(j)
					if j == 0 {
						minv = v
						maxv = v
					}
					if bytes.Compare(v, globalMax) > 0 {
						globalMax = v
					}
					if bytes.Compare(v, globalMin) < 0 {
						globalMin = v
					}
					if bytes.Compare(v, maxv) > 0 {
						maxv = v
					}
					if bytes.Compare(v, minv) < 0 {
						minv = v
					}
				}
				partMaxs = append(partMaxs, maxv)
				partMins = append(partMins, minv)
			}
		}
		zmi := NewSegmentZoneMap(t, globalMin, globalMax, colIdx, partMins, partMaxs)
		return zmi, nil
	default:
		panic("unsupported type")
	}
}

func BuildBlockZoneMapIndex(data *vector.Vector, t types.Type, colIdx int16, isSorted bool) (Index, error) {
	switch t.Oid {
	case types.T_int8:
		vec := data.Col.([]int8)
		var min, max int8
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for i, e := range vec {
				if nulls.Contains(data.Nsp, uint64(i)) {
					continue
				}
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_int16:
		vec := data.Col.([]int16)
		var min, max int16
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for i, e := range vec {
				if nulls.Contains(data.Nsp, uint64(i)) {
					continue
				}
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_int32:
		vec := data.Col.([]int32)
		var min, max int32
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for i, e := range vec {
				if nulls.Contains(data.Nsp, uint64(i)) {
					continue
				}
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_int64:
		vec := data.Col.([]int64)
		var min, max int64
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for i, e := range vec {
				if nulls.Contains(data.Nsp, uint64(i)) {
					continue
				}
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_uint8:
		vec := data.Col.([]uint8)
		var min, max uint8
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for i, e := range vec {
				if nulls.Contains(data.Nsp, uint64(i)) {
					continue
				}
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_uint16:
		vec := data.Col.([]uint16)
		var min, max uint16
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for i, e := range vec {
				if nulls.Contains(data.Nsp, uint64(i)) {
					continue
				}
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_uint32:
		vec := data.Col.([]uint32)
		var min, max uint32
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for i, e := range vec {
				if nulls.Contains(data.Nsp, uint64(i)) {
					continue
				}
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_uint64:
		vec := data.Col.([]uint64)
		var min, max uint64
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for i, e := range vec {
				if nulls.Contains(data.Nsp, uint64(i)) {
					continue
				}
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_float32:
		vec := data.Col.([]float32)
		var min, max float32
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for i, e := range vec {
				if nulls.Contains(data.Nsp, uint64(i)) {
					continue
				}
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_float64:
		vec := data.Col.([]float64)
		var min, max float64
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for i, e := range vec {
				if nulls.Contains(data.Nsp, uint64(i)) {
					continue
				}
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_date:
		vec := data.Col.([]types.Date)
		var min, max types.Date
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for i, e := range vec {
				if nulls.Contains(data.Nsp, uint64(i)) {
					continue
				}
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_datetime:
		vec := data.Col.([]types.Datetime)
		var min, max types.Datetime
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for i, e := range vec {
				if nulls.Contains(data.Nsp, uint64(i)) {
					continue
				}
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_char, types.T_varchar, types.T_json:
		vec := data.Col.(*types.Bytes)
		var min, max []byte
		if isSorted {
			min = vec.Get(0)
			max = vec.Get(int64(len(vec.Lengths) - 1))
		} else {
			min = vec.Get(0)
			max = min
			for i := 0; i < len(vec.Lengths); i++ {
				if nulls.Contains(data.Nsp, uint64(i)) {
					continue
				}
				e := vec.Get(int64(i))
				if bytes.Compare(e, max) > 0 {
					max = e
				}
				if bytes.Compare(e, min) < 0 {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	default:
		panic("unsupported")
	}
}
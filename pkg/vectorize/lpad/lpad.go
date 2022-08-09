// Copyright 2022 Matrix Origin
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

package lpad

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	Lpad        func(res *types.Bytes, src *types.Bytes, length uint32, pad *types.Bytes) *types.Bytes
	LpadVarchar func(a *types.Bytes, b []int64, c *types.Bytes) *types.Bytes
)

func init() {
	Lpad = lpadPure
	LpadVarchar = lpadVarcharPure
}

func lpadVarcharPure(a *types.Bytes, b []int64, c *types.Bytes) *types.Bytes {
	var res = &types.Bytes{}
	//in fact,the length of three slice is the same with each other
	for i := 0; i < len(a.Lengths); i++ {
		if a.Lengths[i] > uint32(b[0]) { //length less
			res.Offsets = append(res.Offsets, uint32(len(res.Data)))
			res.Data = append(res.Data, a.Data[a.Offsets[i]:a.Offsets[i]+uint32(b[0])]...)
			res.Lengths = append(res.Lengths, uint32(b[0]))
		} else {
			lens := uint32(b[0]) - a.Lengths[i]
			t1 := lens / c.Lengths[0]
			t2 := lens % c.Lengths[0]
			temp := []byte{}
			for j := 0; j < int(t1); j++ {
				temp = append(temp, c.Data[c.Offsets[0]:c.Offsets[0]+c.Lengths[0]]...)
			}
			temp = append(temp, c.Data[c.Offsets[0]:c.Offsets[0]+t2]...)
			temp = append(temp, a.Data[a.Offsets[i]:a.Offsets[i]+a.Lengths[i]]...)

			res.Offsets = append(res.Offsets, uint32(len(res.Data)))
			res.Data = append(res.Data, temp...)
			res.Lengths = append(res.Lengths, uint32(len(temp)))
		}
	}
	return res
}

func lpadPure(res *types.Bytes, src *types.Bytes, length uint32, pad *types.Bytes) *types.Bytes {
	var retCursor uint32 = 0
	padLengh := pad.Lengths[0]
	padOffset := pad.Offsets[0]
	padBytes := pad.Data[padOffset:padLengh]
	for idx, offset := range src.Offsets {
		cursor := offset
		curLen := src.Lengths[idx]
		bytes := src.Data[cursor : cursor+curLen]

		if length <= src.Lengths[idx] {
			slice, size := getSliceFromLeft(bytes, length)
			for _, b := range slice {
				res.Data[retCursor] = b
				retCursor++
			}
			if idx != 0 {
				res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
			} else {
				res.Offsets[idx] = uint32(0)
			}
			res.Lengths[idx] = uint32(size)
		} else {
			diff := length - curLen
			if diff <= padLengh {
				leftpad, size := getSliceFromLeft(padBytes, diff)
				for _, b := range leftpad {
					res.Data[retCursor] = b
					retCursor++
				}
				for _, b := range bytes {
					res.Data[retCursor] = b
					retCursor++
				}
				if idx != 0 {
					res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
				} else {
					res.Offsets[idx] = uint32(0)
				}
				res.Lengths[idx] = uint32(size) + curLen
			} else {
				frequency := diff / padLengh
				remainder := diff % padLengh

				for i := uint32(0); i < frequency; i++ {
					for j := uint32(0); j < padLengh; j++ {
						res.Data[retCursor] = padBytes[j]
						retCursor++
					}
				}
				for k := uint32(0); k < remainder; k++ {
					res.Data[retCursor] = padBytes[k]
					retCursor++
				}
				for _, b := range bytes {
					res.Data[retCursor] = b
					retCursor++
				}
				if idx != 0 {
					res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
				} else {
					res.Offsets[idx] = uint32(0)
				}
				res.Lengths[idx] = uint32(length)
			}
		}
	}
	return res
}

// Slice from left to right, starting from 0
func getSliceFromLeft(bytes []byte, length uint32) ([]byte, uint32) {
	elemsize := uint32(len(bytes))
	if length > elemsize {
		return bytes, elemsize
	}
	return bytes[0:length], length
}

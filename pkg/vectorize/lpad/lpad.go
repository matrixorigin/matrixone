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

import "github.com/matrixorigin/matrixone/pkg/container/types"

var (
	LpadVarchar func(*types.Bytes, []int64, *types.Bytes) *types.Bytes
)

func init() {
	LpadVarchar = lpadVarcharPure
}

func lpadVarcharPure(a *types.Bytes, b []int64, c *types.Bytes) *types.Bytes {
	var res *types.Bytes = &types.Bytes{}
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

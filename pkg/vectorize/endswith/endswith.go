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

package endswith

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	endsWithChar    func(*types.Bytes, *types.Bytes, []uint8) []uint8
	endsWithVarChar func(*types.Bytes, *types.Bytes, []uint8) []uint8
)

func init() {
	endsWithChar = EndsWith
	endsWithVarChar = EndsWith
}

func EndsWith(lvs *types.Bytes, rvs *types.Bytes, rs []uint8) []uint8 {
	fmt.Println("here")
	fmt.Println("len(lvs.Offsets)=", len(lvs.Offsets))
	fmt.Println("len(rvs.Offsets)=", len(rvs.Offsets))
	fmt.Println("len(rs)=", len(rs))
	for idx := range lvs.Offsets {
		lv_offset, rv_offset := lvs.Offsets[idx], rvs.Offsets[idx]
		lv_length, rv_length := lvs.Lengths[idx], rvs.Lengths[idx]
		fmt.Println("lv_offset=", lv_offset)
		fmt.Println("rv_offset=", rv_offset)
		fmt.Println("lv_length=", lv_length)
		fmt.Println("rv_length=", rv_length)
		if lv_length < rv_length { // lv is shorter than rv
			rs[idx] = 0
			continue
		}

		i, j := lv_offset+lv_length-1, rv_offset+rv_length-1
		for ; i >= lv_offset && j >= rv_offset; i, j = i-1, j-1 {
			fmt.Println("i=", i)
			fmt.Println("j=", j)
			fmt.Println("lvs.Data[i]=", lvs.Data[i])
			fmt.Println("rvs.Data[j]=", rvs.Data[j])
			if lvs.Data[i] != rvs.Data[j] {
				fmt.Println("before =0")
				rs[idx] = 0
				break
			}
			if j == rv_offset { // two strings are exactly equal
				fmt.Println("before =1")
				rs[idx] = 1
				break
			}
		}
	}
	fmt.Println("finish")
	return rs
}

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

package findinset

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func findInStrList(str, strlist string) uint64 {
	for j, s := range strings.Split(strlist, ",") {
		if s == str {
			return uint64(j + 1)
		}
	}
	return 0
}

func FindInSet(lv, rv *types.Bytes, rs []uint64) []uint64 {
	for i := range lv.Offsets {
		strlist := string(rv.Data[rv.Offsets[i] : rv.Offsets[i]+rv.Lengths[i]])
		target := string(lv.Data[lv.Offsets[i] : lv.Offsets[i]+lv.Lengths[i]])
		rs[i] = findInStrList(target, strlist)
	}
	return rs
}

func FindInSetWithLeftConst(lv, rv *types.Bytes, rs []uint64) []uint64 {
	target := string(lv.Data[lv.Offsets[0] : lv.Offsets[0]+lv.Lengths[0]])
	for i := range rv.Offsets {
		strlist := string(rv.Data[rv.Offsets[i] : rv.Offsets[i]+rv.Lengths[i]])
		rs[i] = findInStrList(target, strlist)
	}
	return rs
}

func FindInSetWithRightConst(lv, rv *types.Bytes, rs []uint64) []uint64 {
	strlist := string(rv.Data[rv.Offsets[0] : rv.Offsets[0]+rv.Lengths[0]])
	for i := range lv.Offsets {
		target := string(lv.Data[lv.Offsets[i] : lv.Offsets[i]+lv.Lengths[i]])
		rs[i] = findInStrList(target, strlist)
	}
	return rs
}

func FindInSetWithAllConst(lv, rv *types.Bytes, rs []uint64) []uint64 {
	target := string(lv.Data[lv.Offsets[0] : lv.Offsets[0]+lv.Lengths[0]])
	strlist := string(rv.Data[rv.Offsets[0] : rv.Offsets[0]+rv.Lengths[0]])
	rs[0] = findInStrList(target, strlist)
	return rs
}

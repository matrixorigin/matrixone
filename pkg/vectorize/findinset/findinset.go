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
)

func findInStrList(str, strlist string) uint64 {
	for j, s := range strings.Split(strlist, ",") {
		if s == str {
			return uint64(j + 1)
		}
	}
	return 0
}

func FindInSet(lv, rv []string, rs []uint64) []uint64 {
	for i := range lv {
		rs[i] = findInStrList(lv[i], rv[i])
	}
	return rs
}

func FindInSetWithLeftConst(lv string, rv []string, rs []uint64) []uint64 {
	for i := range rv {
		rs[i] = findInStrList(lv, rv[i])
	}
	return rs
}

func FindInSetWithRightConst(lv []string, rv string, rs []uint64) []uint64 {
	for i := range lv {
		rs[i] = findInStrList(lv[i], rv)
	}
	return rs
}

func FindInSetWithAllConst(lv, rv string, rs []uint64) []uint64 {
	rs[0] = findInStrList(lv, rv)
	return rs
}

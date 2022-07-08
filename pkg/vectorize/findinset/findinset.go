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

// bytes.Split should be used here instead of converting to a string operation every time, and the specific implementor of the function needs to refactor this part of the code.

func findInStrList(str, strlist string) types.UInt64 {
	for j, s := range strings.Split(strlist, ",") {
		if s == str {
			return types.UInt64(j + 1)
		}
	}
	return 0
}

func FindInSet(lvs, rvs []types.String, rs []types.UInt64) []types.UInt64 {
	for i, lv := range lvs {
		rs[i] = findInStrList(string(lv), string(rvs[i]))
	}
	return rs
}

func FindInSetWithLeftConst(lvs, rvs []types.String, rs []types.UInt64) []types.UInt64 {
	for i, rv := range rvs {
		rs[i] = findInStrList(string(lvs[0]), string(rv))
	}
	return rs
}

func FindInSetWithRightConst(lvs, rvs []types.String, rs []types.UInt64) []types.UInt64 {
	for i, lv := range lvs {
		rs[i] = findInStrList(string(lv), string(rvs[0]))
	}
	return rs
}

func FindInSetWithAllConst(lvs, rvs []types.String, rs []types.UInt64) []types.UInt64 {
	rs[0] = findInStrList(string(lvs[0]), string(rvs[0]))
	return rs
}

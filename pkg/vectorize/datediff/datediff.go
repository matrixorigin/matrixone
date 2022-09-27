// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package datediff

import "github.com/matrixorigin/matrixone/pkg/container/types"

func DateDiff(lv, rv []types.Date, rs []int64) []int64 {
	for i := range lv {
		rs[i] = int64(lv[i] - rv[i])
	}
	return rs
}

func DateDiffRightConst(lv []types.Date, rv types.Date, rs []int64) []int64 {
	for i := range lv {
		rs[i] = int64(lv[i] - rv)
	}
	return rs
}

func DateDiffLeftConst(lv types.Date, rv []types.Date, rs []int64) []int64 {
	for i := range rv {
		rs[i] = int64(lv - rv[i])
	}
	return rs
}

func DateDiffAllConst(lv, rv types.Date, rs []int64) []int64 {
	rs[0] = int64(lv - rv)
	return rs
}

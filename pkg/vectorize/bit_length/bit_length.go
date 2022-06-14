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

package bit_length

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	StrBitLength func(*types.Bytes, []int64) []int64
)

func init() {
	StrBitLength = strBitLength
}

func strBitLength(xs *types.Bytes, rs []int64) []int64 {
	for i, n := range xs.Lengths {
		rs[i] = int64(n) * 8
	}
	return rs
}

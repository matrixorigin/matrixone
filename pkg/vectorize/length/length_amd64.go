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

package length

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"golang.org/x/sys/cpu"
)

func strLengthAvx2Asm(x []uint32, r []int64)
func strLengthAvx512Asm(x []uint32, r []int64)

func init() {
	if cpu.X86.HasAVX512 {
		StrLength = strLengthAvx512
	} else if cpu.X86.HasAVX2 {
		StrLength = strLengthAvx2
	}
}

func strLengthAvx2(xs *types.Bytes, rs []int64) []int64 {
	strLengthAvx2Asm(xs.Lengths, rs)
	return rs
}

func strLengthAvx512(xs *types.Bytes, rs []int64) []int64 {
	strLengthAvx512Asm(xs.Lengths, rs)
	return rs
}

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

package lengthutf8

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"unicode/utf8"
)

var (
	StrLengthUTF8 func(*types.Bytes, []uint64) []uint64
)

func init() {
	StrLengthUTF8 = strLengthUTF8
}

func strLengthUTF8(xs *types.Bytes, rs []uint64) []uint64 {
	for i := range xs.Lengths {
		x := xs.Get(int64(i))
		rs[i] = uint64(utf8.RuneCount(x))
	}
	return rs
}

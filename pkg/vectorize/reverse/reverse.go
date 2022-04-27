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
package reverse

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	ReverseChar    func(*types.Bytes, *types.Bytes) *types.Bytes
	ReverseVarChar func(*types.Bytes, *types.Bytes) *types.Bytes
)

func init() {
	ReverseChar = reverse
	ReverseVarChar = reverse
}

func reverse(xs *types.Bytes, rs *types.Bytes) *types.Bytes {
	var retCursor uint32

	for idx, offset := range xs.Offsets {
		cursor := offset
		curLen := xs.Lengths[idx]

		// handle with unicode
		unicodes := []rune(string(xs.Data[cursor : cursor+curLen]))
		for i, j := 0, len(unicodes)-1; i < j; i, j = i+1, j-1 {
			unicodes[i], unicodes[j] = unicodes[j], unicodes[i]
		}

		for i, b := range []byte(string(unicodes)) {
			rs.Data[retCursor+uint32(i)] = b
		}

		retCursor += curLen
		rs.Lengths[idx] = xs.Lengths[idx]
		rs.Offsets[idx] = xs.Offsets[idx]
	}

	return rs
}

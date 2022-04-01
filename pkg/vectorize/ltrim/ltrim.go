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

package ltrim

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	ltrimChar    func(*types.Bytes, *types.Bytes) *types.Bytes
	ltrimVarChar func(*types.Bytes, *types.Bytes) *types.Bytes
)

func init() {
	ltrimChar = Ltrim
	ltrimVarChar = Ltrim
}

func CountSpacesFromLeft(xs *types.Bytes) int32 {
	var (
		spaceCount int32
	)

	for i, offset := range xs.Offsets {
		cursor := offset
		for ; cursor < offset+xs.Lengths[i] && xs.Data[cursor] == ' '; cursor++ {
			spaceCount++
		}
	}

	return spaceCount
}

func Ltrim(xs *types.Bytes, rs *types.Bytes) *types.Bytes {
	var resultCursor uint32

	for i, offset := range xs.Offsets {
		cursor := offset
		// ignore the leading spaces
		for ; cursor < offset+xs.Lengths[i] && xs.Data[cursor] == ' '; cursor++ {
			continue
		}

		// copy the non-space characters
		length := xs.Lengths[i] - (cursor - offset)
		copy(rs.Data[resultCursor:resultCursor+length], xs.Data[cursor:cursor+length])
		rs.Lengths[i] = length
		rs.Offsets[i] = resultCursor
		resultCursor += length
	}

	return rs
}

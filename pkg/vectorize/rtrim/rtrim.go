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

package rtrim

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	RtrimChar    func(*types.Bytes, *types.Bytes) *types.Bytes
	RtrimVarChar func(*types.Bytes, *types.Bytes) *types.Bytes
)

func init() {
	RtrimChar = rtrim
	RtrimVarChar = rtrim
}

func CountSpacesFromRight(xs *types.Bytes) int32 {
	var (
		spaceCount int32
	)

	for i, offset := range xs.Offsets {
		if xs.Lengths[i] == 0 {
			continue
		}

		cursor := offset + xs.Lengths[i] - 1
		//cursor >= offset >=0
		for ; cursor > offset && xs.Data[cursor] == ' '; cursor-- {
			spaceCount++
		}

		if cursor == offset && xs.Data[cursor] == ' ' {
			spaceCount++
		}
	}

	return spaceCount
}

func rtrim(xs *types.Bytes, rs *types.Bytes) *types.Bytes {
	var resultCursor uint32 = 0

	for i, offset := range xs.Offsets {
		if xs.Lengths[i] == 0 {
			rs.Lengths[i] = 0
			rs.Offsets[i] = resultCursor

			continue
		}

		cursor := offset + xs.Lengths[i] - 1
		//cursor >= offset >=0
		// ignore the tailing spaces
		for ; cursor > offset && xs.Data[cursor] == ' '; cursor-- {
			continue
		}

		length := uint32(0)
		//cursor == offset :all spaces
		if cursor > offset {
			// copy the non-space characters
			length = cursor - offset + 1
			copy(rs.Data[resultCursor:resultCursor+length], xs.Data[offset:offset+length])
		}

		rs.Lengths[i] = length
		rs.Offsets[i] = resultCursor
		resultCursor += length
	}

	return rs
}

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

package concat_ws

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type ConcatWsResult struct {
	OutputBytes *types.Bytes
	OutputNsp   *nulls.Nulls
}

func ConcatWs(inputBytes []*types.Bytes, inputNsps []*nulls.Nulls, separator []byte, length int, resultValues *types.Bytes) ConcatWsResult {
	resultNsp := new(nulls.Nulls)
	lengthOfSeparator := uint32(len(separator))
	numOfColumns := len(inputBytes)

	offsetsAll := uint32(0)
	for i := 0; i < length; i++ {
		offsetThisResultRow := uint32(0)
		allNull := true
		for j := 0; j < numOfColumns; j++ {
			if nulls.Contains(inputNsps[j], uint64(i)) {
				continue
			}
			allNull = false
			offset := inputBytes[j].Offsets[i]
			lengthOfThis := inputBytes[j].Lengths[i]
			bytes := inputBytes[j].Data[offset : offset+lengthOfThis]
			resultValues.Data = append(resultValues.Data, bytes...)
			offsetThisResultRow += lengthOfThis
			for k := j + 1; k < numOfColumns; k++ {
				if !nulls.Contains(inputNsps[k], uint64(i)) {
					resultValues.Data = append(resultValues.Data, separator...)
					offsetThisResultRow += lengthOfSeparator
					break
				}
			}
		}

		if allNull {
			nulls.Add(resultNsp, uint64(i))
			resultValues.Offsets[i] = offsetsAll
			resultValues.Lengths[i] = offsetThisResultRow
		} else {
			resultValues.Offsets[i] = offsetsAll
			resultValues.Lengths[i] = offsetThisResultRow
			offsetsAll += offsetThisResultRow
		}
	}
	return ConcatWsResult{resultValues, resultNsp}
}

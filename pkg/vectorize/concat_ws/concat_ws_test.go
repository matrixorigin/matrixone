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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConcatWs(t *testing.T) {
	inputBytes := make([]*types.Bytes, 2)
	inputNsps := make([]*nulls.Nulls, 2)
	inputBytes[0] = &types.Bytes{Data: []byte("hellogutenguten"),
		Lengths: []uint32{5, 0, 5, 5},
		Offsets: []uint32{0, 5, 5, 10},
	}
	inputBytes[1] = &types.Bytes{Data: []byte("hellogutenguten"),
		Lengths: []uint32{5, 0, 5, 5},
		Offsets: []uint32{0, 5, 5, 10},
	}
	inputNsps[0] = new(nulls.Nulls)
	inputNsps[1] = new(nulls.Nulls)
	nulls.Add(inputNsps[0], uint64(1))
	nulls.Add(inputNsps[1], uint64(1))
	resultValues := types.Bytes{
		Data:    make([]byte, 0),
		Lengths: make([]uint32, 4),
		Offsets: make([]uint32, 4),
	}
	ConcatWs(inputBytes, inputNsps, []byte("---"), 4, &resultValues)
	require.Equal(t, []byte("hello---helloguten---gutenguten---guten"), resultValues.Data)
	require.Equal(t, []uint32{0, 13, 13, 26}, resultValues.Offsets)
	require.Equal(t, []uint32{13, 0, 13, 13}, resultValues.Lengths)
}

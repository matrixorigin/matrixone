// Copyright 2021 - 2024 Matrix Origin
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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestGroupConcatVecBlockText(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		f, _ := oid.BlockScaledFormat()
		cell, err := types.AppendBlockScaled(nil, f, []float32{1, -3, 0, 6})
		require.NoError(t, err)
		out, err := appendGroupConcatData(nil, types.New(oid, 4, 0), cell)
		require.NoError(t, err)
		require.Equal(t, "[1, -3, 0, 6]", string(out))

		_, err = appendGroupConcatData(nil, types.New(oid, 4, 0), []byte{0x7f})
		require.Error(t, err)
	}
}

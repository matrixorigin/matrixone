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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJSONCheckedDecodeBeforeTrustedComparison(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "valid"
		if corrupt {
			name = "invalid later child"
		}
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			encode := func(s string) []byte {
				v, err := bytejson.ParseFromString(s)
				require.NoError(t, err)
				b, err := v.Marshal()
				require.NoError(t, err)
				return b
			}
			source, err := vector.NewConstBytes(types.T_json.ToType(), encode(`[0,0]`), 1, proc.Mp())
			require.NoError(t, err)
			defer source.Free(proc.Mp())
			if corrupt {
				source.GetBytesAt(0)[1+8+5] = 0xfd
			}
			wire, err := source.MarshalBinary()
			require.NoError(t, err)
			left := vector.NewVec(types.T_json.ToType())
			defer left.Free(proc.Mp())
			err = left.UnmarshalBinary(wire)
			if corrupt {
				require.Error(t, err)
				require.Zero(t, left.Length())
				return
			}
			require.NoError(t, err)
			right, err := vector.NewConstBytes(types.T_json.ToType(), encode(`[1,0]`), 1, proc.Mp())
			require.NoError(t, err)
			defer right.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp()).(*vector.FunctionResult[bool])
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(1))
			require.NoError(t, lessThanFn([]*vector.Vector{left, right}, result, proc, 1, nil))
			require.True(t, vector.MustFixedColNoTypeCheck[bool](result.GetResultVector())[0])
		})
	}
}

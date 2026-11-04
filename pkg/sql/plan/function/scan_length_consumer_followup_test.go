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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestScanFollowupLengthConsumer(t *testing.T) {
	proc := testutil.NewProcess(t)
	payload := []byte(strings.Repeat("a", 32) + "中\x00")
	input := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{payload, payload, {}, payload}, []types.RuntimeStringDomain{types.RuntimeStringInherit, types.RuntimeStringBinary, types.RuntimeStringInherit, types.RuntimeStringInherit})
	defer input.Free(proc.Mp())
	input.SetNull(3)
	for _, typ := range []types.T{types.T_uint64, types.T_int64} {
		func() {
			result := vector.NewFunctionResultWrapper(typ.ToType(), proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(input.Length()))
			require.NoError(t, LengthUTF8([]*vector.Vector{input}, result, proc, input.Length(), nil))
			output := result.GetResultVector()
			require.Equal(t, typ, output.GetType().Oid)
			require.True(t, output.IsNull(3))
			if typ == types.T_uint64 {
				require.Equal(t, []uint64{34, 36, 0}, vector.MustFixedColNoTypeCheck[uint64](output)[:3])
			} else {
				require.Equal(t, []int64{34, 36, 0}, vector.MustFixedColNoTypeCheck[int64](output)[:3])
			}
		}()
	}
}

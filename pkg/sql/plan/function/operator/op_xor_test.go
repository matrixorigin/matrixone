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

package operator

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestOpXorGeneral(t *testing.T) {
	testCases := []arg{
		{
			info: "op_xor(1, 2)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(1),
				testutil.MakeScalarInt64(1, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(1, 1),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			{
				inputTypes := make([]types.T, len(tc.vs))
				for i := range inputTypes {
					inputTypes[i] = tc.vs[i].Typ.Oid
				}
				b := CoalesceTypeCheckFn(inputTypes, nil, types.T_int64)
				if !tc.match {
					require.False(t, b, fmt.Sprintf("case '%s' shouldn't meet the function's requirement but it meets.", tc.info))
					return
				}
				require.True(t, b)
			}

			got, ergot := coalesceGeneral[int64](tc.vs, tc.proc, types.Type{Oid: types.T_int64})
			if tc.err {
				require.Errorf(t, ergot, fmt.Sprintf("case '%d' expected error, but no error happens", i))
			} else {
				require.NoError(t, ergot)
				require.True(t, testutil.CompareVectors(tc.expect, got), "got vector is different with expected")
			}
		})
	}
}

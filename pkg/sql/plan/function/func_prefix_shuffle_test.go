// Copyright 2026 Matrix Origin
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
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPrefixPredicatesAfterShuffle(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, buffered := range []bool{false, true} {
		for _, name := range []string{"prefix_eq", "prefix_in"} {
			t.Run(fmt.Sprintf("%s/buffered=%v", name, buffered), func(t *testing.T) {
				lhs := vector.NewVec(types.T_varchar.ToType())
				defer lhs.Free(proc.Mp())
				for _, s := range []string{"", "\x00", "\x00xy", "abc", "az", "zebra"} {
					require.NoError(t, vector.AppendBytes(lhs, []byte(s), false, proc.Mp()))
				}
				lhs.SetSorted(true)
				sels := []int64{0, 3, 4, 5, 1, 2}
				if buffered {
					var scratch []byte
					require.NoError(t, lhs.ShuffleWithBuf(sels, proc.Mp(), &scratch))
				} else {
					require.NoError(t, lhs.Shuffle(sels, proc.Mp()))
				}
				rhs := vector.NewVec(types.T_varchar.ToType())
				defer rhs.Free(proc.Mp())
				prefixes := [][]byte{{0}}
				if name == "prefix_in" {
					prefixes = append(prefixes, []byte("zz"))
				}
				for _, p := range prefixes {
					require.NoError(t, vector.AppendBytes(rhs, p, false, proc.Mp()))
				}
				rhs.SetSorted(true)
				result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
				defer result.Free()
				require.NoError(t, result.PreExtendAndReset(lhs.Length()))
				if name == "prefix_eq" {
					require.NoError(t, PrefixEq([]*vector.Vector{lhs, rhs}, result, proc, lhs.Length(), nil))
				} else {
					require.NoError(t, newImplPrefixIn().doPrefixIn([]*vector.Vector{lhs, rhs}, result, proc, lhs.Length(), nil))
				}
				actual := vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())
				for i := range lhs.Length() {
					want := false
					for _, p := range prefixes {
						want = want || bytes.HasPrefix(lhs.GetBytesAt(i), p)
					}
					require.Equal(t, want, actual[i], "row %d value=%q", i, lhs.GetBytesAt(i))
				}
			})
		}
	}
	require.Zero(t, proc.Mp().CurrNB())
}

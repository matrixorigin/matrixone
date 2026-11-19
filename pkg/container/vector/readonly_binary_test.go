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

package vector

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestReadonlyBinarySetterAdmission(t *testing.T) {
	mpool.EnableDebugPoisonOnFree()
	t.Cleanup(mpool.DisableDebugPoisonOnFree)
	for _, leased := range []bool{false, true} {
		for _, binaryString := range []bool{false, true} {
			for _, alias := range []bool{false, true} {
				for failAfter := range 10 {
					t.Run(fmt.Sprintf("leased=%t/binary=%t/alias=%t/fail=%d", leased, binaryString, alias, failAfter), func(t *testing.T) {
						mp := mpool.MustNewZero()
						t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
						controller := &readonlyWriteController{}
						state := readonlyWriteAccount(t, controller)
						values := [][]byte{[]byte("old"), []byte("source"), bytes.Repeat([]byte{'a'}, 64)}
						_, destination := readonlyAliasVectors(t, mp, values, leased, state.selection)
						destination.SetIsBinaryString(!binaryString)
						var snapshot *Vector
						var err error
						if leased {
							snapshot, err = destination.RetainedReadonlyViewWithMP(mp)
						} else {
							snapshot, err = destination.CloneWindowWithAllocation(0, 3, mp, state.selection)
						}
						require.NoError(t, err)
						t.Cleanup(func() { snapshot.Free(mp) })
						want := []byte("replacement")
						input := want
						if alias {
							want = values[1]
							input = destination.GetBytesAt(1)
						}
						if failAfter != 0 {
							controller.failAt = controller.calls + failAfter
						}
						err = SetBytesAtWithBinaryString(destination, 0, input, binaryString, mp)
						if err != nil {
							require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
							require.True(t, controller.rejected)
							for row, value := range values {
								require.Equal(t, value, destination.GetBytesAt(row), "failed admission must not publish payload")
								require.Equal(t, !binaryString, destination.GetIsBinaryStringAt(row))
							}
							controller.failAt = 0
							input = want
							if alias {
								input = destination.GetBytesAt(1)
							}
							require.NoError(t, SetBytesAtWithBinaryString(destination, 0, input, binaryString, mp))
						} else {
							require.False(t, controller.rejected)
						}
						require.Equal(t, 3, destination.Length())
						require.Equal(t, want, destination.GetBytesAt(0))
						require.Equal(t, binaryString, destination.GetIsBinaryStringAt(0))
						for row := 1; row < 3; row++ {
							require.Equal(t, values[row], destination.GetBytesAt(row))
							require.Equal(t, !binaryString, destination.GetIsBinaryStringAt(row))
						}
						for row, value := range values {
							require.Equal(t, value, snapshot.GetBytesAt(row))
							require.Equal(t, !binaryString, snapshot.GetIsBinaryStringAt(row))
						}
					})
				}
			}
		}
	}
}

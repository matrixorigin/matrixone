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

package docfilter

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestChunkedIntegerMembership(t *testing.T) {
	for _, test := range []struct {
		name string
		step uint64
		kind byte
		rows int
	}{
		{"dense", 1, TagCbitmap, 33},
		{"clustered", 0, TagCRoaring, 1024},
		{"sparse", 1 << 40, TagSorted64, 33},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.step == 0 {
				old := CbitmapUseOffset
				CbitmapUseOffset = false
				t.Cleanup(func() { CbitmapUseOffset = old })
			}
			mp := mpool.MustNewZero()
			var chunks [][]byte
			value := func(row int) uint64 {
				if test.step == 0 {
					return uint64(row) + (1 << 40)
				}
				return uint64(row) * test.step
			}
			for chunk := 0; chunk < 3; chunk++ {
				func() {
					v := vector.NewVec(types.T_uint64.ToType())
					defer v.Free(mp)
					for row := 0; row < test.rows; row++ {
						require.NoError(t, vector.AppendFixed(v, value(chunk*test.rows+row), row == 0, mp))
					}
					data, err := v.MarshalBinary()
					require.NoError(t, err)
					chunks = append(chunks, data)
				}()
			}
			keys, err := keycodec.NewKeyPayload(context.Background(), chunks, 0, nil)
			require.NoError(t, err)
			defer keys.Release()
			admission := &recordingMemoryAdmission{grant: true}
			filter, err := NewForCKeys(context.Background(), keys, admission)
			require.NoError(t, err)
			defer filter.Free()
			// All native forms and the serialized Go form must include every
			// chunk in one set, including values beyond the first chunk.
			data, err := BuildKeysWithMemoryAdmission(context.Background(), keys, admission)
			require.NoError(t, err)
			decoded, err := NewWithMemoryAdmission(data, admission)
			require.NoError(t, err)
			defer decoded.Free()
			require.True(t, filter.Exact())
			require.True(t, decoded.Exact())
			require.Equal(t, test.kind, filter.(CFilter).CKind())
			for row := 0; row < 3*test.rows; row++ {
				want := row%test.rows != 0
				key := types.EncodeFixed(value(row))
				require.Equal(t, want, filter.Test(key), "native row %d", row)
				require.Equal(t, want, decoded.Test(key), "serialized row %d", row)
			}
			filter.Free()
			decoded.Free()
			require.ElementsMatch(t, admission.acquired, admission.released)
			require.Zero(t, mp.CurrNB())
		})
	}
}

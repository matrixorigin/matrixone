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

package objectio

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

func TestCopyCachedVectorAllReadonlyIsolatesCache(t *testing.T) {
	for _, oid := range []types.T{types.T_varchar, types.T_array_float32} {
		for _, sealed := range []bool{false, true} {
			name := oid.String() + "/raw"
			if sealed {
				name = oid.String() + "/sealed"
			}
			t.Run(name, func(t *testing.T) {
				writerMP := mpool.MustNewZero()
				source := vector.NewVec(oid.ToType())
				defer func() {
					source.Free(writerMP)
					require.Zero(t, writerMP.CurrNB())
					mpool.DeleteMPool(writerMP)
				}()
				first := bytes.Repeat([]byte{'a'}, 4096)
				second := []byte("inline")
				if oid == types.T_array_float32 {
					values := make([]float32, 768)
					for i := range values {
						values[i] = float32(i+1) / 768
					}
					first = bytes.Clone(types.EncodeSlice(values))
					second = bytes.Clone(first)
					second[0] ^= 1
				}
				require.NoError(t, vector.AppendBytes(source, first, false, writerMP))
				require.NoError(t, vector.AppendBytes(source, second, false, writerMP))
				require.NoError(t, vector.AppendBytes(source, nil, true, writerMP))
				encoded := marshalLegacyTestColumn(t, source)
				source.Free(writerMP)
				require.Zero(t, writerMP.CurrNB())

				var cacheData fscache.Data = fileservice.NewBytes(encoded)
				var probe *validatedVectorBytesProbe
				if sealed {
					validated, err := validateVectorCacheData(cacheData)
					require.NoError(t, err)
					defer validated.Release()
					probe = &validatedVectorBytesProbe{
						backing: validated.(validatedVectorCacheDataMarker).validatedVectorBackingForScope(),
					}
					cacheData = probe
				}
				defer cacheData.Release()
				mp := mpool.MustNewZero()
				one := vector.NewOffHeapVecWithType(oid.ToType())
				two := vector.NewOffHeapVecWithType(oid.ToType())
				owned := vector.NewOffHeapVecWithType(oid.ToType())
				defer func() {
					one.Free(mp)
					two.Free(mp)
					owned.Free(mp)
					require.Zero(t, mp.CurrNB())
					mpool.DeleteMPool(mp)
				}()

				require.NoError(t, CopyCachedVectorAllReadonly(one, cacheData, mp))
				require.True(t, one.HasBorrowedBacking())
				require.Equal(t, first, one.GetBytesAt(0))
				require.Equal(t, second, one.GetBytesAt(1))
				require.True(t, one.GetNulls().Contains(2))
				require.Error(t, CopyCachedVectorAllReadonly(one, cacheData, mp), "readonly publication is not append")
				require.NoError(t, one.MaterializeOwned(mp))
				one.GetBytesAt(0)[0] ^= 1
				require.NoError(t, CopyCachedVectorAllReadonly(two, cacheData, mp))
				require.Equal(t, first, two.GetBytesAt(0), "another query must see unchanged cached values")
				require.NoError(t, CopyCachedVectorAll(owned, cacheData, mp))
				require.NoError(t, CopyCachedVectorAll(owned, cacheData, mp))
				require.Equal(t, 6, owned.Length(), "existing owned append semantics must stay unchanged")
				require.False(t, owned.HasBorrowedBacking())
				if probe != nil {
					require.Zero(t, probe.bytesCalls.Load(), "materialize directly inside the sealed scope")
				}
			})
		}
	}
}

func TestCopyCachedVectorAllReadonlySmallControls(t *testing.T) {
	for _, shape := range []string{"inline", "null", "empty", "fixed", "constant"} {
		t.Run(shape, func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := types.T_varchar.ToType()
			if shape == "fixed" {
				typ = types.T_int64.ToType()
			}
			source := vector.NewVec(typ)
			var destination *vector.Vector
			defer func() {
				destination.Free(mp)
				source.Free(mp)
				require.Zero(t, mp.CurrNB())
				mpool.DeleteMPool(mp)
			}()
			switch shape {
			case "inline":
				require.NoError(t, vector.AppendBytes(source, []byte("short"), false, mp))
			case "null":
				require.NoError(t, vector.AppendBytes(source, nil, true, mp))
			case "fixed":
				require.NoError(t, vector.AppendFixed(source, int64(42), false, mp))
			case "constant":
				var err error
				source, err = vector.NewConstBytes(typ, bytes.Repeat([]byte{'x'}, 64), 4, mp)
				require.NoError(t, err)
			}
			cacheData := fileservice.NewBytes(marshalLegacyTestColumn(t, source))
			defer cacheData.Release()
			destination = vector.NewOffHeapVecWithType(typ)
			require.NoError(t, CopyCachedVectorAllReadonly(destination, cacheData, mp))
			require.Equal(t, source.Length(), destination.Length())
			require.Equal(t, shape == "constant", destination.HasBorrowedBacking())
			if shape == "constant" {
				for row := 0; row < 4; row++ {
					require.Equal(t, bytes.Repeat([]byte{'x'}, 64), destination.GetBytesAt(row))
				}
			}
		})
	}
}

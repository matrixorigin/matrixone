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
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"
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
				source := vector.NewVec(oid.ToType(), writerMP)
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
				one := vector.NewVec(oid.ToType(), mp)
				two := vector.NewVec(oid.ToType(), mp)
				owned := vector.NewVec(oid.ToType(), mp)
				defer func() {
					one.Free(mp)
					two.Free(mp)
					owned.Free(mp)
					require.Zero(t, mp.CurrNB())
					mpool.DeleteMPool(mp)
				}()

				require.NoError(t, CopyCachedVectorAllReadonly(one, cacheData, mp))
				require.False(t, one.IsReadOnly(), "the readonly loader publishes an owned private copy")
				require.Equal(t, first, one.GetBytesAt(0))
				require.Equal(t, second, one.GetBytesAt(1))
				require.True(t, one.GetNulls().Contains(2))
				require.Error(t, CopyCachedVectorAllReadonly(one, cacheData, mp), "readonly publication is not append")
				one.GetBytesAt(0)[0] ^= 1
				require.NoError(t, CopyCachedVectorAllReadonly(two, cacheData, mp))
				require.Equal(t, first, two.GetBytesAt(0), "another query must see unchanged cached values")
				require.NoError(t, CopyCachedVectorAll(owned, cacheData, mp))
				require.NoError(t, CopyCachedVectorAll(owned, cacheData, mp))
				require.Equal(t, 6, owned.Length(), "existing owned append semantics must stay unchanged")
				require.False(t, owned.IsReadOnly())
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
			source := vector.NewVec(typ, mp)
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
			destination = vector.NewVec(typ, mp)
			require.NoError(t, CopyCachedVectorAllReadonly(destination, cacheData, mp))
			require.Equal(t, source.Length(), destination.Length())
			require.False(t, destination.IsReadOnly())
			if shape == "constant" {
				for row := 0; row < 4; row++ {
					require.Equal(t, bytes.Repeat([]byte{'x'}, 64), destination.GetBytesAt(row))
				}
			}
		})
	}
}

type retainedDictionaryCacheProbe struct {
	*validatedVectorBytesProbe
	refs     atomic.Int32
	released atomic.Int32
}

func (d *retainedDictionaryCacheProbe) Retain() { d.refs.Add(1) }
func (d *retainedDictionaryCacheProbe) Release() {
	if d.refs.Add(-1) == 0 {
		d.released.Add(1)
	}
}

func TestRetainedCachedVectorLifetimeAndCow(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := vector.NewVec(types.T_varchar.ToType(), mp)
	t.Cleanup(func() { source.Free(mp) })
	long := bytes.Repeat([]byte{'x'}, 64)
	require.NoError(t, vector.AppendBytes(source, long, false, mp))
	require.NoError(t, vector.AppendBytes(source, []byte("small"), false, mp))
	encoded := marshalLegacyTestColumn(t, source)
	data, err := validateVectorCacheData(fileservice.NewBytes(encoded))
	require.NoError(t, err)
	t.Cleanup(data.Release)
	probe := &retainedDictionaryCacheProbe{validatedVectorBytesProbe: &validatedVectorBytesProbe{backing: data.(validatedVectorCacheDataMarker).validatedVectorBackingForScope()}}
	probe.refs.Store(1)
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	allocation, err := mp.WithAccount(account, mpool.AllocationOwnerSelection, 1, mpool.AllocationCapacityClassDefault)
	require.NoError(t, err)
	destination := vector.NewVec(types.T_varchar.ToType(), mp)
	t.Cleanup(func() { destination.Free(mp) })
	// The lender (normally the output batch) holds the pin, not the Vector.
	var keeper bufferlease.Holder
	t.Cleanup(keeper.Release)
	admitted, err := TryBorrowCachedVector(destination, probe, allocation, &keeper)
	require.NoError(t, err)
	require.True(t, admitted)
	require.Equal(t, 1, keeper.Len())
	require.True(t, destination.IsReadOnly())
	require.Zero(t, probe.bytesCalls.Load())
	require.GreaterOrEqual(t, account.Snapshot().Used, uint64(probe.Capacity()), "the full pinned capacity is charged while held")
	visible := destination.GetBytesAt(0)
	position := bytes.Index(probe.backing, long)
	require.GreaterOrEqual(t, position, 0)
	require.Same(t, &probe.backing[position], &visible[0], "payload must remain cache-backed")
	probe.Release()
	require.Zero(t, probe.released.Load())
	require.Equal(t, long, destination.GetBytesAt(0))
	before := bytes.Clone(probe.backing)
	// A read-only vector is copied before it is changed.
	owned, err := destination.Dup(mp)
	require.NoError(t, err)
	defer owned.Free(mp)
	require.NoError(t, vector.SetBytesAt(owned, 0, []byte("changed"), mp))
	require.Equal(t, before, probe.backing)
	require.Zero(t, probe.released.Load(), "copying does not release the lender's pin")
	keeper.Release()
	require.Equal(t, int32(1), probe.released.Load())
	require.Zero(t, account.Snapshot().Used, "releasing the holder returns the pin reservation")
}

func TestRetainedCachedVectorPinRefusal(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := vector.NewVec(types.T_varchar.ToType(), mp)
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, vector.AppendBytes(source, bytes.Repeat([]byte{'p'}, 128), false, mp))
	data, err := validateVectorCacheData(fileservice.NewBytes(marshalLegacyTestColumn(t, source)))
	require.NoError(t, err)
	t.Cleanup(data.Release)
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.Open(16)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	allocation, err := mp.WithAccount(account, mpool.AllocationOwnerSelection, 1, mpool.AllocationCapacityClassDefault)
	require.NoError(t, err)
	for _, transient := range []bool{false, true} {
		func() {
			allocMP := allocation
			var transientBytes uint64
			if transient {
				var err error
				allocMP, err = mp.WithAdmission(func(size uint64) (func(), error) {
					transientBytes += size
					return func() { transientBytes -= size }, nil
				})
				require.NoError(t, err)
				defer func() { require.Zero(t, transientBytes) }()
			}
			destination := vector.NewVec(types.T_varchar.ToType(), allocMP)
			defer destination.Free(allocMP)
			var keeper bufferlease.Holder
			admitted, err := TryBorrowCachedVector(destination, data, allocMP, &keeper)
			require.NoError(t, err)
			require.False(t, admitted)
			require.Zero(t, keeper.Len(), "a refused pin is not held")
			require.Zero(t, destination.Length())

			if transient {
				require.NoError(t, CopyCachedVectorAll(destination, data, allocMP))
				require.Equal(t, source.GetBytesAt(0), destination.GetBytesAt(0))
				require.False(t, destination.IsReadOnly())
				require.Positive(t, transientBytes)
			}
		}()
	}
}

func TestCachedColumnScopePinsOnlyForCallback(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := vector.NewVec(types.T_varchar.ToType(), mp)
	t.Cleanup(func() { source.Free(mp) })
	payload := bytes.Repeat([]byte{'q'}, 64)
	require.NoError(t, vector.AppendBytes(source, payload, false, mp))
	data, err := validateVectorCacheData(fileservice.NewBytes(marshalLegacyTestColumn(t, source)))
	require.NoError(t, err)
	t.Cleanup(data.Release)
	probe := &retainedDictionaryCacheProbe{validatedVectorBytesProbe: &validatedVectorBytesProbe{backing: data.(validatedVectorCacheDataMarker).validatedVectorBackingForScope()}}
	probe.refs.Store(1)
	output := vector.NewVec(types.T_varchar.ToType(), mp)
	t.Cleanup(func() { output.Free(mp) })
	scope := &CachedColumnScope{}
	entries := []fileservice.IOEntry{{CachedData: probe}}
	failure := moerr.NewInvalidInputNoCtx("injected scoped consumer failure")
	err = scope.With(entries, []*vector.Vector{output}, func(inputs []*vector.Vector, marked []bool) error {
		require.True(t, marked[0])
		require.True(t, inputs[0].IsReadOnly())
		require.Equal(t, int32(1), probe.refs.Load(), "the existing pinned read owns cache lifetime")
		position := bytes.Index(probe.backing, payload)
		require.GreaterOrEqual(t, position, 0)
		require.Same(t, &probe.backing[position], &inputs[0].GetBytesAt(0)[0])
		return failure
	})
	require.ErrorIs(t, err, failure)
	require.Nil(t, scope.inputs[0])
	require.Zero(t, scope.views[0].Length())
	require.False(t, scope.marked[0])
	header := &scope.views[0]
	err = scope.With(entries, []*vector.Vector{output}, func(inputs []*vector.Vector, marked []bool) error {
		require.True(t, marked[0])
		require.Same(t, header, inputs[0])
		return output.Union(inputs[0], []int64{0}, mp)
	})
	require.NoError(t, err)
	require.Zero(t, probe.bytesCalls.Load())
	probe.Release()
	require.Equal(t, int32(1), probe.released.Load())
	require.Equal(t, payload, output.GetBytesAt(0), "owned output survives the end of its cache pin")
	require.False(t, output.IsReadOnly())
}

func TestCachedColumnScopeFixedValidationAndLifetime(t *testing.T) {
	mp := mpool.MustNewZero()
	source := vector.NewVec(types.T_int64.ToType(), mp)
	output := vector.NewVec(types.T_int64.ToType(), mp)
	t.Cleanup(func() { source.Free(mp); output.Free(mp); require.Zero(t, mp.CurrNB()) })
	require.NoError(t, vector.AppendFixedList(source, []int64{7, 8, 9}, []bool{false, true, false}, mp))
	encoded := marshalLegacyTestColumn(t, source)
	original := bytes.Clone(encoded)
	data := fileservice.NewBytes(encoded)
	t.Cleanup(data.Release)
	entries := []fileservice.IOEntry{{CachedData: data}}
	scope := &CachedColumnScope{}
	require.NoError(t, scope.With(entries, []*vector.Vector{output}, func(inputs []*vector.Vector, marked []bool) error {
		require.False(t, marked[0], "ordinary reader policy is unchanged")
		require.Same(t, output, inputs[0])
		return nil
	}))
	failure := moerr.NewInvalidInputNoCtx("fixed scoped consumer failure")
	err := scope.WithFixed(entries, []*vector.Vector{output}, func(inputs []*vector.Vector, marked []bool) error {
		require.True(t, marked[0])
		require.True(t, inputs[0].IsReadOnly())
		require.True(t, inputs[0].IsNull(1))
		require.NoError(t, output.Union(inputs[0], []int64{0, 2}, mp))
		return failure
	})
	require.ErrorIs(t, err, failure)
	require.Nil(t, scope.inputs[0])
	require.Zero(t, scope.views[0].Length())
	require.False(t, scope.marked[0])
	require.Equal(t, []int64{7, 9}, vector.MustFixedColNoTypeCheck[int64](output))
	require.False(t, output.IsReadOnly())
	require.Equal(t, original, encoded)
	invalid := fileservice.NewBytes(encoded[:IOEntryHeaderSize+1])
	t.Cleanup(invalid.Release)
	require.Error(t, scope.WithFixed([]fileservice.IOEntry{{CachedData: invalid}}, []*vector.Vector{output}, func([]*vector.Vector, []bool) error {
		t.Fatal("untrusted fixed data must be fully validated before callback")
		return nil
	}))
}

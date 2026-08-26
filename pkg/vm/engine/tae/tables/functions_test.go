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

package tables

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func TestCommitTSLoaderLoadsOnceAndReleases(t *testing.T) {
	mp := mpool.MustNewZero()
	commitTS := containers.MakeVector(types.T_TS.ToType(), mp)
	commitTS.Append(types.BuildTS(5, 0), false)

	loads := 0
	loader := &commitTSLoader{
		load: func() (containers.Vector, error) {
			loads++
			return commitTS, nil
		},
	}

	for range 2 {
		got, err := loader.get()
		require.NoError(t, err)
		require.Same(t, commitTS, got)
	}
	require.Equal(t, 1, loads)
	loader.close()
	require.Zero(t, mp.CurrNB())
}

func TestCommitTSLoaderCachesError(t *testing.T) {
	loadErr := errors.New("load commit timestamps")
	loads := 0
	loader := &commitTSLoader{
		load: func() (containers.Vector, error) {
			loads++
			return nil, loadErr
		},
	}

	for range 2 {
		got, err := loader.get()
		require.ErrorIs(t, err, loadErr)
		require.Nil(t, got)
	}
	require.Equal(t, 1, loads)
	loader.close()
}

func TestMissingCommitTSFollowsDedupPolicy(t *testing.T) {
	for _, typ := range []types.Type{
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	} {
		for _, policyCase := range []struct {
			name      string
			policy    txnif.DedupPolicy
			duplicate bool
		}{
			{
				name:      "strict",
				policy:    txnif.DedupPolicy_CheckAll,
				duplicate: true,
			},
			{
				name:      "incremental",
				policy:    txnif.DedupPolicy_CheckIncremental,
				duplicate: false,
			},
		} {
			for _, commitTSCase := range []struct {
				name string
				make func() containers.Vector
			}{
				{
					name: "missing vector",
					make: func() containers.Vector {
						return nil
					},
				},
				{
					name: "short vector",
					make: func() containers.Vector {
						return containers.MakeVector(
							types.T_TS.ToType(),
							common.DefaultAllocator,
						)
					},
				},
				{
					name: "null row",
					make: func() containers.Vector {
						return containers.NewConstNullVector(
							types.T_TS.ToType(),
							1,
							common.DefaultAllocator,
						)
					},
				},
			} {
				t.Run(typ.String()+"/"+policyCase.name+"/"+commitTSCase.name, func(t *testing.T) {
					data := containers.MakeVector(typ, common.DefaultAllocator)
					defer data.Close()
					keys := containers.MakeVector(typ, common.DefaultAllocator)
					defer keys.Close()
					if typ.Oid == types.T_int64 {
						data.Append(int64(1), false)
						keys.Append(int64(1), false)
					} else {
						data.Append([]byte("pk"), false)
						keys.Append([]byte("pk"), false)
					}

					rowIDs := containers.MakeVector(
						types.T_Rowid.ToType(),
						common.DefaultAllocator,
					)
					defer rowIDs.Close()
					rowIDs.Append(nil, true)

					commitTS := commitTSCase.make()
					loader := &commitTSLoader{
						load: func() (containers.Vector, error) {
							return commitTS, nil
						},
					}
					defer loader.close()

					txn := txnbase.MockTxnReaderWithStartTS(types.BuildTS(10, 0))
					txn.SetDedupType(policyCase.policy)
					op := containers.MakeForeachVectorOp(
						keys.GetType().Oid,
						getRowIDAlkFunctions,
						data,
						rowIDs,
						types.Blockid{},
						loader,
						txn,
						types.BuildTS(1, 0),
						types.BuildTS(9, 0),
					)
					require.NoError(t, containers.ForeachVector(keys, op, nil))
					require.Equal(t, policyCase.duplicate, !rowIDs.IsNull(0))
				})
			}
		}
	}
}

func TestCommitTSAtDedupLowerBoundIsIncluded(t *testing.T) {
	from := types.BuildTS(5, 1)
	to := types.BuildTS(9, 0)
	txn := txnbase.MockTxnReaderWithStartTS(from.Prev())

	for _, typ := range []types.Type{
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	} {
		t.Run(typ.String(), func(t *testing.T) {
			data := containers.MakeVector(typ, common.DefaultAllocator)
			defer data.Close()
			keys := containers.MakeVector(typ, common.DefaultAllocator)
			defer keys.Close()
			if typ.Oid == types.T_int64 {
				data.Append(int64(1), false)
				keys.Append(int64(1), false)
			} else {
				data.Append([]byte("pk"), false)
				keys.Append([]byte("pk"), false)
			}

			rowIDs := containers.MakeVector(
				types.T_Rowid.ToType(),
				common.DefaultAllocator,
			)
			defer rowIDs.Close()
			rowIDs.Append(nil, true)

			commitTS := containers.MakeVector(
				types.T_TS.ToType(),
				common.DefaultAllocator,
			)
			commitTS.Append(from, false)
			loader := &commitTSLoader{
				load: func() (containers.Vector, error) {
					return commitTS, nil
				},
			}
			defer loader.close()

			op := containers.MakeForeachVectorOp(
				keys.GetType().Oid,
				getRowIDAlkFunctions,
				data,
				rowIDs,
				types.Blockid{},
				loader,
				txn,
				from,
				to,
			)
			require.ErrorIs(
				t,
				containers.ForeachVector(keys, op, nil),
				txnif.ErrTxnWWConflict,
			)
		})
	}
}

func TestCommitTSIsNotLoadedWithoutExactPKMatch(t *testing.T) {
	for _, typ := range []types.Type{
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	} {
		t.Run(typ.String(), func(t *testing.T) {
			data := containers.MakeVector(typ, common.DefaultAllocator)
			defer data.Close()
			keys := containers.MakeVector(typ, common.DefaultAllocator)
			defer keys.Close()
			if typ.Oid == types.T_int64 {
				data.Append(int64(1), false)
				keys.Append(int64(2), false)
			} else {
				data.Append([]byte("stored"), false)
				keys.Append([]byte("incoming"), false)
			}

			rowIDs := containers.MakeVector(
				types.T_Rowid.ToType(),
				common.DefaultAllocator,
			)
			defer rowIDs.Close()
			rowIDs.Append(nil, true)

			loads := 0
			loader := &commitTSLoader{
				load: func() (containers.Vector, error) {
					loads++
					return nil, errors.New("unexpected commit-TS read")
				},
			}
			defer loader.close()

			txn := txnbase.MockTxnReaderWithStartTS(types.BuildTS(10, 0))
			op := containers.MakeForeachVectorOp(
				keys.GetType().Oid,
				getRowIDAlkFunctions,
				data,
				rowIDs,
				types.Blockid{},
				loader,
				txn,
				types.BuildTS(1, 0),
				types.BuildTS(9, 0),
			)
			require.NoError(t, containers.ForeachVector(keys, op, nil))
			require.Zero(t, loads)
			require.True(t, rowIDs.IsNull(0))
		})
	}
}

func BenchmarkFunctions(b *testing.B) {
	vec := containers.MockVector2(types.T_int64.ToType(), 10000, 0)
	defer vec.Close()
	vec2 := containers.MakeVector(*vec.GetType(), common.DefaultAllocator)
	defer vec2.Close()
	for i := 9999999; i < 9999999+1000; i++ {
		vec2.Append(int64(i), false)
	}

	b.Run("old-dedup-int64", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec2.Foreach(dedupNABlkClosure(vec, nil, nil, nil), nil)
		}
	})
	b.Run("new-dedup-int64", func(b *testing.B) {
		op := containers.MakeForeachVectorOp(vec2.GetType().Oid, getDuplicatedRowIDNABlkFunctions, vec, nil, nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			containers.ForeachVectorWindow(vec2, 0, vec2.Length(), op, nil, nil)
		}
	})

	vec3 := containers.MockVector(types.T_decimal128.ToType(), 20000, true, nil)
	defer vec3.Close()
	vec4 := vec3.CloneWindow(0, 10000)
	defer vec4.Close()
	vec5 := vec3.CloneWindow(11000, 100)
	defer vec5.Close()
	b.Run("old-dedup-d128", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec5.Foreach(dedupNABlkClosure(vec4, nil, nil, nil), nil)
		}
	})
	b.Run("new-dedup-d128", func(b *testing.B) {
		op := containers.MakeForeachVectorOp(vec4.GetType().Oid, getDuplicatedRowIDNABlkFunctions, vec4, nil, nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			containers.ForeachVectorWindow(vec5, 0, vec5.Length(), op, nil, nil)
		}
	})

	vec6 := containers.MockVector2(types.T_varchar.ToType(), 12000, 0)
	defer vec6.Close()
	vec7 := vec6.CloneWindow(0, 10000)
	defer vec7.Close()
	vec8 := vec6.CloneWindow(10500, 10)
	defer vec8.Close()

	b.Run("old-dedup-chars", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec8.Foreach(dedupNABlkClosure(vec7, nil, nil, nil), nil)
		}
	})
	b.Run("new-dedup-chars", func(b *testing.B) {
		op := containers.MakeForeachVectorOp(vec7.GetType().Oid, getDuplicatedRowIDNABlkFunctions, vec7, nil, nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			containers.ForeachVectorWindow(vec8, 0, vec8.Length(), op, nil, nil)
		}
	})

	b.Run("old-dedup-achars", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec8.Foreach(dedupABlkClosureFactory(nil)(vec7, nil, nil, nil), nil)
		}
	})

	b.Run("new-dedup-achars", func(b *testing.B) {
		op := containers.MakeForeachVectorOp(vec7.GetType().Oid, getRowIDAlkFunctions, vec7, nil, nil, nil, nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			containers.ForeachVectorWindow(vec8, 0, vec8.Length(), op, nil, nil)
		}
	})
}

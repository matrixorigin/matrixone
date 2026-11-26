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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	containers.AppendValue(commitTS, types.BuildTS(5, 0), false)

	loads := 0
	loader := &commitTSLoader{
		load: func() (containers.Vector, containers.Vector, error) {
			loads++
			return commitTS, nil, nil
		},
	}

	for range 2 {
		got, abort, err := loader.get()
		require.NoError(t, err)
		require.Same(t, commitTS, got)
		require.Nil(t, abort)
	}
	require.Equal(t, 1, loads)
	loader.close()
	require.Zero(t, mp.CurrNB())
}

func TestCommitTSLoaderCachesError(t *testing.T) {
	loadErr := errors.New("load commit timestamps")
	loads := 0
	loader := &commitTSLoader{
		load: func() (containers.Vector, containers.Vector, error) {
			loads++
			return nil, nil, loadErr
		},
	}

	for range 2 {
		got, abort, err := loader.get()
		require.ErrorIs(t, err, loadErr)
		require.Nil(t, got)
		require.Nil(t, abort)
	}
	require.Equal(t, 1, loads)
	loader.close()
}

func TestPersistedAppendableDedupSkipsAbortedRow(t *testing.T) {
	for _, test := range []struct {
		name     string
		commitTS types.TS
		aborted  bool
	}{
		{
			name:     "v11-abort-column",
			commitTS: types.BuildTS(5, 0),
			aborted:  true,
		},
		{
			name:     "v9-uncommitted-sentinel",
			commitTS: txnif.UncommitTS,
		},
	} {
		for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
			t.Run(test.name+"/"+typ.String(), func(t *testing.T) {
				data := containers.MakeVector(typ, common.DefaultAllocator)
				defer containers.ReleaseVector(data)
				keys := containers.MakeVector(typ, common.DefaultAllocator)
				defer containers.ReleaseVector(keys)
				if typ.Oid == types.T_int64 {
					containers.AppendValue(data, int64(42), false)
					containers.AppendValue(keys, int64(42), false)
				} else {
					containers.AppendValue(data, []byte("pk"), false)
					containers.AppendValue(keys, []byte("pk"), false)
				}
				rowIDs := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
				defer containers.ReleaseVector(rowIDs)
				containers.AppendValue(rowIDs, nil, true)

				commitTS := containers.MakeVector(types.T_TS.ToType(), common.DefaultAllocator)
				containers.AppendValue(commitTS, test.commitTS, false)
				var abort containers.Vector
				if test.aborted {
					abort = containers.MakeVector(types.T_bool.ToType(), common.DefaultAllocator)
					containers.AppendValue(abort, true, false)
				} else {
					abort = containers.NewConstNullVector(
						types.T_bool.ToType(), 1, common.DefaultAllocator,
					)
				}
				loader := &commitTSLoader{
					load: func() (containers.Vector, containers.Vector, error) {
						return commitTS, abort, nil
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
					types.TS{},
					types.MaxTs(),
				)
				require.NoError(t, containers.ForeachVector(keys, op, nil))
				require.True(t, rowIDs.IsNull(0))
			})
		}
	}
}

func TestPersistedTombstoneContainsSkipsAbortedMatches(t *testing.T) {
	var blk types.Blockid
	target := types.NewRowid(&blk, 7)
	txn := txnbase.MockTxnReaderWithStartTS(types.BuildTS(10, 0))

	check := func(commitTSs []types.TS, aborts []bool) bool {
		persisted := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
		defer containers.ReleaseVector(persisted)
		commitTS := containers.MakeVector(types.T_TS.ToType(), common.DefaultAllocator)
		var abortVec containers.Vector
		if aborts == nil {
			abortVec = containers.NewConstNullVector(
				types.T_bool.ToType(),
				len(commitTSs),
				common.DefaultAllocator,
			)
		} else {
			abortVec = containers.MakeVector(types.T_bool.ToType(), common.DefaultAllocator)
		}
		for row, ts := range commitTSs {
			containers.AppendValue(persisted, target, false)
			containers.AppendValue(commitTS, ts, false)
			if aborts != nil {
				containers.AppendValue(abortVec, aborts[row], false)
			}
		}
		keys := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
		defer containers.ReleaseVector(keys)
		containers.AppendValue(keys, target, false)

		op := containers.MakeForeachVectorOp(
			types.T_Rowid,
			containsAlkFunctions,
			persisted,
			keys,
			func(uint16) (containers.Vector, containers.Vector, error) {
				return commitTS, abortVec, nil
			},
			txn,
			func(any, types.TS) (types.TS, error) {
				t.Fatal("old committed tombstone must not invoke WW lookup")
				return types.TS{}, nil
			},
		)
		require.NoError(t, containers.ForeachVector(keys, op, nil))
		return keys.IsNull(0)
	}

	committed := types.BuildTS(5, 0)
	require.True(t, check([]types.TS{committed, committed}, []bool{true, false}), "a live physical match must delete the data row")
	require.False(t, check([]types.TS{committed, committed}, []bool{true, true}), "all-aborted physical matches must not delete the data row")
	require.False(t, check([]types.TS{txnif.UncommitTS}, nil), "a v9 rollback sentinel must not delete the data row")
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
					defer containers.ReleaseVector(data)
					keys := containers.MakeVector(typ, common.DefaultAllocator)
					defer containers.ReleaseVector(keys)
					if typ.Oid == types.T_int64 {
						containers.AppendValue(data, int64(1), false)
						containers.AppendValue(keys, int64(1), false)
					} else {
						containers.AppendValue(data, []byte("pk"), false)
						containers.AppendValue(keys, []byte("pk"), false)
					}

					rowIDs := containers.MakeVector(
						types.T_Rowid.ToType(),
						common.DefaultAllocator,
					)
					defer containers.ReleaseVector(rowIDs)
					containers.AppendValue(rowIDs, nil, true)

					commitTS := commitTSCase.make()
					loader := &commitTSLoader{
						load: func() (containers.Vector, containers.Vector, error) {
							return commitTS, nil, nil
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
			defer containers.ReleaseVector(data)
			keys := containers.MakeVector(typ, common.DefaultAllocator)
			defer containers.ReleaseVector(keys)
			if typ.Oid == types.T_int64 {
				containers.AppendValue(data, int64(1), false)
				containers.AppendValue(keys, int64(1), false)
			} else {
				containers.AppendValue(data, []byte("pk"), false)
				containers.AppendValue(keys, []byte("pk"), false)
			}

			rowIDs := containers.MakeVector(
				types.T_Rowid.ToType(),
				common.DefaultAllocator,
			)
			defer containers.ReleaseVector(rowIDs)
			containers.AppendValue(rowIDs, nil, true)

			commitTS := containers.MakeVector(
				types.T_TS.ToType(),
				common.DefaultAllocator,
			)
			containers.AppendValue(commitTS, from, false)
			loader := &commitTSLoader{
				load: func() (containers.Vector, containers.Vector, error) {
					return commitTS, nil, nil
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
			defer containers.ReleaseVector(data)
			keys := containers.MakeVector(typ, common.DefaultAllocator)
			defer containers.ReleaseVector(keys)
			if typ.Oid == types.T_int64 {
				containers.AppendValue(data, int64(1), false)
				containers.AppendValue(keys, int64(2), false)
			} else {
				containers.AppendValue(data, []byte("stored"), false)
				containers.AppendValue(keys, []byte("incoming"), false)
			}

			rowIDs := containers.MakeVector(
				types.T_Rowid.ToType(),
				common.DefaultAllocator,
			)
			defer containers.ReleaseVector(rowIDs)
			containers.AppendValue(rowIDs, nil, true)

			loads := 0
			loader := &commitTSLoader{
				load: func() (containers.Vector, containers.Vector, error) {
					loads++
					return nil, nil, errors.New("unexpected commit-TS read")
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
	makeRowIDs := func(b *testing.B, length int) containers.Vector {
		b.Helper()
		rowIDs := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
		b.Cleanup(func() { containers.ReleaseVector(rowIDs) })
		for range length {
			containers.AppendValue(rowIDs, nil, true)
		}
		return rowIDs
	}
	checkNoMatches := func(b *testing.B, rowIDs containers.Vector) {
		b.Helper()
		for i := 0; i < rowIDs.Length(); i++ {
			if !rowIDs.IsNull(uint64(i)) {
				b.Fatal("benchmark probes must not match the block")
			}
		}
	}
	vec := containers.MockVector2(types.T_int64.ToType(), 10000, 0)
	defer containers.ReleaseVector(vec)
	vec2 := containers.MakeVector(*vec.GetType(), common.DefaultAllocator)
	defer containers.ReleaseVector(vec2)
	for i := 9999999; i < 9999999+1000; i++ {
		containers.AppendValue(vec2, int64(i), false)
	}

	b.Run("new-dedup-int64", func(b *testing.B) {
		rowIDs := makeRowIDs(b, vec2.Length())
		op := containers.MakeForeachVectorOp(vec2.GetType().Oid, getDuplicatedRowIDNABlkFunctions, vec, rowIDs, types.Blockid{})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := containers.ForeachVectorWindow(vec2, 0, vec2.Length(), op, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		checkNoMatches(b, rowIDs)
	})

	vec3 := containers.MockVector(types.T_decimal128.ToType(), 20000, true, nil)
	defer containers.ReleaseVector(vec3)
	vec4 := containers.CloneWindow(vec3, 0, 10000, vec3.MPool() /* REVIEW: CloneWindow pool */)
	defer containers.ReleaseVector(vec4)
	vec5 := containers.CloneWindow(vec3, 11000, 100, vec3.MPool() /* REVIEW: CloneWindow pool */)
	defer containers.ReleaseVector(vec5)

	b.Run("new-dedup-d128", func(b *testing.B) {
		rowIDs := makeRowIDs(b, vec5.Length())
		op := containers.MakeForeachVectorOp(vec4.GetType().Oid, getDuplicatedRowIDNABlkFunctions, vec4, rowIDs, types.Blockid{})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := containers.ForeachVectorWindow(vec5, 0, vec5.Length(), op, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		checkNoMatches(b, rowIDs)
	})

	vec6 := containers.MockVector2(types.T_varchar.ToType(), 12000, 0)
	defer containers.ReleaseVector(vec6)
	vec7 := containers.CloneWindow(vec6, 0, 10000, vec6.MPool() /* REVIEW: CloneWindow pool */)
	defer containers.ReleaseVector(vec7)
	vec8 := containers.CloneWindow(vec6, 10500, 10, vec6.MPool() /* REVIEW: CloneWindow pool */)
	defer containers.ReleaseVector(vec8)

	b.Run("new-dedup-chars", func(b *testing.B) {
		rowIDs := makeRowIDs(b, vec8.Length())
		op := containers.MakeForeachVectorOp(vec7.GetType().Oid, getDuplicatedRowIDNABlkFunctions, vec7, rowIDs, types.Blockid{})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := containers.ForeachVectorWindow(vec8, 0, vec8.Length(), op, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		checkNoMatches(b, rowIDs)
	})

	b.Run("new-dedup-achars", func(b *testing.B) {
		rowIDs := makeRowIDs(b, vec8.Length())
		loader := &commitTSLoader{load: func() (containers.Vector, containers.Vector, error) {
			return nil, nil, errors.New("benchmark probes must not match the block")
		}}
		b.Cleanup(loader.close)
		txn := txnbase.MockTxnReaderWithStartTS(types.BuildTS(10, 0))
		op := containers.MakeForeachVectorOp(vec7.GetType().Oid, getRowIDAlkFunctions,
			vec7, rowIDs, types.Blockid{}, loader, txn, types.TS{}, types.MaxTs())
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := containers.ForeachVectorWindow(vec8, 0, vec8.Length(), op, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		checkNoMatches(b, rowIDs)
	})
}

func TestPersistedContainsReturnsTombstoneBitmapCapacityError(t *testing.T) {
	for _, appendable := range []bool{false, true} {
		name := "non-appendable"
		if appendable {
			name = "appendable"
		}
		t.Run(name, func(t *testing.T) {
			const capacity = 1 << 20
			pool, err := mpool.NewMPool(t.Name(), capacity, mpool.NoFixed)
			require.NoError(t, err)
			defer mpool.DeleteMPool(pool)

			typ := types.T_Rowid.ToType()
			var blk types.Blockid
			rowID := types.NewRowid(&blk, 7)
			persisted := containers.MakeVector(typ, common.DefaultAllocator)
			defer containers.ReleaseVector(persisted)
			containers.AppendValue(persisted, rowID, false)
			keys := containers.MakeVector(typ, pool)
			defer containers.ReleaseVector(keys)
			containers.AppendValue(keys, rowID, false)

			op := containers.MakeForeachVectorOp(typ.Oid, containsNABlkFunctions, persisted, keys)
			if appendable {
				txn := txnbase.MockTxnReaderWithStartTS(types.BuildTS(10, 0))
				op = containers.MakeForeachVectorOp(
					typ.Oid, containsAlkFunctions, persisted, keys,
					func(uint16) (containers.Vector, containers.Vector, error) {
						commitTS := containers.MakeVector(types.T_TS.ToType(), common.DefaultAllocator)
						containers.AppendValue(commitTS, types.BuildTS(5, 0), false)
						return commitTS, nil, nil
					},
					txn,
					func(any, types.TS) (types.TS, error) {
						t.Fatal("old committed tombstone must not perform a conflict lookup")
						return types.TS{}, nil
					},
				)
			}
			before := pool.CurrNB()
			t.Run("under pressure", func(t *testing.T) {
				filler, err := pool.Alloc(capacity-int(before), true)
				require.NoError(t, err)
				defer pool.Free(filler)
				err = containers.ForeachVector(keys, op, nil)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrMPoolCapacity), "%v", err)
				require.False(t, keys.IsNull(0))
				require.EqualValues(t, capacity, pool.CurrNB())
			})
			require.Equal(t, before, pool.CurrNB())
			require.NoError(t, containers.ForeachVector(keys, op, nil))
			require.True(t, keys.IsNull(0))
		})
	}
}

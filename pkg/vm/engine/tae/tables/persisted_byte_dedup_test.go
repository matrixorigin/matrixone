// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func TestPersistedByteDedupSelectedProbes(t *testing.T) {
	for _, oid := range []types.T{
		types.T_char, types.T_varchar, types.T_blob, types.T_binary, types.T_varbinary,
		types.T_json, types.T_text, types.T_datalink, types.T_array_float32, types.T_array_float64,
	} {
		for _, start := range []int64{10, 20} {
			t.Run(fmt.Sprintf("%s/start=%d", oid, start), func(t *testing.T) {
				rng := rand.New(rand.NewPCG(29, 65))
				keyFor := func(n int) []byte {
					key := []byte(fmt.Sprintf("0123456789abcdef%08dfedcba9876543210", n))
					if oid == types.T_json {
						value, err := types.ParseStringToByteJson(fmt.Sprintf("%q", key))
						require.NoError(t, err)
						key, err = types.EncodeJson(value)
						require.NoError(t, err)
					}
					return key
				}
				data := containers.MakeVector(oid.ToType(), common.DefaultAllocator)
				defer data.Close()
				keys := containers.MakeVector(oid.ToType(), common.DefaultAllocator)
				defer keys.Close()
				rows := containers.MakeVector(objectio.RowidType, common.DefaultAllocator)
				defer rows.Close()
				loader := &commitTSLoader{loaded: true}
				defer loader.close()
				loader.commitTS = containers.MakeVector(types.T_TS.ToType(), common.DefaultAllocator)
				loader.abort = containers.MakeVector(types.T_bool.ToType(), common.DefaultAllocator)
				values := make([][]byte, 65)
				timestamps, aborted := make([]int64, len(values)), make([]bool, len(values))
				for i := range values {
					// Equal edges and length deliberately put distinct values in one bucket.
					values[i] = keyFor(rng.IntN(8))
					timestamps[i], aborted[i] = int64(rng.IntN(16)), rng.IntN(4) == 0
					data.Append(values[i], false)
					loader.commitTS.Append(types.BuildTS(timestamps[i], 0), false)
					loader.abort.Append(aborted[i], false)
				}
				var selected nulls.Bitmap
				want := make([]int, 160)
				var wantErr error
				for i := range want {
					key := keyFor(99)
					if i >= 80 {
						key = values[rng.IntN(len(values))]
					}
					keys.Append(key, false)
					want[i] = -1
					if i%11 == 0 {
						rows.Append(objectio.NewRowid(&types.Blockid{}, 999), false)
						want[i] = 999
					} else {
						rows.Append(nil, true)
					}
					if i%3 == 0 {
						continue
					}
					selected.Add(uint64(i))
					if want[i] >= 0 || wantErr != nil {
						continue
					}
					// Scalar oracle: last visible matching row wins; the first conflict
					// stops the whole selected batch without modifying later outputs.
					for j := len(values) - 1; j >= 0; j-- {
						if !bytes.Equal(key, values[j]) || aborted[j] || timestamps[j] < 5 || timestamps[j] > 12 {
							continue
						}
						if timestamps[j] > start {
							wantErr = txnif.ErrTxnWWConflict
						} else {
							want[i] = j
						}
						break
					}
				}
				txn := txnbase.MockTxnReaderWithStartTS(types.BuildTS(start, 0))
				op := containers.MakeForeachVectorOp(oid, getRowIDAlkFunctions, data, rows, types.Blockid{}, loader, txn, types.BuildTS(5, 0), types.BuildTS(12, 0))
				err := containers.ForeachVector(keys, op, &selected)
				if wantErr != nil {
					require.ErrorIs(t, err, wantErr)
				} else {
					require.NoError(t, err)
				}
				for i, row := range want {
					require.Equal(t, row < 0, rows.IsNull(i), "probe %d", i)
					if row >= 0 {
						rowID := rows.Get(i).(types.Rowid)
						require.Equal(t, uint32(row), rowID.GetRowOffset(), "probe %d", i)
					}
				}
			})
		}
	}
}

func TestPersistedByteDedupProbePaths(t *testing.T) {
	readErr := errors.New("read MVCC metadata")
	for _, tc := range []struct {
		name                                                        string
		incremental, abortMissing, conflict, empty, miss, colliding bool
		loadErr                                                     error
		wantRow                                                     int
	}{
		{name: "strict missing timestamp", wantRow: 5},
		{name: "incremental skips missing timestamp", incremental: true, wantRow: 1},
		{name: "eligible before later conflict", abortMissing: true, wantRow: 1},
		{name: "earlier conflict survives", abortMissing: true, conflict: true, wantRow: -1},
		{name: "load failure", loadErr: readErr, wantRow: -1},
		{name: "miss keeps MVCC cold", miss: true, wantRow: -1},
		{name: "empty byte comparison retains reverse row choice", empty: true, wantRow: 6},
		{name: "bucket collision retains exact matching", colliding: true, wantRow: 5},
		{name: "bucket collision miss keeps MVCC cold", colliding: true, miss: true, wantRow: -1},
	} {
		pk, filler := []byte("pk"), []byte("filler")
		if tc.colliding {
			pk = []byte("0123456789abcdefbbbbfedcba9876543210")
			filler = []byte("0123456789abcdefaaaafedcba9876543210")
		}
		for _, warmup := range []int{0, 32} {
			t.Run(fmt.Sprintf("%s/warmup=%d", tc.name, warmup), func(t *testing.T) {
				data := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
				defer data.Close()
				for i := 0; i < 65; i++ {
					key := filler
					if i < 7 {
						key = pk
					}
					if tc.empty && i >= 5 && i < 7 {
						key = []byte{}
					}
					data.Append(key, tc.empty && i == 6)
				}
				keys := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
				defer keys.Close()
				rows := containers.MakeVector(objectio.RowidType, common.DefaultAllocator)
				defer rows.Close()
				for i := 0; i <= warmup; i++ {
					key := []byte("absent")
					if i == warmup && !tc.miss {
						key = pk
						if tc.empty {
							key = []byte{}
						}
					}
					if i == warmup && tc.colliding && tc.miss {
						key = []byte("0123456789abcdefccccfedcba9876543210")
					}
					keys.Append(key, false)
					rows.Append(nil, true)
				}
				loads := 0
				loader := &commitTSLoader{}
				defer loader.close()
				loader.load = func() (containers.Vector, containers.Vector, error) {
					loads++
					if tc.loadErr != nil {
						return nil, nil, tc.loadErr
					}
					loader.commitTS = containers.MakeVector(types.T_TS.ToType(), common.DefaultAllocator)
					loader.abort = containers.MakeVector(types.T_bool.ToType(), common.DefaultAllocator)
					for i := 0; i < data.Length(); i++ {
						ts := types.BuildTS(5, 0)
						switch i {
						case 4:
							ts = txnif.UncommitTS
						case 3:
							ts = types.BuildTS(4, 0)
						case 2:
							ts = types.BuildTS(21, 0)
						case 0:
							ts = types.BuildTS(12, 0)
						}
						loader.commitTS.Append(ts, i == 5 && !tc.empty)
						loader.abort.Append(!tc.empty && (i == 6 || (i == 5 && tc.abortMissing)), false)
					}
					return loader.commitTS, loader.abort, nil
				}
				start := types.BuildTS(10, 0)
				if tc.conflict {
					start = types.BuildTS(4, 0)
				}
				txn := txnbase.MockTxnReaderWithStartTS(start)
				if tc.incremental {
					txn.SetDedupType(txnif.DedupPolicy_CheckIncremental)
				}
				op := containers.MakeForeachVectorOp(types.T_varchar, getRowIDAlkFunctions, data, rows, types.Blockid{}, loader, txn, types.BuildTS(5, 0), types.BuildTS(20, 0))
				err := containers.ForeachVector(keys, op, nil)
				if tc.loadErr != nil {
					require.ErrorIs(t, err, tc.loadErr)
				} else if tc.conflict {
					require.ErrorIs(t, err, txnif.ErrTxnWWConflict)
				} else {
					require.NoError(t, err)
				}
				if tc.miss {
					require.Zero(t, loads)
				} else {
					require.Equal(t, 1, loads)
				}
				for i := 0; i < warmup; i++ {
					require.True(t, rows.IsNull(i))
				}
				if tc.wantRow < 0 {
					require.True(t, rows.IsNull(warmup))
				} else {
					require.False(t, rows.IsNull(warmup))
					rowID := rows.Get(warmup).(types.Rowid)
					require.Equal(t, uint32(tc.wantRow), rowID.GetRowOffset())
					// Later probes must not overwrite a row resolved by an earlier block.
					keys.Update(warmup, filler, false)
					require.NoError(t, containers.ForeachVector(keys, op, nil))
					require.Equal(t, rowID, rows.Get(warmup))
					require.Equal(t, 1, loads)
				}
			})
		}
	}
}

func BenchmarkPersistedByteDedup(b *testing.B) {
	for _, shape := range []struct {
		rows, probes, width int
		miss                bool
	}{
		{1, 1, 0, false}, {32, 32, 0, false}, {65, 33, 0, false},
		{1024, 1024, 0, false}, {4096, 1, 0, false}, {4096, 33, 0, false},
		{65, 33, 32768, false}, {65, 33, 32768, true},
	} {
		b.Run(fmt.Sprintf("rows=%d/probes=%d/bytes=%d/miss=%t", shape.rows, shape.probes, shape.width, shape.miss), func(b *testing.B) {
			data := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
			b.Cleanup(data.Close)
			keys := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
			b.Cleanup(keys.Close)
			rows := containers.MakeVector(objectio.RowidType, common.DefaultAllocator)
			b.Cleanup(rows.Close)
			loader := &commitTSLoader{loaded: true}
			b.Cleanup(loader.close)
			loader.commitTS = containers.MakeVector(types.T_TS.ToType(), common.DefaultAllocator)
			keyFor := func(i int, probe bool) []byte {
				key := append(bytes.Repeat([]byte{'x'}, shape.width), fmt.Sprintf("key-%d", i)...)
				if shape.miss && probe {
					key[0] = 'y'
				}
				return key
			}
			for i := 0; i < shape.rows; i++ {
				data.Append(keyFor(i, false), false)
				loader.commitTS.Append(types.BuildTS(5, 0), false)
			}
			for i := 0; i < shape.probes; i++ {
				keys.Append(keyFor(i, true), false)
				rows.Append(nil, true)
			}
			txn := txnbase.MockTxnReaderWithStartTS(types.BuildTS(10, 0))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				nulls.AddRange(rows.NullMask(), 0, uint64(rows.Length()))
				op := containers.MakeForeachVectorOp(types.T_varchar, getRowIDAlkFunctions, data, rows, types.Blockid{}, loader, txn, types.TS{}, types.MaxTs())
				if err := containers.ForeachVector(keys, op, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestPersistedByteDedupConstantBlock(t *testing.T) {
	for _, nullBlock := range []bool{false, true} {
		t.Run(fmt.Sprintf("null=%t", nullBlock), func(t *testing.T) {
			key := []byte("pk")
			var data containers.Vector
			if nullBlock {
				data = containers.NewConstNullVector(types.T_varchar.ToType(), 65, common.DefaultAllocator)
				key = nil
			} else {
				data = containers.NewConstBytes(types.T_varchar.ToType(), key, 65)
			}
			defer data.Close()
			keys := containers.NewConstBytes(types.T_varchar.ToType(), key, 33)
			defer keys.Close()
			rows := containers.MakeVector(objectio.RowidType, common.DefaultAllocator)
			defer rows.Close()
			for range 33 {
				rows.Append(nil, true)
			}
			loader := &commitTSLoader{loaded: true, commitTS: containers.NewConstFixed(types.T_TS.ToType(), types.BuildTS(5, 0), 65)}
			defer loader.close()
			txn := txnbase.MockTxnReaderWithStartTS(types.BuildTS(10, 0))
			op := containers.MakeForeachVectorOp(types.T_varchar, getRowIDAlkFunctions, data, rows, types.Blockid{}, loader, txn, types.TS{}, types.MaxTs())
			require.NoError(t, containers.ForeachVector(keys, op, nil))
			for i := range rows.Length() {
				require.False(t, rows.IsNull(i))
				rowID := rows.Get(i).(types.Rowid)
				require.Equal(t, uint32(64), rowID.GetRowOffset())
			}
		})
	}
}

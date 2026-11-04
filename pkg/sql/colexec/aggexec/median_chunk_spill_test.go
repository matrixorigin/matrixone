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

package aggexec

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestMedianFlushAcrossResultChunks(t *testing.T) {
	for _, n := range []int{8191, 8192, 8193, 16385} {
		for _, kind := range []string{"numeric", "decimal64", "decimal128"} {
			t.Run(fmt.Sprintf("%s_%d", kind, n), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
				var typ types.Type
				var input *vector.Vector
				switch kind {
				case "numeric":
					typ = types.T_int64.ToType()
					values := make([]int64, n)
					for i := range values {
						values[i] = int64(i + 1)
					}
					input = buildFixedVec(t, mp, typ, values)
				case "decimal64":
					typ = types.New(types.T_decimal64, 18, 0)
					values := make([]types.Decimal64, n)
					for i := range values {
						values[i] = types.Decimal64(i + 1)
					}
					input = buildFixedVec(t, mp, typ, values)
				case "decimal128":
					typ = types.New(types.T_decimal128, 20, 0)
					values := make([]types.Decimal128, n)
					for i := range values {
						values[i] = types.Decimal128{B0_63: uint64(i + 1)}
					}
					input = buildFixedVec(t, mp, typ, values)
				}
				t.Cleanup(func() { input.Free(mp) })
				exec, err := makeMedian(mp, AggIdOfMedian, false, typ)
				require.NoError(t, err)
				t.Cleanup(exec.Free)
				exec.GetOptResult().modifyChunkSize(8192)
				require.NoError(t, exec.GroupGrow(n))
				for i := 0; i < n; i++ {
					if i%17 != 0 {
						require.NoError(t, exec.Fill(i, i, []*vector.Vector{input}))
					}
				}
				output, err := exec.Flush()
				require.NoError(t, err)
				t.Cleanup(func() {
					for _, v := range output {
						v.Free(mp)
					}
				})
				for i := 0; i < n; i++ {
					v, row := output[i/8192], i%8192
					if got, want := v.IsNull(uint64(row)), i%17 == 0; got != want {
						t.Fatalf("group %d null=%v, want %v", i, got, want)
					}
					if i%17 == 0 {
						continue
					}
					if kind == "numeric" {
						if got := vector.GetFixedAtNoTypeCheck[float64](v, row); got != float64(i+1) {
							t.Fatalf("group %d: got %v, want %d", i, got, i+1)
						}
					} else {
						got := vector.GetFixedAtNoTypeCheck[types.Decimal128](v, row).Format(v.GetType().Scale)
						want := fmt.Sprintf("%d.0", i+1)
						if got != want {
							t.Fatalf("group %d: got %s, want %s", i, got, want)
						}
					}
				}
			})
		}
	}
}

func TestRuntimeAggSelectionKeepsResultsAndGroupsAligned(t *testing.T) {
	tests := []struct {
		name   string
		flags  [][]uint8
		values []int64
		empty  []bool
		groups []string
	}{
		{"leading", [][]uint8{nil, {0, 1}, {0, 0}}, []int64{44}, []bool{true}, []string{"g3"}},
		{"middle", [][]uint8{{1, 0}, {}, {0, 1}}, []int64{11, 66}, []bool{false, true}, []string{"g0", "g5"}},
		{"trailing", [][]uint8{{0, 1}, {1, 0}}, []int64{22, 33}, []bool{true, false}, []string{"g1", "g2"}},
		{"partial", [][]uint8{{1}, {0, 1}, nil}, []int64{11, 44}, []bool{false, true}, []string{"g0", "g3"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			ret := initAggResultWithFixedTypeResult[int64](mp, types.T_int64.ToType(), true, 0, false)
			t.Cleanup(ret.free)
			ret.optInformation.chunkSize = 2
			require.NoError(t, ret.grows(6))
			for i := 0; i < 6; i++ {
				ret.values[i/2][i%2] = int64((i + 1) * 11)
				ret.bsFromEmptyList[i/2][i%2] = i%2 != 0
			}
			states := []fakeSerdeGroup{"g0", "g1", "g2", "g3", "g4", "g5"}
			var buf bytes.Buffer
			require.NoError(t, marshalRetAndGroupsToBuffer(int64(len(tc.values)), tc.flags, &buf, &ret.optSplitResult, states, [][]byte{[]byte("extra")}))
			reader := bytes.NewReader(buf.Bytes())
			count, err := types.ReadInt64(reader)
			require.NoError(t, err)
			require.Equal(t, int64(len(tc.values)), count)
			restored := initAggResultWithFixedTypeResult[int64](mp, types.T_int64.ToType(), true, 0, false)
			t.Cleanup(restored.free)
			require.NoError(t, restored.unmarshalFromReader(reader))
			require.Equal(t, tc.values, vector.MustFixedColNoTypeCheck[int64](restored.resultList[0]))
			require.Equal(t, tc.empty, vector.MustFixedColNoTypeCheck[bool](restored.emptyList[0]))
			groupCount, err := types.ReadInt64(reader)
			require.NoError(t, err)
			require.Equal(t, int64(len(tc.groups)), groupCount)
			for _, want := range tc.groups {
				_, data, err := types.ReadSizeBytes(reader)
				require.NoError(t, err)
				require.Equal(t, want, string(data))
			}
			extraCount, err := types.ReadInt64(reader)
			require.NoError(t, err)
			require.Equal(t, int64(1), extraCount)
			_, data, err := types.ReadSizeBytes(reader)
			require.NoError(t, err)
			require.Equal(t, "extra", string(data))
			require.Zero(t, reader.Len())
		})
	}
}

func TestDistinctMarshalToBuffersAllowsPartialSelection(t *testing.T) {
	mp := mpool.MustNewZero()
	distinct := newDistinctHash(mp)
	t.Cleanup(func() { distinct.free(); require.Zero(t, mp.CurrNB()) })
	require.NoError(t, distinct.grows(2))
	var encoded bytes.Buffer
	require.NoError(t, distinct.marshalToBuffers([]uint8{1}, &encoded))

	reader := bytes.NewReader(encoded.Bytes())
	count, err := types.ReadInt64(reader)
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	var expected bytes.Buffer
	_, err = distinct.maps[0].WriteTo(&expected)
	require.NoError(t, err)
	require.Equal(t, expected.Bytes(), encoded.Bytes()[8:])
}

func TestRuntimeAggSelectionRejectsInvalidInput(t *testing.T) {
	mp := mpool.MustNewZero()
	ret := initAggResultWithFixedTypeResult[int64](mp, types.T_int64.ToType(), true, 0, false)
	ret.optInformation.chunkSize = 2
	require.NoError(t, ret.grows(2))
	t.Cleanup(func() { ret.free(); require.Zero(t, mp.CurrNB()) })
	for _, tc := range []struct {
		count int64
		flags [][]uint8
	}{
		{-1, nil}, {1, [][]uint8{{2, 0}}}, {1, [][]uint8{{1, 0, 0}}},
		{1, [][]uint8{{1, 0}, {1}}}, {2, [][]uint8{{1, 0}}},
	} {
		var buf bytes.Buffer
		buf.WriteString("prefix")
		err := marshalRetAndGroupsToBuffer(tc.count, tc.flags, &buf, &ret.optSplitResult, []fakeSerdeGroup{"g0", "g1"}, nil)
		require.Error(t, err)
		require.Equal(t, "prefix", buf.String())
	}
	var zero bytes.Buffer
	require.NoError(t, marshalRetAndGroupsToBuffer[fakeSerdeGroup](0, nil, &zero, &ret.optSplitResult, nil, nil))
	require.Len(t, zero.Bytes(), 8)
	require.Error(t, marshalChunkToBuffer(-1, &zero, &ret.optSplitResult, []fakeSerdeGroup{"g0", "g1"}, nil))
	require.Error(t, marshalChunkToBuffer(1, &zero, &ret.optSplitResult, []fakeSerdeGroup{"g0", "g1"}, nil))
}

func TestMedianSerializationReturnsAllocationErrorAndCanRetry(t *testing.T) {
	for _, tc := range []struct {
		name      string
		freeBelow int64
		minPrefix int
	}{
		{"result", 65536, 0},
		{"empty", 65536 + 4096, 65536},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const limit = int64(1 << 20)
			mp, err := mpool.NewMPool("median-spill-allocation", limit, mpool.NoFixed)
			require.NoError(t, err)
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			exec, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
			require.NoError(t, err)
			t.Cleanup(exec.Free)
			exec.GetOptResult().modifyChunkSize(8192)
			require.NoError(t, exec.GroupGrow(8192))
			flags := make([]uint8, 8192)
			for i := range flags {
				flags[i] = 1
			}
			var ballast [][]byte
			t.Cleanup(func() {
				for _, b := range ballast {
					mp.Free(b)
				}
			})
			for i := 0; i < 300 && limit-mp.CurrNB() >= tc.freeBelow; i++ {
				b, allocErr := mp.Alloc(4096, true)
				require.NoError(t, allocErr)
				ballast = append(ballast, b)
			}
			require.Less(t, limit-mp.CurrNB(), tc.freeBelow)
			if tc.name == "empty" {
				require.GreaterOrEqual(t, limit-mp.CurrNB(), int64(65536))
			}
			var buf bytes.Buffer
			err = exec.SaveIntermediateResult(8192, [][]uint8{flags}, &buf)
			require.Truef(t, moerr.IsMoErrCode(err, moerr.ErrMPoolCapacity), "error: %v", err)
			if tc.minPrefix > 0 {
				require.Greater(t, buf.Len(), tc.minPrefix)
			}
			for _, b := range ballast {
				mp.Free(b)
			}
			ballast = nil
			buf.Reset()
			require.NoError(t, exec.SaveIntermediateResult(8192, [][]uint8{flags}, &buf))
			restored, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
			require.NoError(t, err)
			t.Cleanup(restored.Free)
			require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))
		})
	}
}

func TestMedianDistinctReplacementDecodeResetsHash(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	input := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2})
	t.Cleanup(func() { input.Free(mp) })
	var records [2][]byte
	for row := range records {
		source, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
		require.NoError(t, err)
		require.NoError(t, source.GroupGrow(1))
		require.NoError(t, source.Fill(0, row, []*vector.Vector{input}))
		var buf bytes.Buffer
		require.NoError(t, source.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))
		records[row] = append([]byte(nil), buf.Bytes()...)
		source.Free()
	}
	target, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
	require.NoError(t, err)
	t.Cleanup(target.Free)
	for _, record := range records {
		require.NoError(t, target.UnmarshalFromReader(bytes.NewReader(record), mp))
	}
	require.NoError(t, target.Fill(0, 0, []*vector.Vector{input}))
	require.NoError(t, target.Fill(0, 1, []*vector.Vector{input}))
	output, err := target.Flush()
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, v := range output {
			v.Free(mp)
		}
	})
	require.Equal(t, 1.5, vector.GetFixedAtNoTypeCheck[float64](output[0], 0))
}

func TestAggregateResultFullChunkGrowthReturnsAllocationError(t *testing.T) {
	const limit = int64(1 << 20)
	mp, err := mpool.NewMPool("aggregate-chunk-growth", limit, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	ret := initAggResultWithFixedTypeResult[int64](mp, types.T_int64.ToType(), true, 0, false)
	t.Cleanup(ret.free)
	ret.optInformation.chunkSize = 8192
	require.NoError(t, ret.grows(8192))
	var ballast [][]byte
	t.Cleanup(func() {
		for _, b := range ballast {
			mp.Free(b)
		}
	})
	for i := 0; i < 300 && limit-mp.CurrNB() >= 65536; i++ {
		b, allocErr := mp.Alloc(4096, true)
		require.NoError(t, allocErr)
		ballast = append(ballast, b)
	}
	err = ret.grows(8192)
	require.Truef(t, moerr.IsMoErrCode(err, moerr.ErrMPoolCapacity), "error: %v", err)
	for _, b := range ballast {
		mp.Free(b)
	}
	ballast = nil
	require.NoError(t, ret.grows(8192))
	require.Equal(t, 8192, ret.resultList[0].Length())
	require.Equal(t, 8192, ret.resultList[1].Length())
}

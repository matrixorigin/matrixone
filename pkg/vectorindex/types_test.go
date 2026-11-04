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

package vectorindex

import (
	"math"
	"math/rand"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestValidDistributionMode(t *testing.T) {
	require.True(t, ValidDistributionMode(DistributionMode_SINGLE_GPU_Str))
	require.True(t, ValidDistributionMode(DistributionMode_SHARDED_Str))
	require.True(t, ValidDistributionMode(DistributionMode_REPLICATED_Str))
	require.False(t, ValidDistributionMode("unknown"))
	require.False(t, ValidDistributionMode(""))
}

func TestSimulateDevices(t *testing.T) {
	real := []int{0}
	// n < 2 is a no-op: the real device list is returned unchanged.
	require.Equal(t, real, SimulateDevices(real, 0))
	require.Equal(t, real, SimulateDevices(real, 1))
	multi := []int{0, 1, 2}
	require.Equal(t, multi, SimulateDevices(multi, 1))

	// n >= 2 presents N logical GPUs, all mapped to physical device 0.
	require.Equal(t, []int{0, 0}, SimulateDevices([]int{0}, 2))
	require.Equal(t, []int{0, 0, 0}, SimulateDevices([]int{0}, 3))

	// Even on a real multi-GPU host the simulation forces every logical rank
	// onto device 0 so it stays deterministic on a single-GPU machine.
	require.Equal(t, []int{0, 0}, SimulateDevices([]int{0, 1, 2, 3}, 2))
}

func TestCdc(t *testing.T) {
	key := int64(0)
	v := []float32{0, 1, 2}
	key2 := int64(1)
	v2 := []float32{1, 2, 3}

	cdc := NewVectorIndexCdc[float32](8192)

	// Insert
	cdc.Insert(key, v, nil)

	js, err := cdc.ToJson()
	require.Nil(t, err)

	require.Equal(t, js, `{"cdc":[{"t":"I","pk":0,"v":[0,1,2]}]}`)

	// delete
	cdc.Delete(key)
	js, err = cdc.ToJson()
	require.Nil(t, err)

	require.Equal(t, js, `{"cdc":[{"t":"I","pk":0,"v":[0,1,2]},{"t":"D","pk":0}]}`)

	// upsert
	cdc.Upsert(key2, v2, nil)

	js, err = cdc.ToJson()
	require.Nil(t, err)

	require.Equal(t, js, `{"cdc":[{"t":"I","pk":0,"v":[0,1,2]},{"t":"D","pk":0},{"t":"U","pk":1,"v":[1,2,3]}]}`)

	require.False(t, cdc.Empty())

	require.False(t, cdc.Full())

	cdc.Reset()

	js, err = cdc.ToJson()
	require.NoError(t, err)
	require.Equal(t, js, `{"cdc":[]}`)
}

// A reader reaches this column by exact index_id, so it already knows whether the row is a tail
// frame, and the chunk count selects the rule. What the value must do is CHANGE when the tail
// does -- a checksum that survives a reordered or missing chunk verifies nothing.
func TestCdcChunkSetChecksumIsSelfDescribing(t *testing.T) {
	require.Empty(t, CdcChunkSetChecksum(nil), "nothing to describe")
	require.Empty(t, CdcChunkSetChecksum([]uint32{}))

	one := CdcChunkSetChecksum([]uint32{0xdeadbeef})
	require.Equal(t, "deadbeef", one, "a single chunk is its own footer CRC, comparable directly")

	set := CdcChunkSetChecksum([]uint32{1, 2, 3})
	require.Len(t, set, 8, "several chunks fold to one CRC, same width: %s", set)
	require.NotEqual(t, one, set)

	require.NotEqual(t, set, CdcChunkSetChecksum([]uint32{3, 2, 1}),
		"order is part of what it covers: chunks replayed out of order are not the same tail")
	require.NotEqual(t, set, CdcChunkSetChecksum([]uint32{1, 2, 4}),
		"and a changed chunk must change it")
	require.Equal(t, set, CdcChunkSetChecksum([]uint32{1, 2, 3}), "stable for the same input")
}

// The tail-frame predicate is the boundary between "base generation" and "tail frame": a row on
// the wrong side is excluded from LoadMetadata, kept by DeleteAllBasesSqls, and deleted by
// DeleteTailSqls. It must not be a LIKE pattern -- `_` is LIKE's single-character wildcard, so
// 'cdc_tail:%' also matches cdcXtail:... for any X, and the classification would rest on nothing
// but today's generated id formats.
func TestTailFramePredicatesHaveNoWildcards(t *testing.T) {
	for _, sql := range []string{TailFrameSQL("index_id"), NotTailFrameSQL("index_id")} {
		require.NotContains(t, sql, "LIKE")
		require.NotContains(t, sql, "%")
		require.Contains(t, sql, "index_id")
		// prefix_eq is bytes.HasPrefix against the sentinel itself: no pattern to escape, and
		// no second layer that has to agree about backslashes. It is also what the planner
		// recognizes as a range predicate, so index_id can still be scanned by prefix.
		require.Contains(t, sql, "prefix_eq(index_id, '"+TailFrameMetaPrefix+"')")
	}
	require.Equal(t, "prefix_eq(c, 'cdc_tail:')", TailFrameSQL("c"))
	require.Equal(t, "NOT prefix_eq(c, 'cdc_tail:')", NotTailFrameSQL("c"))

	// The length is the sentinel's, so a row id one character short cannot be compared equal.
	require.Equal(t, len("cdc_tail:"), len(TailFrameMetaPrefix))
	require.Equal(t, TailFrameMetaPrefix, TailFrameMetaId(7)[:len(TailFrameMetaPrefix)])
}

// A float32 CDC batch decodes to the values it encodes, bit for bit: ToJson writes each
// value's shortest float32 text, which a float32 decoder must round once. Through float64,
// 7.038531e-26 (0x15ae43fd) reads as the next float32.
func TestVectorIndexCdcFloat32RoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	vals := []float32{
		math.Float32frombits(0x15ae43fd), math.Float32frombits(0x95ae43fd), 1.0000001,
		math.MaxFloat32, -math.MaxFloat32, math.SmallestNonzeroFloat32, 0, float32(math.Copysign(0, -1)),
	}
	for i := 0; i < 100000; i++ {
		f := math.Float32frombits(rng.Uint32())
		if !math.IsNaN(float64(f)) && !math.IsInf(float64(f), 0) {
			vals = append(vals, f)
		}
	}
	cdc := NewVectorIndexCdc[float32](64)
	for start := 0; start < len(vals); start += 1000 {
		end := min(start+1000, len(vals))
		if start%2000 == 0 {
			cdc.Insert(int64(start), vals[start:end], []byte{1, 2, 3})
		} else {
			cdc.Upsert(int64(start), vals[start:end], nil)
		}
	}
	cdc.Delete(-1)
	text, err := cdc.ToJson()
	require.NoError(t, err)
	check := func(got VectorIndexCdc[float32]) {
		require.Len(t, got.Data, len(cdc.Data))
		for i, e := range cdc.Data {
			g := got.Data[i]
			require.Equal(t, e.Type, g.Type)
			require.Equal(t, e.PKey, g.PKey)
			require.Equal(t, e.IncludeBytes, g.IncludeBytes)
			require.Len(t, g.Vec, len(e.Vec))
			for j := range e.Vec {
				require.Equal(t, math.Float32bits(e.Vec[j]), math.Float32bits(g.Vec[j]), "entry %d value %d (%v)", i, j, e.Vec[j])
			}
		}
	}
	var direct VectorIndexCdc[float32]
	require.NoError(t, direct.UnmarshalJSON([]byte(text)))
	check(direct)
	var viaSonic VectorIndexCdc[float32]
	require.NoError(t, sonic.Unmarshal([]byte(text), &viaSonic))
	check(viaSonic)

	// other element types decode as before
	c64 := NewVectorIndexCdc[float64](2)
	c64.Insert(1, []float64{0.1, -2.5e-300}, nil)
	text, err = c64.ToJson()
	require.NoError(t, err)
	var g64 VectorIndexCdc[float64]
	require.NoError(t, g64.UnmarshalJSON([]byte(text)))
	require.Equal(t, c64.Data, g64.Data)
	cbf := NewVectorIndexCdc[types.BF16](1)
	cbf.Upsert(2, []types.BF16{types.BF16FromFloat32(1.5), types.BF16FromFloat32(-3)}, nil)
	text, err = cbf.ToJson()
	require.NoError(t, err)
	var gbf VectorIndexCdc[types.BF16]
	require.NoError(t, gbf.UnmarshalJSON([]byte(text)))
	require.Equal(t, cbf.Data, gbf.Data)
}

// cdcFloat32 is a named float32 element type, which ArrayElement (~float32) admits.
type cdcFloat32 float32

// A named float32 element type decodes exactly too.
func TestVectorIndexCdcNamedFloat32RoundTrip(t *testing.T) {
	cdc := NewVectorIndexCdc[cdcFloat32](1)
	cdc.Insert(1, []cdcFloat32{cdcFloat32(math.Float32frombits(0x15ae43fd)), 1.0000001}, nil)
	text, err := cdc.ToJson()
	require.NoError(t, err)
	var got VectorIndexCdc[cdcFloat32]
	require.NoError(t, got.UnmarshalJSON([]byte(text)))
	require.Equal(t, uint32(0x15ae43fd), math.Float32bits(float32(got.Data[0].Vec[0])))
	require.Equal(t, cdc.Data, got.Data)
}

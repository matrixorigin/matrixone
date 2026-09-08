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
	"testing"

	"github.com/stretchr/testify/require"
	"strings"
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

// The checksum column carries an MD5 for base sub-index rows and a CRC for tail frames, so the
// value has to say which it is -- a reader that assumes one and gets the other silently compares
// nonsense. The set form also has to change when the chunks do, or it verifies nothing.
func TestCdcChunkSetChecksumIsSelfDescribing(t *testing.T) {
	require.Empty(t, CdcChunkSetChecksum(nil), "nothing to describe")
	require.Empty(t, CdcChunkSetChecksum([]uint32{}))

	one := CdcChunkSetChecksum([]uint32{0xdeadbeef})
	require.Equal(t, "crc32:deadbeef", one, "a single chunk is comparable to its own footer CRC")

	set := CdcChunkSetChecksum([]uint32{1, 2, 3})
	require.True(t, strings.HasPrefix(set, "crc32set:"), "several chunks fold, and say so: %s", set)
	require.NotEqual(t, one, set)

	require.NotEqual(t, set, CdcChunkSetChecksum([]uint32{3, 2, 1}),
		"order is part of what it covers: chunks replayed out of order are not the same tail")
	require.NotEqual(t, set, CdcChunkSetChecksum([]uint32{1, 2, 4}),
		"and a changed chunk must change it")
	require.Equal(t, set, CdcChunkSetChecksum([]uint32{1, 2, 3}), "stable for the same input")
}

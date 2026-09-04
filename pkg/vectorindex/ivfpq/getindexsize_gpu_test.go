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

//go:build gpu

package ivfpq

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// GetIndexSize is what the index cache's byte governor charges an entry against, so the two
// arenas must stay separate: RAM and VRAM are different budgets and summing them would bound
// neither. The figures come from what the load gate measured with cuvs.MeasureTar --
// HostComponentBytes for what stays in RAM, DeviceComponentBytes for what is deserialized onto
// the GPU -- never from the tar's FileSize, which conflates the two.
func TestGetIndexSizeSumsArenasSeparately(t *testing.T) {
	s := &IvfpqSearch[float32, float32]{
		Indexes: []*IvfpqModel[float32, float32]{
			{
				Id:                   "a",
				FileSize:             1 << 30, // must not leak into either arena
				HostComponentBytes:   100,
				DeviceComponentBytes: map[string]int64{"index.bin": 700},
			},
			{
				Id:                   "b",
				HostComponentBytes:   50,
				DeviceComponentBytes: map[string]int64{"shard_0.bin": 200, "shard_1.bin": 300},
			},
		},
	}
	host, device := s.GetIndexSize()
	require.EqualValues(t, 150, host, "host is the sum of HostComponentBytes, not the tar size")
	require.EqualValues(t, 1200, device, "device sums every GPU-resident component, sharded or not")
}

// A nil entry in the slice must not panic: Preload can leave the slice partially populated when
// a load is abandoned between Preload and Load.
func TestGetIndexSizeSkipsNilModels(t *testing.T) {
	s := &IvfpqSearch[float32, float32]{
		Indexes: []*IvfpqModel[float32, float32]{nil, {Id: "a", HostComponentBytes: 7}},
	}
	host, device := s.GetIndexSize()
	require.EqualValues(t, 7, host)
	require.EqualValues(t, 0, device)
}

// Nothing loaded is zero in both arenas -- the governor skips such an entry rather than charging
// it, and must not see a spurious figure.
func TestGetIndexSizeEmptyIsZero(t *testing.T) {
	var s IvfpqSearch[float32, float32]
	host, device := s.GetIndexSize()
	require.EqualValues(t, 0, host)
	require.EqualValues(t, 0, device)
}

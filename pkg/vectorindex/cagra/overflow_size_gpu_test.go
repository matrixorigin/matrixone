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

//go:build gpu

package cagra

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// stubOverflow reports a vector count and nothing else; GetIndexSize needs only Len().
type stubOverflow[B cuvs.VectorType] struct{ n uint64 }

func (s *stubOverflow[B]) SearchQuantizeAsync([]B, uint64, uint32, uint32) (uint64, error) {
	return 0, nil
}
func (s *stubOverflow[B]) SearchQuantizeWithFilterAsync([]B, uint64, uint32, uint32, string) (uint64, error) {
	return 0, nil
}
func (s *stubOverflow[B]) SearchWait(uint64, uint64, uint32) ([]int64, []float32, error) {
	return nil, nil, nil
}
func (s *stubOverflow[B]) Cap() uint64    { return s.n }
func (s *stubOverflow[B]) Len() uint64    { return s.n }
func (s *stubOverflow[B]) Destroy() error { return nil }

var _ cuvs.BruteForceOverflow[float32] = (*stubOverflow[float32])(nil)

// The CDC overflow is a device-resident brute-force index held for the entry's whole
// lifetime, so GetIndexSize must charge it to the device arena. An index whose rows all
// arrived by CDC has no built sub-index at all; without the overflow it would report 0/0,
// and both makeRoom and snapshotResidents skip a zero-sized entry -- so it would hold VRAM
// the governor never sees.
func TestCagraGetIndexSizeChargesOverflow(t *testing.T) {
	s := &CagraSearch[float32, float32]{}
	s.Idxcfg.CuvsCagra.Dimensions = 128

	host, device := s.GetIndexSize()
	require.Equal(t, int64(0), host)
	require.Equal(t, int64(0), device, "no sub-index and no overflow")

	s.Overflow = &stubOverflow[float32]{n: 1000}
	host, device = s.GetIndexSize()
	require.Equal(t, int64(0), host, "the overflow is device resident only")
	require.Equal(t, int64(1000*128*4), device)
}

// A built sub-index and an overflow are charged together, not one or the other.
func TestCagraGetIndexSizeAddsOverflowToSubIndexes(t *testing.T) {
	s := &CagraSearch[float32, float32]{}
	s.Idxcfg.CuvsCagra.Dimensions = 64
	s.Indexes = []*CagraModel[float32, float32]{
		{HostComponentBytes: 500, DeviceComponentBytes: map[string]int64{"0": 2000}},
	}
	s.Overflow = &stubOverflow[float32]{n: 10}

	host, device := s.GetIndexSize()
	require.Equal(t, int64(500), host)
	require.Equal(t, int64(2000+10*64*4), device)
}

// A nil overflow contributes nothing rather than panicking.
func TestCagraOverflowDeviceBytesNil(t *testing.T) {
	s := &CagraSearch[float32, float32]{}
	s.Idxcfg.CuvsCagra.Dimensions = 128
	require.Equal(t, int64(0), s.overflowDeviceBytes())
}

var _ = vectorindex.RuntimeConfig{}

// The overflow is built inside Load, not Preload, so the pre-load reservation cannot see it.
// This pins that timing: the governor's makeRoom charge is taken while Overflow is still nil,
// and only the post-load capture includes it. If buildOverflow ever moves into Preload this
// test should be updated to expect the reservation to cover it too.
func TestCagraOverflowIsInvisibleUntilLoad(t *testing.T) {
	s := &CagraSearch[float32, float32]{}
	s.Idxcfg.CuvsCagra.Dimensions = 128

	// Preload's view: metadata is known, the CDC overflow is not built yet.
	require.Nil(t, s.Overflow, "buildOverflow runs in Load, not Preload")
	_, deviceAtPreload := s.GetIndexSize()
	require.Equal(t, int64(0), deviceAtPreload,
		"the pre-load reservation cannot include an overflow that does not exist yet")

	// Load builds it; the post-load capture is what charges it.
	s.Overflow = &stubOverflow[float32]{n: 1000}
	_, deviceAfterLoad := s.GetIndexSize()
	require.Equal(t, int64(1000*128*4), deviceAfterLoad,
		"the post-load capture charges the overflow, so the entry is no longer 0/0")
	require.Greater(t, deviceAfterLoad, deviceAtPreload)
}

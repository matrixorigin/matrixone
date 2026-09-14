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

// The overflow is built inside Load, so its REAL size does not exist when admission decides.
// A CDC-only generation was therefore charged 0 at Preload: the governor reserved nothing and
// Load then allocated the VRAM unreserved, which no post-load pass can undo. The tail's own
// metadata rows carry the row count, so Preload charges an estimate and the real count
// supersedes it once the overflow exists.
func TestCagraOverflowIsChargedBeforeLoad(t *testing.T) {
	s := &CagraSearch[float32, float32]{}
	s.Idxcfg.CuvsCagra.Dimensions = 128

	// A CDC-only generation with nothing built yet: this is exactly what makeRoom sees.
	require.Nil(t, s.Overflow, "buildOverflow runs in Load, not Preload")
	s.overflowRowsEstimate = 1000 // what Preload read from the tail's metadata rows

	_, deviceAtPreload := s.GetIndexSize()
	require.Equal(t, int64(1000*128*4), deviceAtPreload,
		"admission must see the overflow's bytes BEFORE Load allocates them")

	// Load builds it; the real count replaces the estimate, which counted deletes the
	// overflow does not hold.
	s.Overflow = &stubOverflow[float32]{n: 900}
	_, deviceAfterLoad := s.GetIndexSize()
	require.Equal(t, int64(900*128*4), deviceAfterLoad,
		"the real count supersedes the upper bound rather than adding to it")
}

// Without an estimate -- a tail whose frames predate the metadata rows AND whose chunk count
// could not be read -- the charge is 0, which is the behaviour that existed before the estimate.
// It must not become a refusal.
func TestCagraOverflowWithoutAnEstimateChargesZero(t *testing.T) {
	s := &CagraSearch[float32, float32]{}
	s.Idxcfg.CuvsCagra.Dimensions = 128
	_, device := s.GetIndexSize()
	require.Zero(t, device)
}

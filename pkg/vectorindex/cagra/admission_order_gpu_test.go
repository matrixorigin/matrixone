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
	"github.com/matrixorigin/matrixone/pkg/vectorindex/memory"
)

// budgetStub reports a fixed hardware ceiling and a free reading the test controls, so the
// order of the two gates is observable without a real card.
type budgetStub struct {
	hardware int64
	free     int64
}

func (b *budgetStub) MaxAdmissible(int) (uint64, error) { return uint64(b.hardware), nil }

// RowsFitting is asked with perRowBytes == 1 by the aggregate gates, which makes "rows" the
// free-byte budget itself; see the comment on DeviceAggregateFitsFree.
func (b *budgetStub) RowsFitting(_ int, perRowBytes uint64) (int64, uint64, error) {
	if perRowBytes == 0 {
		perRowBytes = 1
	}
	return b.free / int64(perRowBytes), uint64(b.free), nil
}

var _ memory.DeviceBudget = (*budgetStub)(nil)

// The replacement case the split exists for: an index that does NOT fit in currently-free VRAM
// but does fit the hardware must pass Preload's gate, so the cache can evict and Load can then
// admit it. Before the split, Preload asked the free question and refused a load that eviction
// would have satisfied.
func TestPermanentGatePassesWhatEvictionCanFix(t *testing.T) {
	// 8 GiB card, 5 GiB free, index needs 6 GiB: impossible now, possible after eviction.
	const gib = int64(1) << 30
	budget := &budgetStub{hardware: 8 * gib, free: 5 * gib}
	demand := map[int]int64{0: 6 * gib}

	require.NoError(t, memory.DeviceAggregateFitsHardware(demand, 1, budget),
		"6 GiB fits an 8 GiB card, so the permanent gate must not refuse -- eviction can free the room")

	require.Error(t, memory.DeviceAggregateFitsFree(demand, 1, 1, budget),
		"and the situational gate does refuse at 5 GiB free, which is why it must run after eviction")
}

// An index larger than the card is refused by the permanent gate, so the early abort inside the
// fetch loop is preserved: no amount of eviction creates VRAM the hardware does not have.
func TestPermanentGateStillRefusesTheImpossible(t *testing.T) {
	const gib = int64(1) << 30
	budget := &budgetStub{hardware: 8 * gib, free: 8 * gib}

	require.Error(t, memory.DeviceAggregateFitsHardware(map[int]int64{0: 20 * gib}, 1, budget),
		"20 GiB never fits an 8 GiB card; refuse before downloading the rest")
}

// deviceFitsFreeNow is what Load calls; with nothing measured it must not refuse.
func TestDeviceFitsFreeNowNoMeasuredComponents(t *testing.T) {
	const gib = int64(1) << 30
	s := &CagraSearch[float32, float32]{Devices: []int{0}}
	require.NoError(t, s.deviceFitsFreeNow(&budgetStub{hardware: 8 * gib, free: 0}),
		"no measured device components means nothing to admit")

	s.Indexes = []*CagraModel[float32, float32]{{DeviceComponentBytes: map[string]int64{}}}
	require.NoError(t, s.deviceFitsFreeNow(&budgetStub{hardware: 8 * gib, free: 0}))
}

// The stubs above prove the DECISION; this proves the WIRING. deviceFitsFreeNow is a new call
// site in Load, and it is the half of admission that moved -- so it is exercised here against
// the REAL cuvs.BudgetFor reading of this machine's card, not a stub, with device components
// small enough that any working card admits them.
//
// It does not reproduce the eviction-then-admit sequence: that needs an index which exceeds
// current free VRAM but fits the card, which on an 8 GiB device means multi-GiB fixtures. What
// it does catch is a wiring error -- a budget that cannot be read, a demand map built wrong, or
// a gate that refuses everything -- which is the failure mode the reorder could introduce.
func TestDeviceFitsFreeNowAgainstTheRealCard(t *testing.T) {
	s := &CagraSearch[float32, float32]{Devices: []int{0}}
	s.Idxcfg.CuvsCagra.Dimensions = 8
	s.Idxcfg.Type = "cagra"
	s.Indexes = []*CagraModel[float32, float32]{
		{Id: "tiny", DeviceComponentBytes: map[string]int64{"index.bin": 4 << 10}},
	}

	require.NoError(t, s.deviceFitsFreeNow(cuvs.BudgetFor(s.Idxcfg.Type)),
		"4 KiB must be admissible on a real card; a refusal here is a wiring fault, not pressure")
}

// And the same real budget refuses a demand no card could hold, so the gate is not simply
// returning nil for everything.
func TestDeviceFitsFreeNowRefusesTheImpossibleOnTheRealCard(t *testing.T) {
	s := &CagraSearch[float32, float32]{Devices: []int{0}}
	s.Idxcfg.CuvsCagra.Dimensions = 8
	s.Idxcfg.Type = "cagra"
	s.Indexes = []*CagraModel[float32, float32]{
		{Id: "huge", DeviceComponentBytes: map[string]int64{"index.bin": 1 << 50}}, // 1 PiB
	}

	require.Error(t, s.deviceFitsFreeNow(cuvs.BudgetFor(s.Idxcfg.Type)),
		"a petabyte fits no card, so the real budget must refuse it")
}

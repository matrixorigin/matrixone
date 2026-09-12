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

package ivfpq

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/memory"
)

// ivfpq carries its own copy of deviceFitsFreeNow, differing from cagra's only in the config
// field it reads. The cagra file covers the shared memory.* gates; this covers the ivfpq method,
// so an edit to one algorithm cannot silently diverge from the other.

// With nothing measured the gate must not refuse -- including when a model slot is nil, which is
// the guard the cagra cases do not reach.
func TestIvfpqDeviceFitsFreeNowNoMeasuredComponents(t *testing.T) {
	const gib = int64(1) << 30
	budget := &budgetStub{hardware: 8 * gib, free: 0}

	s := &IvfpqSearch[float32, float32]{Devices: []int{0}}
	require.NoError(t, s.deviceFitsFreeNow(budget), "no indexes means nothing to admit")

	s.Indexes = []*IvfpqModel[float32, float32]{nil}
	require.NoError(t, s.deviceFitsFreeNow(budget), "a nil model is skipped, not dereferenced")

	s.Indexes = []*IvfpqModel[float32, float32]{{DeviceComponentBytes: map[string]int64{}}}
	require.NoError(t, s.deviceFitsFreeNow(budget), "an unmeasured model contributes no demand")
}

// The wiring against this machine's real card: a demand any working GPU admits, and one no card
// could hold. Catches an unreadable budget or a demand map built wrong -- the failure the
// reorder could introduce -- without needing multi-GiB fixtures.
func TestIvfpqDeviceFitsFreeNowAgainstTheRealCard(t *testing.T) {
	s := &IvfpqSearch[float32, float32]{Devices: []int{0}}
	s.Idxcfg.CuvsIvfpq.Dimensions = 8
	s.Idxcfg.Type = "ivfpq"

	s.Indexes = []*IvfpqModel[float32, float32]{
		{Id: "tiny", DeviceComponentBytes: map[string]int64{"index.bin": 4 << 10}},
	}
	require.NoError(t, s.deviceFitsFreeNow(cuvs.BudgetFor(s.Idxcfg.Type)),
		"4 KiB must be admissible on a real card; a refusal here is a wiring fault, not pressure")

	s.Indexes = []*IvfpqModel[float32, float32]{
		{Id: "huge", DeviceComponentBytes: map[string]int64{"index.bin": 1 << 50}}, // 1 PiB
	}
	require.Error(t, s.deviceFitsFreeNow(cuvs.BudgetFor(s.Idxcfg.Type)),
		"a petabyte fits no card, so the real budget must refuse it")
}

// budgetStub mirrors the cagra file's stub: a fixed hardware ceiling and a free reading the test
// controls. RowsFitting is asked with perRowBytes == 1 by the aggregate gates, which makes
// "rows" the free-byte budget itself.
type budgetStub struct {
	hardware int64
	free     int64
}

func (b *budgetStub) MaxAdmissible(int) (uint64, error) { return uint64(b.hardware), nil }

func (b *budgetStub) RowsFitting(_ int, perRowBytes uint64) (int64, uint64, error) {
	if perRowBytes == 0 {
		perRowBytes = 1
	}
	return b.free / int64(perRowBytes), uint64(b.free), nil
}

var _ memory.DeviceBudget = (*budgetStub)(nil)

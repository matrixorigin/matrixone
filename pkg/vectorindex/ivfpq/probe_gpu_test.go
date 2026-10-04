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

//go:build gpu

package ivfpq

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

func probeCfg() vectorindex.IndexConfig {
	cfg := vectorindex.IndexConfig{}
	cfg.CuvsIvfpq.Dimensions = 128
	cfg.CuvsIvfpq.Lists = 16
	cfg.CuvsIvfpq.M = 4
	cfg.CuvsIvfpq.BitsPerCode = 8
	cfg.CuvsIvfpq.Metric = uint16(metric.Metric_L2Distance)
	cfg.CuvsIvfpq.DistributionMode = uint16(vectorindex.DistributionMode_SINGLE_GPU)
	return cfg
}

// TestProbeRowsFittingReleasesEverything calls the probe repeatedly. Each call
// builds a worker pool and tears it down again, so a teardown that leaked
// threads, streams or device memory shows up here as a later call failing or as
// the answer drifting down.
//
// The drift check is the real assertion: the probe must leave nothing behind. If
// it did, each iteration would see less free memory than the last -- which is
// exactly the failure mode that makes the probe a once-only call in the first
// place.
func TestProbeRowsFittingReleasesEverything(t *testing.T) {
	cfg := probeCfg()
	devices := []int{0}

	first, firstTrain, perRow, _, _, err := ProbeRowsFitting(cfg, metric.Quantization_F32, devices)
	require.NoError(t, err)
	require.Greater(t, first, int64(0))
	require.Greater(t, firstTrain, int64(0))
	require.Equal(t, uint64(12), perRow, "m=4 at 8 bits = 4 code bytes + 8 payload")

	for i := 0; i < 20; i++ {
		rows, _, gotPerRow, _, _, perr := ProbeRowsFitting(cfg, metric.Quantization_F32, devices)
		require.NoError(t, perr, "probe %d failed; a leaked worker pool would surface here", i)
		require.Equal(t, perRow, gotPerRow, "the cost model is deterministic")
		// Free VRAM moves a little under other activity, so allow a small band
		// rather than demanding equality -- but a leak would walk it downwards
		// monotonically, well outside this.
		require.InEpsilon(t, first, rows, 0.05,
			"probe %d answered %d against a first answer of %d: the probe is leaking", i, rows, first)
	}
}

// TestProbeRowsFittingHonoursQuantization checks the storage type reaches the
// C++ cost model: narrower codes are not what changes here (the PQ code width is
// set by m and bits_per_code), but the TRAINSET is staged in float32 plus a copy
// in T, so a narrow type costs MORE training memory, not less.
func TestProbeRowsFittingHonoursQuantization(t *testing.T) {
	cfg := probeCfg()
	devices := []int{0}

	_, trainF32, _, _, _, err := ProbeRowsFitting(cfg, metric.Quantization_F32, devices)
	require.NoError(t, err)
	_, trainF16, _, _, _, err := ProbeRowsFitting(cfg, metric.Quantization_F16, devices)
	require.NoError(t, err)

	// f32 trainset costs dim*4; f16 costs dim*(4+2). More bytes per row means
	// fewer rows fit, so the f16 bound must be the smaller number.
	require.Less(t, trainF16, trainF32,
		"a narrower storage type makes the trainset bigger, so fewer training rows fit")
}

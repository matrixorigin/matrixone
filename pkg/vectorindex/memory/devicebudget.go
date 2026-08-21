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

package memory

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// DeviceDistinct returns the distinct PHYSICAL device ids in first-seen order.
//
// Under gpu_multi_simulation the logical device list aliases one card several
// times ([0,0,0,0]). Anything that QUERIES hardware must iterate this rather
// than the raw list, or it asks the same card N times and reports the answer as
// though N cards had been surveyed. (Anything that ATTRIBUTES bytes wants the
// raw list, so the aliased shards accumulate onto the one physical card.)
func DeviceDistinct(devices []int) []int {
	if len(devices) == 0 {
		return nil
	}
	seen := make(map[int]struct{}, len(devices))
	out := make([]int, 0, len(devices))
	for _, d := range devices {
		if _, dup := seen[d]; dup {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	return out
}

// DeviceRowsFittingFunc reports how many rows of perRowBytes fit in the free
// memory of one device. It exists so the sizing POLICY below can be exercised
// without a GPU; production passes cuvs.RowsFittingFreeMem, whose signature this
// matches exactly.
type DeviceRowsFittingFunc func(dev int, perRowBytes uint64) (rows int64, freeBytes uint64, err error)

// DeviceMinRowsFitting sizes a build against the SMALLEST participating device.
//
// Heterogeneous free VRAM is supported and SHARDED cuts EQUAL shards, so the
// binding constraint is the smallest card, not the first one. Sampling only
// devices[0] on a 40 GiB + 8 GiB pair sizes every shard for the 40 GiB card and
// the 8 GiB card OOMs the moment its shard lands.
//
// Returns the minimum row capacity along with which device produced it and that
// device's free bytes, so callers can name the binding card in their log line.
// An empty device list yields (0, 0, 0, nil): "not measured", matching the
// caller contract that a missing GPU reading falls back to other bounds rather
// than collapsing capacity to zero.
func DeviceMinRowsFitting(devices []int, perRowBytes uint64, rowsFitting DeviceRowsFittingFunc) (
	rows int64, minDev int, minFree uint64, err error) {
	distinct := DeviceDistinct(devices)
	if len(distinct) == 0 {
		return 0, 0, 0, nil
	}
	if rowsFitting == nil {
		return 0, 0, 0, moerr.NewInternalErrorNoCtx("DeviceMinRowsFitting: nil rows-fitting func")
	}
	for i, d := range distinct {
		r, free, gerr := rowsFitting(d, perRowBytes)
		if gerr != nil {
			// Never guess. Assuming the whole table fits is precisely the
			// failure being prevented.
			return 0, d, 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"cannot size the index build against GPU memory on device %d: %v", d, gerr))
		}
		if i == 0 || r < rows {
			rows, minDev, minFree = r, d, free
		}
	}
	return rows, minDev, minFree, nil
}

// DeviceBuildBytes attributes a build's total device demand to the physical
// devices that will hold it.
//
// SHARDED splits the work, so each device holds total/N. REPLICATED puts a full
// copy on every device. SINGLE_GPU puts it all on the first. The map keys by
// device id, so under gpu_multi_simulation -- where the list aliases one card
// several times -- the shards correctly accumulate back onto that one card
// instead of pretending there are N.
func DeviceBuildBytes(mode vectorindex.DistributionMode, devices []int, totalBytes uint64) map[int]uint64 {
	perDev := make(map[int]uint64, len(devices))
	if totalBytes == 0 || len(devices) == 0 {
		return perDev
	}
	switch mode {
	case vectorindex.DistributionMode_SHARDED:
		per := totalBytes / uint64(len(devices))
		if per == 0 {
			per = totalBytes
		}
		for _, d := range devices {
			perDev[d] += per
		}
	case vectorindex.DistributionMode_REPLICATED:
		for _, d := range devices {
			perDev[d] += totalBytes
		}
	default:
		perDev[devices[0]] += totalBytes
	}
	return perDev
}

// DeviceBuildPeakBytes is the device demand a build must claim: the PEAK of its
// two phases, not the footprint of whichever one the caller happened to size.
//
// An IVF-PQ build allocates in two phases that do NOT overlap. The k-means
// trainset block closes (ivf_pq_build.cuh:1369) before detail::extend allocates
// the list data (:1374), so the two never coexist and the demand is max(), not
// the sum -- charging the sum would shrink capacity to buy bytes that are not
// competing.
//
// But max() also means the claim cannot be the resident figure alone. When
// training dominates -- a wide vector with a generous kmeans_train_percent
// against narrow PQ codes -- the trainset is the larger allocation, and claiming
// only the resident codes leaves the difference unclaimed. A concurrent load or
// build is then admitted against memory this build is about to take, and the
// combined allocation can OOM even though both admissions succeeded.
//
// Both figures are TOTALS for the build; distribution across devices is applied
// afterwards by DeviceBuildBytes, so a sharded build divides the peak and a
// replicated one charges it to every device.
func DeviceBuildPeakBytes(residentTotal, trainsetTotal uint64) uint64 {
	if trainsetTotal > residentTotal {
		return trainsetTotal
	}
	return residentTotal
}

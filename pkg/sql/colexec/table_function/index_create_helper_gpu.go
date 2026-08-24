//go:build gpu

// Copyright 2022 Matrix Origin
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

package table_function

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// quantizationBytes is the on-device element size of one vector component for a storage
// quantization. It sizes the per-row cost used to bound a build against VRAM.
//
// Only the GPU create paths size a build this way, so this lives behind the gpu tag
// with them rather than in the shared (untagged) helper.
func quantizationBytes(qt metric.QuantizationType) uint64 {
	switch qt {
	case metric.Quantization_F16:
		return 2
	case metric.Quantization_INT8, metric.Quantization_UINT8:
		return 1
	default: // Quantization_F32
		return 4
	}
}

// warnAggregateNotResident warns at CREATE time when a rotated build will produce an
// index that no query on THIS device set can load.
//
// Rotation bounds each BUILD, not the search. A build materialises one sub-index at a
// time, so N of them can each fit a device that could never hold their sum; a query is
// the opposite, reaching every list of every sub-index, so all N must be resident at
// once. memory.DeviceLoadFits enforces that at load and refuses cleanly before any
// allocation -- but that refusal arrives at the first query, potentially hours after the
// build that caused it. This says it while the operator is still watching the build.
//
// DESIGN DECISION -- warn, do not reject.
//
// The alternative raised in review was to fail CREATE outright (or to add bounded
// load/search/evict so a too-large index stays searchable on one device). Both were
// rejected deliberately:
//
//   - An index is a persisted artifact, and the build-time device set is not the
//     search-time device set. The same pair of sub-indexes that overflows one card is
//     fully searchable on two, or on the sharded multi-server layout that this rotation
//     exists to feed. Rejecting CREATE bakes the building host's topology into the
//     artifact and refuses a build that is valid for its target deployment.
//   - Bounded load/evict is single-node-only machinery that sharded search supersedes,
//     so building it now is work that would be deleted rather than extended.
//
// The interim contract is therefore: the build succeeds, the operator is warned here,
// and a query fails deterministically at load with an actionable message naming the
// aggregate. Revisit when search can span devices/servers -- at that point the gate has
// a larger budget to compare against and this warning stops firing on its own.
func warnAggregateNotResident(algo string, plan capacityPlan, rowsFit int64, perRow uint64) {
	if !aggregateNotResident(plan, rowsFit) {
		return
	}
	// rowsFit is the aggregate the participating devices admit -- rows_fitting() in C++
	// already scales it by the distribution mode -- so this compares totals, not
	// per-card numbers, and the message must not name a single device.
	logutil.Warnf("%s create: this index needs ~%d MB resident (%d rows across %d sub-index(es)) to be "+
		"searched, but the participating device(s) admit only ~%d MB (%d rows). The build will "+
		"SUCCEED -- rotation bounds each build, not the search -- and queries will be refused at "+
		"load until it runs on a larger device set. To search it on this host, rebuild with a "+
		"narrower storage type (QUANTIZATION) or index fewer rows",
		algo, (uint64(plan.CdcCutoff)*perRow)>>20, plan.CdcCutoff, plan.NumSubIdx,
		(uint64(rowsFit)*perRow)>>20, rowsFit)
}

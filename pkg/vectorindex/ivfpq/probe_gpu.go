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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// ProbeRowsFitting asks how many rows an IVF-PQ index of this shape can hold on
// these devices, and how many k-means training rows fit.
//
// Every device figure is computed in C++ (gpu_ivf_pq_t::rows_fitting): the
// per-row cost model, the trainset cost and the budget all live on the index
// class, and the probe runs on worker threads already bound to their device.
// The caller supplies index parameters and consumes row counts; it models no
// device bytes.
//
// The probe index is UNSIZED -- it allocates nothing but a worker pool, since
// sizing it would need the number being asked for -- and is destroyed here. The
// real index is created afterwards with the planned capacity.
//
// CALL THIS ONCE, before any sub-index has been built. A second probe runs after
// earlier sub-indexes have allocated, sees less free memory, and would make each
// successive sub-index smaller instead of all of them sharing one capacity.
//
// It lives in this package rather than the create TVF so it reuses ivfpqConfig,
// the one mapping from IndexConfig to cuVS build params. A second copy of that
// mapping in the planner is how the planner and the allocator drift apart.
func ProbeRowsFitting(idxcfg vectorindex.IndexConfig, qt metric.QuantizationType, devices []int) (
	rowsFit int64, trainsetRows int64, perRow uint64, minDevice int, minFree uint64, err error) {
	switch qt {
	case metric.Quantization_F16:
		return probeRowsFitting[float32, cuvs.Float16](idxcfg, devices)
	case metric.Quantization_INT8:
		return probeRowsFitting[float32, int8](idxcfg, devices)
	case metric.Quantization_UINT8:
		return probeRowsFitting[float32, uint8](idxcfg, devices)
	default:
		return probeRowsFitting[float32, float32](idxcfg, devices)
	}
}

func probeRowsFitting[B, Q cuvs.VectorType](idxcfg vectorindex.IndexConfig, devices []int) (
	rowsFit int64, trainsetRows int64, perRow uint64, minDevice int, minFree uint64, err error) {
	// Reuse the model's config mapping rather than restating it. The model itself
	// is never constructed on the device -- this is a pure config translation.
	m := &IvfpqModel[B, Q]{Idxcfg: idxcfg, Devices: devices, NThread: 1}
	_, bp, mode, cerr := m.ivfpqConfig()
	if cerr != nil {
		return 0, 0, 0, 0, 0, cerr
	}
	// The CONFIGURED distribution mode: capacity is about what the finished index
	// occupies, and a SHARDED index is spread over its devices, so its aggregate
	// capacity is larger. (The build-time mode differs -- a build runs on one
	// card -- but that is not what is being sized here.)
	return cuvs.IvfPqRowsFitting(
		uint64(idxcfg.CuvsIvfpq.Dimensions), uint64(bp.M), uint64(bp.BitsPerCode),
		uint64(unsafe.Sizeof(*new(Q))), devices, mode)
}

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

package cagra

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// ProbeRowsFitting asks how many rows a CAGRA index of this shape can hold on
// these devices.
//
// Every device figure is computed in C++ (gpu_cagra_t::rows_fitting): the
// per-row cost model -- the resident dataset plus the intermediate kNN graph --
// and the budget live on the index class, and the probe runs on worker threads
// already bound to their device. The caller consumes a row count and models no
// device bytes.
//
// The probe index is UNSIZED: it allocates nothing but a worker pool, since
// sizing it would need the number being asked for. It is destroyed here; the
// real index is created afterwards with the planned capacity.
//
// CALL THIS ONCE, before any sub-index has been built. A second probe runs after
// earlier sub-indexes have allocated, sees less free memory, and would make each
// successive sub-index smaller instead of all of them sharing one capacity.
//
// It lives here rather than in the create TVF so it reuses cagraConfig, the one
// mapping from IndexConfig to cuVS build params.
func ProbeRowsFitting(idxcfg vectorindex.IndexConfig, qt metric.QuantizationType, devices []int) (
	rowsFit int64, perRow uint64, minDevice int, minFree uint64, err error) {
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
	rowsFit int64, perRow uint64, minDevice int, minFree uint64, err error) {
	m := &CagraModel[B, Q]{Idxcfg: idxcfg, Devices: devices, NThread: 1}
	_, bp, mode, cerr := m.cagraConfig()
	if cerr != nil {
		return 0, 0, 0, 0, cerr
	}
	return cuvs.CagraRowsFitting(
		uint64(idxcfg.CuvsCagra.Dimensions), uint64(unsafe.Sizeof(*new(Q))),
		uint64(bp.IntermediateGraphDegree), devices, mode)
}

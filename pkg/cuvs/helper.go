//go:build gpu

// Copyright 2021 - 2022 Matrix Origin
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

package cuvs

/*
#include "../../cgo/cuvs/helper.h"
#include <stdlib.h>
*/
import "C"
import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"runtime"
	"unsafe"
)

// DistanceType maps to C.distance_type_t
type DistanceType C.distance_type_t

const (
	L2Expanded          DistanceType = C.DistanceType_L2Expanded
	L2SqrtExpanded      DistanceType = C.DistanceType_L2SqrtExpanded
	CosineExpanded      DistanceType = C.DistanceType_CosineExpanded
	L1                  DistanceType = C.DistanceType_L1
	L2Unexpanded        DistanceType = C.DistanceType_L2Unexpanded
	L2SqrtUnexpanded    DistanceType = C.DistanceType_L2SqrtUnexpanded
	InnerProduct        DistanceType = C.DistanceType_InnerProduct
	Linf                DistanceType = C.DistanceType_Linf
	Canberra            DistanceType = C.DistanceType_Canberra
	LpUnexpanded        DistanceType = C.DistanceType_LpUnexpanded
	CorrelationExpanded DistanceType = C.DistanceType_CorrelationExpanded
	JaccardExpanded     DistanceType = C.DistanceType_JaccardExpanded
	HellingerExpanded   DistanceType = C.DistanceType_HellingerExpanded
	Haversine           DistanceType = C.DistanceType_Haversine
	BrayCurtis          DistanceType = C.DistanceType_BrayCurtis
	JensenShannon       DistanceType = C.DistanceType_JensenShannon
	HammingUnexpanded   DistanceType = C.DistanceType_HammingUnexpanded
	KLDivergence        DistanceType = C.DistanceType_KLDivergence
	RusselRaoExpanded   DistanceType = C.DistanceType_RusselRaoExpanded
	DiceExpanded        DistanceType = C.DistanceType_DiceExpanded
	BitwiseHamming      DistanceType = C.DistanceType_BitwiseHamming
	Precomputed         DistanceType = C.DistanceType_Precomputed
	// Aliases
	CosineSimilarity DistanceType = C.DistanceType_CosineSimilarity
	Jaccard          DistanceType = C.DistanceType_Jaccard
	Hamming          DistanceType = C.DistanceType_Hamming
	Unknown          DistanceType = C.DistanceType_Unknown
)

// Quantization maps to C.quantization_t
type Quantization C.quantization_t

const (
	F32   Quantization = C.Quantization_F32
	F16   Quantization = C.Quantization_F16
	INT8  Quantization = C.Quantization_INT8
	UINT8 Quantization = C.Quantization_UINT8
)

// DistributionMode maps to C.distribution_mode_t
type DistributionMode C.distribution_mode_t

const (
	SingleGpu  DistributionMode = C.DistributionMode_SINGLE_GPU
	Sharded    DistributionMode = C.DistributionMode_SHARDED
	Replicated DistributionMode = C.DistributionMode_REPLICATED
)

// CagraBuildParams maps to C.cagra_build_params_t
type CagraBuildParams struct {
	IntermediateGraphDegree uint64
	GraphDegree             uint64
	AttachDatasetOnBuild    bool
}

func DefaultCagraBuildParams() CagraBuildParams {
	return CagraBuildParams{
		IntermediateGraphDegree: 128,
		GraphDegree:             64,
		AttachDatasetOnBuild:    true,
	}
}

// CagraSearchParams maps to C.cagra_search_params_t
type CagraSearchParams struct {
	ItopkSize   uint64
	SearchWidth uint64
}

func DefaultCagraSearchParams() CagraSearchParams {
	return CagraSearchParams{
		ItopkSize:   64,
		SearchWidth: 1,
	}
}

// IvfFlatBuildParams maps to C.ivf_flat_build_params_t
type IvfFlatBuildParams struct {
	NLists                 uint32
	AddDataOnBuild         bool
	KmeansTrainsetFraction float64
}

func DefaultIvfFlatBuildParams() IvfFlatBuildParams {
	return IvfFlatBuildParams{
		NLists:                 1024,
		AddDataOnBuild:         true,
		KmeansTrainsetFraction: 0.5,
	}
}

// IvfFlatSearchParams maps to C.ivf_flat_search_params_t
type IvfFlatSearchParams struct {
	NProbes uint32
}

func DefaultIvfFlatSearchParams() IvfFlatSearchParams {
	return IvfFlatSearchParams{
		NProbes: 20,
	}
}

// IvfPqBuildParams maps to C.ivf_pq_build_params_t
type IvfPqBuildParams struct {
	NLists                 uint32
	M                      uint32
	BitsPerCode            uint32
	AddDataOnBuild         bool
	KmeansTrainsetFraction float64
}

func DefaultIvfPqBuildParams() IvfPqBuildParams {
	return IvfPqBuildParams{
		NLists:                 1024,
		M:                      16,
		BitsPerCode:            8,
		AddDataOnBuild:         true,
		KmeansTrainsetFraction: 0.5,
	}
}

// IvfPqSearchParams maps to C.ivf_pq_search_params_t
type IvfPqSearchParams struct {
	NProbes uint32
}

func DefaultIvfPqSearchParams() IvfPqSearchParams {
	return IvfPqSearchParams{
		NProbes: 20,
	}
}

// Float16 is a 16-bit floating point type (IEEE 754-2008).
// Go does not have a native float16 type, so we use uint16 to represent its memory layout.
type Float16 uint16

// VectorType is a constraint for types that can be used as vector data.
type VectorType interface {
	float32 | Float16 | int8 | uint8
}

// GpuIndex is an interface for all GPU-accelerated indexes.
type GpuIndex interface {
	Start() error
	Build() error
	Destroy() error
	Info() (string, error)
}

// GetQuantization returns the Quantization enum for a given VectorType.
func GetQuantization[T VectorType]() Quantization {
	var zero T
	switch any(zero).(type) {
	case float32:
		return F32
	case Float16:
		return F16
	case int8:
		return INT8
	case uint8:
		return UINT8
	default:
		panic("unsupported vector type")
	}
}

// GpuConvertF32ToF16 converts a float32 slice to a Float16 slice using the GPU.
func GpuConvertF32ToF16(src []float32, dst []Float16, deviceID int) error {
	if len(src) == 0 {
		return nil
	}
	if len(src) != len(dst) {
		return moerr.NewInternalErrorNoCtx("source and destination slices must have the same length")
	}

	var errmsg *C.char
	C.gpu_convert_f32_to_f16(
		(*C.float)(unsafe.Pointer(&src[0])),
		unsafe.Pointer(&dst[0]),
		C.uint64_t(len(src)),
		C.int(deviceID),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(src)
	runtime.KeepAlive(dst)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// GetGpuDeviceCount returns the number of available CUDA devices.
func GetGpuDeviceCount() (int, error) {
	count := int(C.gpu_get_device_count())
	if count < 0 {
		return 0, moerr.NewInternalErrorNoCtx("failed to get GPU device count")
	}
	return count, nil
}

// GetGpuDeviceList returns a slice of available CUDA device IDs.
func GetGpuDeviceList() ([]int, error) {
	count, err := GetGpuDeviceCount()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return []int{}, nil
	}

	cDevices := make([]C.int, count)
	C.gpu_get_device_list(&cDevices[0], C.int(count))

	devices := make([]int, count)
	for i := 0; i < count; i++ {
		devices[i] = int(cDevices[i])
	}
	runtime.KeepAlive(cDevices)
	return devices, nil
}

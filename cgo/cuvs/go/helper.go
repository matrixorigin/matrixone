package mocuvs

/*
#cgo LDFLAGS: /home/eric/github/matrixone/cgo/cuvs/c/libmocuvs.so -Wl,-rpath=/home/eric/github/matrixone/cgo/cuvs/c
#cgo CFLAGS: -I../c

#include "helper.h"
#include <stdlib.h>
*/
import "C"
import (
    "fmt"
    "unsafe"
    "runtime"
)

// DistanceType maps to C.distance_type_t
type DistanceType C.distance_type_t

const (
    L2Expanded      DistanceType = C.DistanceType_L2Expanded
    L1              DistanceType = C.DistanceType_L1
    InnerProduct    DistanceType = C.DistanceType_InnerProduct
    CosineSimilarity DistanceType = C.DistanceType_CosineSimilarity
    Jaccard         DistanceType = C.DistanceType_Jaccard
    Hamming         DistanceType = C.DistanceType_Hamming
    Unknown         DistanceType = C.DistanceType_Unknown
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
}

func DefaultCagraBuildParams() CagraBuildParams {
    return CagraBuildParams{
        IntermediateGraphDegree: 128,
        GraphDegree:             64,
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
    NLists uint32
}

func DefaultIvfFlatBuildParams() IvfFlatBuildParams {
    return IvfFlatBuildParams{
        NLists: 1024,
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

// Float16 is a 16-bit floating point type (IEEE 754-2008).
// Go does not have a native float16 type, so we use uint16 to represent its memory layout.
type Float16 uint16

// VectorType is a constraint for types that can be used as vector data.
type VectorType interface {
    float32 | Float16 | int8 | uint8
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
        return fmt.Errorf("source and destination slices must have the same length")
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
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// GetGpuDeviceCount returns the number of available CUDA devices.
func GetGpuDeviceCount() (int, error) {
    count := int(C.gpu_get_device_count())
    if count < 0 {
        return 0, fmt.Errorf("failed to get GPU device count")
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
    actualCount := int(C.gpu_get_device_list(&cDevices[0], C.int(count)))
    
    devices := make([]int, actualCount)
    for i := 0; i < actualCount; i++ {
        devices[i] = int(cDevices[i])
    }
    runtime.KeepAlive(cDevices)
    return devices, nil
}

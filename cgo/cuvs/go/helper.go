package cuvs

/*
#cgo LDFLAGS: /home/eric/github/matrixone/cgo/cuvs/c/libmocuvs.so -Wl,-rpath=/home/eric/github/matrixone/cgo/cuvs/c
#cgo CFLAGS: -I../c

#include "helper.h"
#include <stdlib.h>
*/
import "C"
import (
    "fmt"
)

// DistanceType maps to C.CuvsDistanceTypeC
type DistanceType C.CuvsDistanceTypeC

const (
    L2Expanded      DistanceType = C.DistanceType_L2Expanded
    L1              DistanceType = C.DistanceType_L1
    InnerProduct    DistanceType = C.DistanceType_InnerProduct
    CosineSimilarity DistanceType = C.DistanceType_CosineSimilarity
    Jaccard         DistanceType = C.DistanceType_Jaccard
    Hamming         DistanceType = C.DistanceType_Hamming
    Unknown         DistanceType = C.DistanceType_Unknown
)

// Quantization maps to C.CuvsQuantizationC
type Quantization C.CuvsQuantizationC

const (
    F32   Quantization = C.Quantization_F32
    F16   Quantization = C.Quantization_F16
    INT8  Quantization = C.Quantization_INT8
    UINT8 Quantization = C.Quantization_UINT8
)

// GetGpuDeviceCount returns the number of available CUDA devices.
func GetGpuDeviceCount() (int, error) {
    count := int(C.GpuGetDeviceCount())
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
    actualCount := int(C.GpuGetDeviceList(&cDevices[0], C.int(count)))
    
    devices := make([]int, actualCount)
    for i := 0; i < actualCount; i++ {
        devices[i] = int(cDevices[i])
    }
    return devices, nil
}

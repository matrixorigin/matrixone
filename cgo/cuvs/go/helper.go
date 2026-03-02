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

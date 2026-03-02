package cuvs

import (
    "testing"
    "fmt"
)

func TestGpuHelpers(t *testing.T) {
    count, err := GetGpuDeviceCount()
    if err != nil {
        t.Fatalf("GetGpuDeviceCount failed: %v", err)
    }
    fmt.Printf("GPU Device Count: %d\n", count)

    devices, err := GetGpuDeviceList()
    if err != nil {
        t.Fatalf("GetGpuDeviceList failed: %v", err)
    }
    fmt.Printf("GPU Device List: %v\n", devices)

    if count > 0 && len(devices) == 0 {
        t.Errorf("Expected devices in list since count is %d", count)
    }
    if len(devices) != count {
        t.Errorf("Expected %d devices, got %d", count, len(devices))
    }
}

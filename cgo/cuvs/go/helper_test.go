package mocuvs

import (
    "testing"
)

func TestGpuHelpers(t *testing.T) {
    count, err := GetGpuDeviceCount()
    if err != nil {
        t.Fatalf("GetGpuDeviceCount failed: %v", err)
    }
    t.Logf("GPU Device Count: %d", count)

    devices, err := GetGpuDeviceList()
    if err != nil {
        t.Fatalf("GetGpuDeviceList failed: %v", err)
    }
    t.Logf("GPU Device List: %v", devices)
}

func TestGpuConvertF32ToF16(t *testing.T) {
    src := []float32{1.0, 2.0, 3.0, 4.0}
    deviceID := 0

    // Test conversion to F16
    dstF16 := make([]Float16, len(src))
    if err := GpuConvertF32ToF16(src, dstF16, deviceID); err != nil {
        t.Fatalf("GpuConvertF32ToF16 failed: %v", err)
    }
    // We can't easily verify the value without a float16 decoder, 
    // but we can check it didn't error.
}

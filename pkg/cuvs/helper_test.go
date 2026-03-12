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

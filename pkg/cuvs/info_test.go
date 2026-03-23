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
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
)

type commonInfo struct {
	ElementSize   int    `json:"element_size"`
	Dimension     int    `json:"dimension"`
	Metric        int    `json:"metric"`
	Status        string `json:"status"`
	Capacity      int    `json:"capacity"`
	CurrentLength int    `json:"current_length"`
	Devices       []int  `json:"devices"`
	Type          string `json:"type"`
}

func verifyCommonInfo(t *testing.T, infoStr string, expectedType string, expectedDim int, expectedElemSize int) {
	var info commonInfo
	err := json.Unmarshal([]byte(infoStr), &info)
	if err != nil {
		t.Fatalf("Failed to parse info JSON: %v\nJSON: %s", err, infoStr)
	}

	if info.Type != expectedType {
		t.Errorf("Expected type %s, got %s", expectedType, info.Type)
	}
	if info.Dimension != expectedDim {
		t.Errorf("Expected dimension %d, got %d", expectedDim, info.Dimension)
	}
	if info.ElementSize != expectedElemSize {
		t.Errorf("Expected element size %d, got %d", expectedElemSize, info.ElementSize)
	}
	if info.Status != "Loaded" {
		t.Errorf("Expected status Loaded, got %s", info.Status)
	}
}

func TestIndexInfoComprehensive(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil {
		t.Fatalf("Failed to get GPU devices: %v", err)
	}
	if len(devices) == 0 {
		t.Skip("No GPU devices available")
	}

	dimension := uint32(128)
	n_vectors := uint64(10000)

	// Test combinations of Index Type, Distribution Mode, and Data Type

	testCases := []struct {
		indexType string
		distMode  DistributionMode
		modeName  string
	}{
		{"CAGRA", SingleGpu, "SingleGPU"},
		{"CAGRA", Sharded, "Sharded"},
		{"CAGRA", Replicated, "Replicated"},
		{"IVF-Flat", SingleGpu, "SingleGPU"},
		{"IVF-Flat", Sharded, "Sharded"},
		{"IVF-Flat", Replicated, "Replicated"},
		{"IVF-PQ", SingleGpu, "SingleGPU"},
		{"IVF-PQ", Sharded, "Sharded"},
		{"IVF-PQ", Replicated, "Replicated"},
	}

	runTest := func(t *testing.T, indexType string, distMode DistributionMode, modeName string, dataType string) {
		if distMode == Sharded {
			t.Skip("Sharded mode is currently disabled due to a suspected bug in cuVS")
		}
		name := fmt.Sprintf("%s/%s/%s", indexType, modeName, dataType)
		t.Run(name, func(t *testing.T) {
			var index GpuIndex
			var err error
			var elemSize int

			// We use a large dataset
			switch dataType {
			case "float32":
				dataset := GenerateRandomDataset(n_vectors, dimension)
				elemSize = 4
				switch indexType {
				case "CAGRA":
					bp := DefaultCagraBuildParams()
					bp.IntermediateGraphDegree = 256
					bp.GraphDegree = 128
					index, err = NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, distMode)
				case "IVF-Flat":
					bp := DefaultIvfFlatBuildParams()
					bp.NLists = 1000
					index, err = NewGpuIvfFlat[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, distMode)
				case "IVF-PQ":
					bp := DefaultIvfPqBuildParams()
					bp.NLists = 1000
					bp.M = 16
					index, err = NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, distMode)
				}
			case "Float16":
				dataset := make([]Float16, n_vectors*uint64(dimension))
				for i := range dataset {
					dataset[i] = Float16(rand.Uint32())
				}
				elemSize = 2
				switch indexType {
				case "CAGRA":
					bp := DefaultCagraBuildParams()
					bp.IntermediateGraphDegree = 256
					bp.GraphDegree = 128
					index, err = NewGpuCagra[Float16](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, distMode)
				case "IVF-Flat":
					bp := DefaultIvfFlatBuildParams()
					bp.NLists = 1000
					index, err = NewGpuIvfFlat[Float16](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, distMode)
				case "IVF-PQ":
					bp := DefaultIvfPqBuildParams()
					bp.NLists = 1000
					bp.M = 16
					index, err = NewGpuIvfPq[Float16](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, distMode)
				}
			case "int8":
				dataset := make([]int8, n_vectors*uint64(dimension))
				for i := range dataset {
					dataset[i] = int8(rand.Intn(256) - 128)
				}
				elemSize = 1
				switch indexType {
				case "CAGRA":
					bp := DefaultCagraBuildParams()
					bp.IntermediateGraphDegree = 256
					bp.GraphDegree = 128
					index, err = NewGpuCagra[int8](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, distMode)
				case "IVF-Flat":
					bp := DefaultIvfFlatBuildParams()
					bp.NLists = 1000
					index, err = NewGpuIvfFlat[int8](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, distMode)
				case "IVF-PQ":
					bp := DefaultIvfPqBuildParams()
					bp.NLists = 1000
					bp.M = 16
					index, err = NewGpuIvfPq[int8](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, distMode)
				}
			case "uint8":
				dataset := make([]uint8, n_vectors*uint64(dimension))
				for i := range dataset {
					dataset[i] = uint8(rand.Intn(256))
				}
				elemSize = 1
				switch indexType {
				case "CAGRA":
					bp := DefaultCagraBuildParams()
					bp.IntermediateGraphDegree = 256
					bp.GraphDegree = 128
					index, err = NewGpuCagra[uint8](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, distMode)
				case "IVF-Flat":
					bp := DefaultIvfFlatBuildParams()
					bp.NLists = 1000
					index, err = NewGpuIvfFlat[uint8](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, distMode)
				case "IVF-PQ":
					bp := DefaultIvfPqBuildParams()
					bp.NLists = 1000
					bp.M = 16
					index, err = NewGpuIvfPq[uint8](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, distMode)
				}
			}

			if err != nil {
				t.Fatalf("Failed to create index: %v", err)
			}
			defer index.Destroy()

			if err := index.Start(); err != nil {
				t.Fatalf("Failed to start index: %v", err)
			}
			if err := index.Build(); err != nil {
				t.Fatalf("Failed to build index: %v", err)
			}

			infoStr, err := index.Info()
			if err != nil {
				t.Fatalf("Failed to get info: %v", err)
			}

			verifyCommonInfo(t, infoStr, indexType, int(dimension), elemSize)
		})
	}

	dataTypes := []string{"float32", "Float16", "int8", "uint8"}

	for _, tc := range testCases {
		for _, dt := range dataTypes {
			runTest(t, tc.indexType, tc.distMode, tc.modeName, dt)
		}
	}
}

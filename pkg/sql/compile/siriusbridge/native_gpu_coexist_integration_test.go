// Copyright 2026 Matrix Origin
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

//go:build gpu && sirius && sirius_integration && cgo && linux && amd64

package siriusbridge

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// TestNativeBridgeCoexistsWithCuvsInOneProcess guards the process-global CUDA
// ownership boundary. It proves that MO can use cuVS before an embedded Sirius
// query, execute the query, destroy Sirius, and then use cuVS again in the same
// process. Separate-process tests cannot detect CUDA teardown conflicts.
func TestNativeBridgeCoexistsWithCuvsInOneProcess(t *testing.T) {
	config := os.Getenv("MO_SIRIUS_TEST_CONFIG")
	if config == "" {
		t.Fatal("sirius_integration requires MO_SIRIUS_TEST_CONFIG")
	}
	config, err := filepath.Abs(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())

	assertCuvsL2 := func(stage string) {
		t.Helper()
		x := [][]float32{{1, 2, 3}}
		y := [][]float32{{1, 2, 3}, {4, 5, 6}}
		distance := make([]float32, len(y))
		handle, launchErr := metric.PairwiseDistanceLaunch(
			x, y, metric.Metric_L2sqDistance, distance,
			metric.GPUThresholdOverlapped, true,
		)
		if launchErr != nil {
			t.Fatalf("%s cuVS launch: %v", stage, launchErr)
		}
		got, waitErr := metric.PairwiseDistanceWait(handle, metric.Metric_L2sqDistance)
		if waitErr != nil {
			t.Fatalf("%s cuVS wait: %v", stage, waitErr)
		}
		if len(got) != 2 || math.Abs(float64(got[0])) > 1e-5 || math.Abs(float64(got[1]-27)) > 1e-5 {
			t.Fatalf("%s cuVS result: %v", stage, got)
		}
	}

	assertCuvsL2("before Sirius")
	runtime, err := New(Config{ConfigPath: config, GPUStreams: 2, MaxWaiting: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if closeErr := runtime.Close(ctx); closeErr != nil {
			t.Error(closeErr)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	request := nativeIntegrationRequest(t, "go-native-gpu-coexist", func(ctx context.Context, input *Input) error {
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, 42)
		return input.Push(ctx, 1, []Vector{{Data: data}})
	})
	query, err := runtime.Prepare(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	var rows uint32
	if err = query.Run(ctx, func(result Result) error {
		rows += result.Rows
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("embedded Sirius rows: %d", rows)
	}
	closeNativeQuery(t, query)
	if err = runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}

	assertCuvsL2("after Sirius")
}

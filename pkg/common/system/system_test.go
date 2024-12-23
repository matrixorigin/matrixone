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

package system

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
)

func TestCPU(t *testing.T) {
	defer leaktest.AfterTest(t)()
	st := stopper.NewStopper("test")
	defer st.Stop()
	Run(st)
	mcpu := NumCPU()
	time.Sleep(2 * time.Second)
	acpu := CPUAvailable()
	require.Equal(t, true, float64(mcpu) >= acpu)
}

func TestMemory(t *testing.T) {
	totalMemory := MemoryTotal()
	availableMemory := MemoryAvailable()
	require.Equal(t, true, totalMemory >= availableMemory)
}

// Benchmark_GoRutinues
// goos: darwin
// goarch: arm64
// pkg: github.com/matrixorigin/matrixone/pkg/common/system
// cpu: Apple M1 Pro
// Benchmark_GoRutinues
// Benchmark_GoRutinues/Atomic
// Benchmark_GoRutinues/Atomic-10         	1000000000	         0.5446 ns/op
// Benchmark_GoRutinues/GoMaxProcs
// Benchmark_GoRutinues/GoMaxProcs-10     	87136477	        14.06 ns/op
// Benchmark_GoRutinues/NumGoroutine
// Benchmark_GoRutinues/NumGoroutine-10   	249281432	         4.795 ns/op
func Benchmark_GoRutinues(b *testing.B) {
	var v atomic.Int32
	b.Logf("v: %d, runtime.GOMAXPROCS(0): %d, runtime.NumGoroutine: %d",
		v.Load(), runtime.GOMAXPROCS(0), runtime.NumGoroutine())
	b.Run("Atomic", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v.Load()
		}
	})
	b.Run("GoMaxProcs", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runtime.GOMAXPROCS(0)
		}
	})
	b.Run("NumGoroutine", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runtime.NumGoroutine() // running go routines, not eq GOMAXPROCS
		}
	})
}

// TestSetGoMaxProcs
// ut for https://github.com/matrixorigin/MO-Cloud/issues/4486
func TestSetGoMaxProcs(t *testing.T) {
	// init
	initMaxProcs := runtime.GOMAXPROCS(0)
	type args struct {
		n int
	}
	tests := []struct {
		name    string
		args    args
		wantRet int
		wantGet int
	}{
		{
			name: "normal",
			args: args{
				n: 5,
			},
			wantRet: initMaxProcs,
			wantGet: 5,
		},
		{
			name: "zero",
			args: args{
				n: 0,
			},
			wantRet: 5,
			wantGet: 5,
		},
		{
			name: "nagetive",
			args: args{
				n: -1,
			},
			wantRet: 5,
			wantGet: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SetGoMaxProcs(tt.args.n)
			require.Equal(t, tt.wantRet, got)
			gotQuery := GoMaxProcs()
			require.Equal(t, tt.wantGet, gotQuery)
		})
	}
}

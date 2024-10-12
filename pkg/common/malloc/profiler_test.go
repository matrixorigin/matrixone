// Copyright 2024 Matrix Origin
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

package malloc

import (
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/pprof/profile"
)

type testSampleValues struct {
	N atomic.Int64
	A atomic.Int64
}

func (t *testSampleValues) Init() {}

func (t *testSampleValues) SampleTypes() []*profile.ValueType {
	return []*profile.ValueType{
		{
			Type: "n",
		},
		{
			Type: "a",
		},
	}
}

func (t *testSampleValues) DefaultSampleType() string {
	return "n"
}

func (t *testSampleValues) Values() []int64 {
	n := t.N.Load()
	a := t.A.Load()
	return []int64{n, a}
}

func (t *testSampleValues) Merge(merges []*testSampleValues) *testSampleValues {
	n := t.N.Load()
	a := t.A.Load()
	for _, merge := range merges {
		n += merge.N.Load()
		a += merge.A.Load()
	}
	ret := new(testSampleValues)
	ret.Init()
	ret.N.Add(n)
	ret.A.Add(a)
	return ret
}

func TestProfiler(t *testing.T) {
	testProfiler(t, io.Discard)
}

func TestProfilerWrite(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "test_profile")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	testProfiler(t, f)
}

func testProfiler(t *testing.T, w io.Writer) {
	profiler := NewProfiler[testSampleValues]()
	for i := 0; i < 65536; i++ {
		values := profiler.Sample(0, 2)
		values.N.Add(1)
		values = profiler.Sample(0, 2)
		values.A.Add(2)
	}
	if err := profiler.Write(w); err != nil {
		t.Fatal(err)
	}
	for _, mergedSample := range profiler.mergedSamples {
		mergedValue := mergedSample.Values[0].Merge(mergedSample.Values[1:])
		values := mergedValue.Values()
		for _, value := range values {
			if value > 0 {
				if value < 30000 {
					t.Fatalf("got %+v", values)
				}
			}
		}
	}
}

func TestParallelProfilerWrite(t *testing.T) {
	profiler := NewProfiler[testSampleValues]()
	wg := new(sync.WaitGroup)
	n := 64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 64; i++ {
				values := profiler.Sample(0, 1)
				values.N.Add(1)
				if err := profiler.Write(io.Discard); err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkProfilerAddSample(b *testing.B) {
	profiler := NewProfiler[testSampleValues]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values := profiler.Sample(0, 1)
		values.N.Add(1)
	}
}

func BenchmarkProfilerAddSampleParallel(b *testing.B) {
	profiler := NewProfiler[testSampleValues]()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			values := profiler.Sample(0, 1)
			values.N.Add(1)
		}
	})
}

func BenchmarkProfilerWrite(b *testing.B) {
	profiler := NewProfiler[testSampleValues]()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			values := profiler.Sample(0, 1)
			values.N.Add(1)
			if err := profiler.Write(io.Discard); err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkCallers(b *testing.B) {
	pcs := make([]uintptr, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runtime.Callers(1, pcs)
	}
}

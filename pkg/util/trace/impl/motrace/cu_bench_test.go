// Copyright 2023 Matrix Origin
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

package motrace

import (
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"testing"
	"time"
)

// BenchmarkCalculateMem
//
// goos: darwin
// goarch: arm64
// BenchmarkCalculateMem/100MiB_100ms
// BenchmarkCalculateMem/100MiB_100ms-10         	564399114	         2.090 ns/op
// BenchmarkCalculateMem/500GiB_300s
// BenchmarkCalculateMem/500GiB_300s-10          	  117150	     10161 ns/op
func BenchmarkCalculateMem(b *testing.B) {
	type args struct {
		memByte    int64
		durationNS int64
		cfg        *config.OBCUConfig
	}
	benchmarks := []struct {
		name string
		args args
	}{
		{
			name: "100MiB_100ms",
			args: args{
				memByte:    100 << 20,
				durationNS: int64(100 * time.Millisecond),
				cfg:        &dummyOBConfig,
			},
		},
		{
			name: "500GiB_300s",
			args: args{
				memByte:    573384797164,
				durationNS: 309319808921,
				cfg:        &dummyOBConfig,
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				CalculateCUMem(bm.args.memByte, bm.args.durationNS, bm.args.cfg)
			}
		})
	}
}

// BenchmarkCalculate10kTimes
// goos: darwin
// goarch: arm64
// pkg: github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace
// BenchmarkCalculate10kTimes/100MiB_100ms
// BenchmarkCalculate10kTimes/100MiB_100ms-10         	   26626	     44598 ns/op
// BenchmarkCalculate10kTimes/500GiB_300s
// BenchmarkCalculate10kTimes/500GiB_300s-10          	      10	 103729042 ns/op
// BenchmarkCalculate10kTimes/100MiB_100ms_sum_fist
// BenchmarkCalculate10kTimes/100MiB_100ms_sum_fist-10         	    9730	    128880 ns/op
// BenchmarkCalculate10kTimes/500GiB_300s_sum_fist
// BenchmarkCalculate10kTimes/500GiB_300s_sum_fist-10          	    9130	    119859 ns/op
//
// -- 2rd time
// BenchmarkCalculate10kTimes
// BenchmarkCalculate10kTimes/100MiB_100ms
// BenchmarkCalculate10kTimes/100MiB_100ms-10         	   26158	     45206 ns/op
// BenchmarkCalculate10kTimes/500GiB_300s
// BenchmarkCalculate10kTimes/500GiB_300s-10          	      12	  97179323 ns/op
// BenchmarkCalculate10kTimes/100MiB_100ms_sum_fist
// BenchmarkCalculate10kTimes/100MiB_100ms_sum_fist-10         	   10000	    130159 ns/op
// BenchmarkCalculate10kTimes/500GiB_300s_sum_fist
// BenchmarkCalculate10kTimes/500GiB_300s_sum_fist-10          	    9540	    120183 ns/op
func BenchmarkCalculate10kTimes(b *testing.B) {
	type args struct {
		stats      statistic.StatsArray
		durationNS int64
		cfg        *config.OBCUConfig
	}

	// [ 3, 7528422223, 573384797164.000, 0, 1, 247109, 2 ]
	var stats statistic.StatsArray
	stats.WithTimeConsumed(123456)
	stats.WithMemorySize(128 << 20)
	stats.WithS3IOInputCount(1)
	stats.WithS3IOOutputCount(1)
	stats.WithOutTrafficBytes(247109)
	stats.WithConnType(2)

	benchmarks := []struct {
		name string
		args args
	}{
		{
			name: "100MiB_100ms",
			args: args{
				stats:      stats,
				durationNS: int64(100 * time.Millisecond),
				cfg:        &dummyOBConfig,
			},
		},
		{
			name: "500GiB_300s",
			args: args{
				stats:      stats,
				durationNS: 309319808921,
				cfg:        &dummyOBConfig,
			},
		},
	}

	batch := int(10e3)

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			val := float64(0)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for cnt := 0; cnt < batch; cnt++ {
					val += CalculateCUWithCfg(bm.args.stats, bm.args.durationNS, bm.args.cfg)
				}
			}
		})
	}
	for _, bm := range benchmarks {
		b.Run(bm.name+"_sum_fist", func(b *testing.B) {
			var stats statistic.StatsArray
			var durationNS int64
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for cnt := 0; cnt < batch; cnt++ {
					stats.Add(&bm.args.stats)
					durationNS += bm.args.durationNS
				}
				_ = CalculateCUWithCfg(stats, durationNS, bm.args.cfg)
			}
		})
	}
}

// BenchmarkCalculateElem
// goos: darwin
// goarch: arm64
// pkg: github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace
// BenchmarkCalculateElem/100MiB_100ms
// BenchmarkCalculateElem/100MiB_100ms-10         	336092049	         3.546 ns/op
// BenchmarkCalculateElem/100MiB_100ms/cpu
// BenchmarkCalculateElem/100MiB_100ms/cpu-10     	568659853	         2.062 ns/op
// BenchmarkCalculateElem/100MiB_100ms/mem
// BenchmarkCalculateElem/100MiB_100ms/mem-10     	583113988	         2.052 ns/op
// BenchmarkCalculateElem/100MiB_100ms/io
// BenchmarkCalculateElem/100MiB_100ms/io-10      	425241382	         2.849 ns/op
// BenchmarkCalculateElem/100MiB_100ms/traffic
// BenchmarkCalculateElem/100MiB_100ms/traffic-10 	585322668	         2.070 ns/op
func BenchmarkCalculateElem(b *testing.B) {
	type args struct {
		stats      statistic.StatsArray
		durationNS int64
		cfg        *config.OBCUConfig
	}

	// [ 3, 7528422223, 573384797164.000, 0, 1, 247109, 2 ]
	var stats statistic.StatsArray
	stats.WithTimeConsumed(123456)
	stats.WithMemorySize(128 << 20)
	stats.WithS3IOInputCount(1)
	stats.WithS3IOOutputCount(1)
	stats.WithOutTrafficBytes(247109)
	stats.WithConnType(2)

	benchmarks := []struct {
		name string
		args args
	}{
		{
			name: "100MiB_100ms",
			args: args{
				stats:      stats,
				durationNS: int64(100 * time.Millisecond),
				cfg:        &dummyOBConfig,
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = CalculateCUWithCfg(bm.args.stats, bm.args.durationNS, bm.args.cfg)
			}
		})
	}
	for _, bm := range benchmarks {
		b.Run(bm.name+"/cpu", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = CalculateCUCpu(int64(bm.args.stats.GetTimeConsumed()), bm.args.cfg)
			}
		})
	}
	for _, bm := range benchmarks {
		b.Run(bm.name+"/mem", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = CalculateCUMem(int64(bm.args.stats.GetMemorySize()), bm.args.durationNS, bm.args.cfg)
			}
		})
	}
	for _, bm := range benchmarks {
		b.Run(bm.name+"/io", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = CalculateCUIOIn(int64(bm.args.stats.GetS3IOInputCount()), bm.args.cfg)
				_ = CalculateCUIOOut(int64(bm.args.stats.GetS3IOOutputCount()), bm.args.cfg)
			}
		})
	}
	for _, bm := range benchmarks {
		b.Run(bm.name+"/traffic", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = CalculateCUTraffic(int64(bm.args.stats.GetOutTrafficBytes()), bm.args.stats.GetConnType(), bm.args.cfg)
			}
		})
	}

}

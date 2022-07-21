// Copyright 2021 Matrix Origin
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

package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/metrics"
	"runtime/pprof"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	cpuProfilePathFlag       = flag.String("cpu-profile", "", "write cpu profile to the specified file")
	allocsProfilePathFlag    = flag.String("allocs-profile", "", "write allocs profile to the specified file")
	heapProfilePathFlag      = flag.String("heap-profile", "", "write heap profile to the specified file")
	heapProfileThresholdFlag = flag.Uint64("heap-profile-threshold", 8*1024*1024*1024,
		"take a heap profile if mapped memory changes exceed the specified threshold bytes")
	logMetricsIntervalFlag = flag.Uint64("log-metrics-interval", 23,
		"log metrics every specified seconds. 0 means disable logging")
	httpFlag = flag.String("http", "",
		"start http server at specified address")
)

func startCPUProfile() func() {
	cpuProfilePath := *cpuProfilePathFlag
	if cpuProfilePath == "" {
		cpuProfilePath = "cpu-profile"
	}
	f, err := os.Create(cpuProfilePath)
	if err != nil {
		panic(err)
	}
	err = pprof.StartCPUProfile(f)
	if err != nil {
		panic(err)
	}
	logutil.Infof("CPU profiling enabled, writing to %s", cpuProfilePath)
	return func() {
		pprof.StopCPUProfile()
		f.Close()
	}
}

func writeAllocsProfile() {
	profile := pprof.Lookup("allocs")
	if profile == nil {
		return
	}
	profilePath := *allocsProfilePathFlag
	if profilePath == "" {
		profilePath = "allocs-profile"
	}
	f, err := os.Create(profilePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := profile.WriteTo(f, 0); err != nil {
		panic(err)
	}
	logutil.Infof("Allocs profile written to %s", profilePath)
}

func handleDebugFlags() {

	if *heapProfilePathFlag != "" {
		go func() {

			samples := []metrics.Sample{
				{
					Name: "/memory/classes/total:bytes",
				},
			}
			var lastUsing uint64
			for range time.NewTicker(time.Second).C {

				writeHeapProfile := func() {
					metrics.Read(samples)
					using := samples[0].Value.Uint64()
					if using-lastUsing > *heapProfileThresholdFlag {
						profile := pprof.Lookup("heap")
						if profile == nil {
							return
						}
						profilePath := *heapProfilePathFlag
						if profilePath == "" {
							profilePath = "allocs-profile"
						}
						profilePath += "." + time.Now().Format("15:04:05.000000")
						f, err := os.Create(profilePath)
						if err != nil {
							panic(err)
						}
						defer f.Close()
						if err := profile.WriteTo(f, 0); err != nil {
							panic(err)
						}
						logutil.Infof("Heap profile written to %s", profilePath)
					}
					lastUsing = using
				}

				writeHeapProfile()
			}

		}()
	}

	if *logMetricsIntervalFlag != 0 {
		go func() {

			samples := []metrics.Sample{
				// gc infos
				{
					Name: "/gc/heap/allocs:bytes",
				},
				{
					Name: "/gc/heap/frees:bytes",
				},
				{
					Name: "/gc/heap/goal:bytes",
				},
				// memory infos
				{
					Name: "/memory/classes/heap/free:bytes",
				},
				{
					Name: "/memory/classes/heap/objects:bytes",
				},
				{
					Name: "/memory/classes/heap/released:bytes",
				},
				{
					Name: "/memory/classes/heap/unused:bytes",
				},
				{
					Name: "/memory/classes/total:bytes",
				},
				// goroutine infos
				{
					Name: "/sched/goroutines:goroutines",
				},
			}

			for range time.NewTicker(time.Second * time.Duration(*logMetricsIntervalFlag)).C {
				metrics.Read(samples)

				var fields []zapcore.Field
				for _, sample := range samples {
					switch sample.Value.Kind() {
					case metrics.KindUint64:
						fields = append(fields, zap.Uint64(sample.Name, sample.Value.Uint64()))
					case metrics.KindFloat64:
						fields = append(fields, zap.Float64(sample.Name, sample.Value.Float64()))
					}
				}

				logutil.Debug("runtime metrics", fields...)

			}

		}()
	}

	if *httpFlag != "" {
		go func() {
			if err := http.ListenAndServe(*httpFlag, nil); err != nil {
				panic(err)
			}
		}()
	}

}

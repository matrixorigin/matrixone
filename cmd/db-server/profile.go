package main

import (
	"flag"
	"os"
	"runtime/metrics"
	"runtime/pprof"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var (
	versionFlag 			 = flag.String("version", "", "if the argument passed in is '--version', mo-server will print MatrixOne build information and exits")
	cpuProfilePathFlag       = flag.String("cpu-profile", "", "write cpu profile to the specified file")
	allocsProfilePathFlag    = flag.String("allocs-profile", "", "write allocs profile to the specified file")
	heapProfilePathFlag      = flag.String("heap-profile", "", "write heap profile to the specified file")
	heapProfileThresholdFlag = flag.Uint64("heap-profile-threshold", 8*1024*1024*1024,
		"take a heap profile if mapped memory changes exceed the specified threshold bytes")
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
	pprof.StartCPUProfile(f)
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

func init() {
	if *heapProfilePathFlag != "" {
		return
	}
	go func() {

		samples := []metrics.Sample{
			{
				Name: "/memory/classes/total:bytes",
			},
		}
		var lastUsing uint64
		for range time.NewTicker(time.Second).C {
			metrics.Read(samples)
			using := samples[0].Value.Uint64()
			if using-lastUsing > *heapProfileThresholdFlag {
				profile := pprof.Lookup("heap")
				if profile == nil {
					continue
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

	}()
}

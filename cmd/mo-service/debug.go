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
	_ "net/http/pprof"
	"os"
	"runtime/pprof"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var (
	cpuProfilePathFlag    = flag.String("cpu-profile", "", "write cpu profile to the specified file")
	allocsProfilePathFlag = flag.String("allocs-profile", "", "write allocs profile to the specified file")
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

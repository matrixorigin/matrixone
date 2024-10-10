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
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"time"

	"github.com/felixge/fgprof"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/profile"
	"github.com/matrixorigin/matrixone/pkg/util/status"
	"go.uber.org/zap"
)

var (
	cpuProfilePathFlag    = flag.String("cpu-profile", "", "write cpu profile to the specified file")
	allocsProfilePathFlag = flag.String("allocs-profile", "", "write allocs profile to the specified file")
	heapProfilePathFlag   = flag.String("heap-profile", "", "write heap profile to the specified file")
	httpListenAddr        = flag.String("debug-http", "", "http server listen address")
	profileInterval       = flag.Duration("profile-interval", 0, "profile interval")
	statusServer          = status.NewServer()
)

func init() {

	// global performance counter
	v, ok := perfcounter.Named.Load(perfcounter.NameForGlobal)
	if ok {
		http.Handle("/debug/perfcounter/", v.(*perfcounter.CounterSet))
	}

	// fgprof
	http.Handle("/debug/fgprof/", fgprof.Handler())

	// status server
	http.Handle("/debug/status/", statusServer)

}

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

func writeHeapProfile() {
	profile := pprof.Lookup("heap")
	if profile == nil {
		return
	}
	profilePath := *heapProfilePathFlag
	if profilePath == "" {
		profilePath = "heap-profile"
	}
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

// saveProfilesLoop save profiles again and again until the
// mo is terminated.
func saveProfilesLoop(sigs chan os.Signal) {
	if *profileInterval == 0 {
		return
	}

	if *profileInterval < time.Second*10 {
		*profileInterval = time.Second * 10
	}

	quit := false
	tk := time.NewTicker(*profileInterval)
	logutil.GetGlobalLogger().Info("save profiles loop started", zap.Duration("profile-interval", *profileInterval))
	for {
		select {
		case <-tk.C:
			logutil.GetGlobalLogger().Info("save profiles start")
			saveProfiles()
			logutil.GetGlobalLogger().Info("save profiles end")
		case <-sigs:
			quit = true
		}
		if quit {
			break
		}
	}
}

func saveProfiles() {
	//dump heap profile before stopping services
	saveProfile(profile.HEAP)
	//dump goroutine before stopping services
	saveProfile(profile.GOROUTINE)
	// dump malloc profile
	saveMallocProfile()
}

func saveProfile(typ string) string {
	name, _ := uuid.NewV7()
	profilePath := catalog.BuildProfilePath(globalServiceType, globalNodeId, typ, name.String()) + ".gz"
	logutil.GetGlobalLogger().Info("save profiles ", zap.String("path", profilePath))
	cnservice.SaveProfile(profilePath, typ, globalEtlFS)
	return profilePath
}

func saveMallocProfile() {
	buf := bytes.Buffer{}
	w := gzip.NewWriter(&buf)
	if err := malloc.WriteProfileData(w); err != nil {
		logutil.GetGlobalLogger().Error("failed to write malloc profile", zap.Error(err))
		return
	}
	if err := w.Close(); err != nil {
		return
	}
	name, _ := uuid.NewV7()
	profilePath := catalog.BuildProfilePath(globalServiceType, globalNodeId, "malloc", name.String()) + ".gz"
	writeVec := fileservice.IOVector{
		FilePath: profilePath,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Data:   buf.Bytes(),
				Size:   int64(len(buf.Bytes())),
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute*3)
	defer cancel()
	if err := globalEtlFS.Write(ctx, writeVec); err != nil {
		logutil.GetGlobalLogger().Error("failed to save malloc profile", zap.Error(err))
	}
}

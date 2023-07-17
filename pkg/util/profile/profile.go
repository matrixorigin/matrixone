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

package profile

import (
	"fmt"
	"io"
	"runtime/pprof"
	"runtime/trace"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	GOROUTINE    = "goroutine"
	HEAP         = "heap"
	ALLOCS       = "allocs"
	THREADCREATE = "threadcreate"
	BLOCK        = "block"
	MUTEX        = "mutex"

	CPU   = "cpu"
	TRACE = "trace"
)

// ProfileGoroutine you can see more infos in https://pkg.go.dev/runtime/pprof#Profile
func ProfileGoroutine(w io.Writer, debug int) error {
	return ProfileRuntime(GOROUTINE, w, debug)
}

func ProfileHeap(w io.Writer, debug int) error {
	return ProfileRuntime(HEAP, w, debug)
}

func ProfileAllocs(w io.Writer, debug int) error {
	return ProfileRuntime(ALLOCS, w, debug)
}

func ProfileThreadcreate(w io.Writer, debug int) error {
	return ProfileRuntime(THREADCREATE, w, debug)
}

func ProfileBlock(w io.Writer, debug int) error {
	return ProfileRuntime(BLOCK, w, debug)
}

func ProfileMutex(w io.Writer, debug int) error {
	return ProfileRuntime(MUTEX, w, debug)
}

func ProfileRuntime(name string, w io.Writer, debug int) error {
	profile := pprof.Lookup(name)
	if profile == nil {
		return moerr.GetOkExpectedEOB()
	}
	if err := profile.WriteTo(w, debug); err != nil {
		return err
	}
	return nil
}

func ProfileCPU(w io.Writer, d time.Duration) error {
	err := pprof.StartCPUProfile(w)
	if err != nil {
		return err
	}
	time.Sleep(d)
	pprof.StopCPUProfile()
	return nil
}

func ProfileTrace(w io.Writer, d time.Duration) error {
	err := trace.Start(w)
	if err != nil {
		return err
	}
	time.Sleep(d)
	trace.Stop()
	return nil
}

const timestampFormatter = "20060102_150405.000000"

func Time2DatetimeString(t time.Time) string {
	return t.Format(timestampFormatter)
}

// GetProfileName get formatted filepath
// for example:
// - pprof/goroutine_${id}_${yyyyDDMM_hhmmss.ns}.pprof
// - pprof/heap_${id}_${yyyyDDMM_hhmmss.ns}.pprof
// - pprof/cpu_${id}_${yyyyDDMM_hhmmss.ns}.pprof
func GetProfileName(typ string, id string, t time.Time) string {
	return fmt.Sprintf("pprof/%s_%s_%s.pprof", typ, id, Time2DatetimeString(t))
}

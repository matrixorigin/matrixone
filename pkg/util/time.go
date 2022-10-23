// Copyright 2022 Matrix Origin
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

package util

import (
	_ "unsafe"
)

var globalStartMonoTimeNS TimeMono

func init() {
	globalStartMonoTimeNS = monotimeNS()
}

// `time.Now()` contain two syscalls in Linux.
// One is `CLOCK_REALTIME` and another is `CLOCK_MONOTONIC`.
// Separate it into two functions: walltime() and nanotime(), which can improve duration calculation.
// PS: runtime.walltime() hav been removed from linux-amd64

//go:linkname nanotime runtime.nanotime
func nanotime() int64

type TimeMono = uint64

// MonotimeNS used to calculate duration.
func monotimeNS() TimeMono {
	return TimeMono(nanotime())
}

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
	"runtime"
	"strconv"
	"strings"
	"unique"
)

// _PCs represents a fixed-size array of program counters (PCs) for stack traces.
// 128 is chosen as a reasonable maximum depth for most stack traces.
type _PCs [128]uintptr

type Stacktrace unique.Handle[_PCs]

func GetStacktrace(skip int) Stacktrace {
	if skip < 0 {
		skip = 0
	}
	var pcs _PCs
	runtime.Callers(2+skip, pcs[:])
	return Stacktrace(unique.Make(pcs))
}

func (s Stacktrace) String() string {
	var n int
	pcs := unique.Handle[_PCs](s).Value()
	for ; n < len(pcs); n++ {
		if pcs[n] == 0 {
			break
		}
	}
	return pcsToString(pcs[:n])
}

func pcsToString(pcs []uintptr) string {
	buf := new(strings.Builder)

	frames := runtime.CallersFrames(pcs)
	for {
		frame, more := frames.Next()

		buf.WriteString(frame.Function)
		buf.WriteString("\n")
		buf.WriteString("\t")
		buf.WriteString(frame.File)
		buf.WriteString(":")
		buf.WriteString(strconv.Itoa(frame.Line))
		buf.WriteString("\n")

		if !more {
			break
		}
	}

	return buf.String()
}

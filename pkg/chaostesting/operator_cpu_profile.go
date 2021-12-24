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

package fz

import (
	"os"
	"runtime/pprof"
)

func SaveCPUProfile(filename string) Operator {
	var f *os.File
	return Operator{
		BeforeStart: func() {
			var err error
			f, err = os.Create(filename)
			ce(err)
			pprof.StartCPUProfile(f)
		},
		AfterStop: func() {
			pprof.StopCPUProfile()
			ce(f.Close())
			f = nil
		},
		Finally: func() {
			if f != nil {
				f.Close()
			}
		},
	}
}

type EnableCPUProfile bool

func (_ Def) EnableCPUProfile() EnableCPUProfile {
	return false
}

func (_ Def) EnableCPUProfileConfig(
	enable EnableCPUProfile,
) ConfigItems {
	return ConfigItems{enable}
}

func (_ Def) CPUProfile(
	enable EnableCPUProfile,
) Operators {
	if !enable {
		return nil
	}
	return Operators{
		SaveCPUProfile("cpu-profile"),
	}
}

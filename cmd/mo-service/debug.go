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
	"fmt"
	"hash/fnv"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var (
	cpuProfilePathFlag    = flag.String("cpu-profile", "", "write cpu profile to the specified file")
	allocsProfilePathFlag = flag.String("allocs-profile", "", "write allocs profile to the specified file")
	httpListenAddr        = flag.String("debug-http", "", "http server listen address")
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

func init() {
	// stacktrace
	http.HandleFunc("/debug/stack/", func(w http.ResponseWriter, _ *http.Request) {

		fmt.Fprintf(w, `
      <style>
      * {
        margin: 0;
        padding: 0;
        font-size: 12px;
      }
      html {
        padding: 10px;
      }
      .grey {
        background-color: #EEE;
      }
      .bold {
        font-weight: bold;
      }
      .underline {
        text-decoration: underline;
      }
      .pad {
        padding: 10px;
      }
      </style>
    `)

		var records []runtime.StackRecord
		l := 1024
		for {
			records = make([]runtime.StackRecord, l)
			n, ok := runtime.GoroutineProfile(records)
			if !ok {
				l *= 2
				continue
			}
			records = records[:n]
			break
		}

		hashSums := make(map[uint64]bool)
		var allInfos [][]positionInfo

		for _, record := range records {
			frames := runtime.CallersFrames(record.Stack())

			h := fnv.New64()
			var infos []positionInfo
			for {
				frame, more := frames.Next()
				info := getPositionInfo(frame)
				infos = append(infos, info)
				fmt.Fprintf(h, "%+v", info.basicPositionInfo)
				if !more {
					break
				}
			}

			hashSum := h.Sum64()
			if hashSums[hashSum] {
				continue
			}
			hashSums[hashSum] = true

			allInfos = append(allInfos, infos)

		}

		sort.Slice(allInfos, func(i, j int) bool {
			aInfos := allInfos[i]
			bInfos := allInfos[j]
			aNumMOFrame := 0
			for _, info := range aInfos {
				if strings.Contains(info.PackagePath, "matrixone") ||
					strings.Contains(info.PackagePath, "main") {
					aNumMOFrame = 1
				}
			}
			bNumMOFrame := 0
			for _, info := range bInfos {
				if strings.Contains(info.PackagePath, "matrixone") ||
					strings.Contains(info.PackagePath, "main") {
					bNumMOFrame = 1
				}
			}
			if aNumMOFrame != bNumMOFrame {
				return aNumMOFrame > bNumMOFrame
			}
			a := aInfos[len(aInfos)-1]
			b := bInfos[len(bInfos)-1]
			if a.FileOnDisk != b.FileOnDisk {
				return a.FileOnDisk < b.FileOnDisk
			}
			return a.Line < b.Line
		})

		for i, infos := range allInfos {
			if i%2 == 0 {
				fmt.Fprintf(w, `<div class="pad">`)
			} else {
				fmt.Fprintf(w, `<div class="pad grey">`)
			}
			for _, info := range infos {
				fmt.Fprintf(w, `<p class="bold underline">%s %s:%d %s</p>`, info.PackagePath, path.Base(info.FileOnDisk), info.Line, info.FunctionName)
				if strings.Contains(info.PackagePath, "matrixone") ||
					strings.Contains(info.PackagePath, "main") {
					for _, line := range info.Content {
						fmt.Fprintf(w, `<div class=""><pre>%s</pre></div>`, line)
					}
				}
			}
			fmt.Fprintf(w, "</div>")
		}

	})
}

type positionInfo struct {
	basicPositionInfo
	Content []string
}

type basicPositionInfo struct {
	FileOnDisk             string
	Line                   int
	FullFunctionName       string
	FunctionName           string
	PackageAndFunctionName string
	PackageName            string
	PackagePath            string
}

func getPositionInfo(frame runtime.Frame) (info positionInfo) {
	packageParentPath, packageAndFuncName := path.Split(frame.Function)
	packageName, funcName, ok := strings.Cut(packageAndFuncName, ".")
	if !ok {
		panic("impossible")
	}
	packagePath := path.Join(packageParentPath, packageName)
	info = positionInfo{
		basicPositionInfo: basicPositionInfo{
			FileOnDisk:             frame.File,
			Line:                   frame.Line,
			FullFunctionName:       frame.Function,
			PackageAndFunctionName: packageAndFuncName,
			FunctionName:           funcName,
			PackageName:            packageName,
			PackagePath:            packagePath,
		},
		Content: getFileContent(frame.File, frame.Line),
	}
	return info
}

func getFileContent(file string, line int) (ret []string) {
	content, err := os.ReadFile(file)
	if err != nil {
		panic(err)
	}
	lines := strings.Split(string(content), "\n")
	n := line - 1
	if n > 0 {
		ret = append(ret, lines[n-1])
	}
	ret = append(ret, lines[n])
	if n+1 < len(lines) {
		ret = append(ret, lines[n+1])
	}
	return
}

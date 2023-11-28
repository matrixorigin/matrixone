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
	"unsafe"

	"github.com/felixge/fgprof"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/status"
)

var (
	cpuProfilePathFlag         = flag.String("cpu-profile", "", "write cpu profile to the specified file")
	allocsProfilePathFlag      = flag.String("allocs-profile", "", "write allocs profile to the specified file")
	heapProfilePathFlag        = flag.String("heap-profile", "", "write heap profile to the specified file")
	fileServiceProfilePathFlag = flag.String("file-service-profile", "", "write file service profile to the specified file")
	httpListenAddr             = flag.String("debug-http", "", "http server listen address")
	statusServer               = status.NewServer()
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

func startFileServiceProfile() func() {
	filePath := *fileServiceProfilePathFlag
	if filePath == "" {
		filePath = "file-service-profile"
	}
	f, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	write, stop := fileservice.FSProfileHandler.StartProfile()
	logutil.Infof("File service profiling enabled, writing to %s", filePath)
	return func() {
		stop()
		write(f)
		f.Close()
	}
}

func init() {

	const cssStyles = `
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
  `

	// stacktrace
	http.HandleFunc("/debug/stack/", func(w http.ResponseWriter, _ *http.Request) {

		fmt.Fprint(w, cssStyles)

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
			infos, sum := getStackInfo(record.Stack())
			if _, ok := hashSums[sum]; ok {
				continue
			}
			hashSums[sum] = true
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

	// heap
	http.HandleFunc("/debug/heap/", func(w http.ResponseWriter, _ *http.Request) {
		runtime.GC()

		fmt.Fprint(w, cssStyles)

		size := 1024
		records := make([]runtime.MemProfileRecord, size)
		for {
			n, ok := runtime.MemProfile(records, false)
			if !ok {
				size *= 2
				records = make([]runtime.MemProfileRecord, size)
				continue
			}
			records = records[:n]
			break
		}

		type position [3]uintptr
		liveBytes := make(map[uint64]int64)
		var positions []position
		for _, record := range records {
			pos := *(*position)(record.Stack0[:unsafe.Sizeof(position{})])
			_, sum := getStackInfo(pos[:])
			if _, ok := liveBytes[sum]; !ok {
				positions = append(positions, pos)
			}
			liveBytes[sum] += record.AllocBytes - record.FreeBytes
		}
		sort.Slice(positions, func(i, j int) bool {
			_, sum1 := getStackInfo(positions[i][:])
			_, sum2 := getStackInfo(positions[j][:])
			return liveBytes[sum1] > liveBytes[sum2]
		})

		for i, pos := range positions {
			posInfos, sum := getStackInfo(pos[:])
			if i%2 == 0 {
				fmt.Fprintf(w, `<div class="pad">`)
			} else {
				fmt.Fprintf(w, `<div class="pad grey">`)
			}
			fmt.Fprintf(w, `<p>%v</p>`, formatSize(liveBytes[sum]))
			for _, info := range posInfos {
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

	// file service profile
	http.Handle("/debug/fs/", fileservice.FSProfileHandler)

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

func getStackInfo(stack []uintptr) ([]positionInfo, uint64) {
	frames := runtime.CallersFrames(stack)

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
	return infos, hashSum
}

var units = []string{"B", "K", "M", "G", "T", "P", "E", "Z", "Y"}

func formatSize(n int64) string {
	if n == 0 {
		return "0"
	}
	return strings.TrimSpace(_formatBytes(n, 0))
}

func _formatBytes(n int64, unitIndex int) string {
	if n == 0 {
		return ""
	}
	var str string
	next := n / 1024
	rem := n - next*1024
	if rem > 0 {
		str = fmt.Sprintf(" %d%s", rem, units[unitIndex])
	}
	return _formatBytes(next, unitIndex+1) + str
}

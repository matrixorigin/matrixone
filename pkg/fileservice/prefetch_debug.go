// Copyright 2026 Matrix Origin
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

package fileservice

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"
)

func init() {
	http.HandleFunc("/debug/prefetch_objects", func(w http.ResponseWriter, _ *http.Request) {
		allDiskCaches.Range(func(k, v any) bool {
			fmt.Fprintf(w, "disk cache %v\n", v)
			k.(*DiskCache).writePrefetchObjects(w)
			return true
		})
	})
}

// writePrefetchObjects writes the prefetch objects of d, registered or waiting for their
// resources, one a line: its kind, object, bytes, where it is in its life and for how long,
// and the bytes and connections the prefetch objects hold.
func (d *DiskCache) writePrefetchObjects(w io.Writer) {
	f := &d.prefetchObjects
	now := time.Now()
	f.mu.Lock()
	var lines []string
	line := func(diskPath string, o prefetchObject) {
		kind := "memory"
		if _, ok := o.(*filePrefetchObject); ok {
			kind = "file"
		}
		state, age := "registered", ""
		if t, ok := o.(waitingObject); ok && t.target().d != nil {
			state, age = t.target().state.String(), now.Sub(t.target().since).Round(time.Millisecond).String()
		}
		if !o.joinable() && state == "registered" {
			state = "registered, not joinable yet"
		}
		start, end := o.span()
		lines = append(lines, fmt.Sprintf("  %s %s [%d, %d) %s %s", kind, diskPath, start, end, state, age))
	}
	for diskPath, objects := range f.m {
		for _, o := range objects {
			line(diskPath, o)
		}
	}
	for o := range f.waiting {
		line(o.target().diskPath, o)
	}
	bytes := f.bytes
	f.mu.Unlock()
	sort.Strings(lines)
	for _, l := range lines {
		fmt.Fprintln(w, l)
	}
	fmt.Fprintf(w, "  buffer bytes %d of %d, download connections %d of %d\n", bytes, d.flightBytes(), d.downloadConns.Load(), d.maxDownloadConns)
}

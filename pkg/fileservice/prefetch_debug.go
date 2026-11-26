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
// resources, and its queued fills, one a line: its kind, object, bytes, where it is in its life and for how long,
// and the bytes and connections the prefetch objects hold. It copies them under the lock
// and writes them after.
func (d *DiskCache) writePrefetchObjects(w io.Writer) {
	type row struct {
		file, fill     bool
		diskPath       string
		start, end     int64
		state          string
		since          time.Time
		joinable, kept bool
	}
	f := &d.prefetchObjects
	f.mu.Lock()
	var rows []row
	add := func(diskPath string, o prefetchObject) {
		r := row{diskPath: diskPath, state: "registered", joinable: o.joinable()}
		_, r.file = o.(*filePrefetchObject)
		r.start, r.end = o.span()
		if t, ok := o.(waitingObject); ok && t.target().d != nil {
			r.state, r.since, r.kept = t.target().state.String(), t.target().since, true
		}
		rows = append(rows, r)
	}
	for diskPath, objects := range f.m {
		for _, o := range objects {
			add(diskPath, o)
		}
	}
	for o := range f.waiting {
		add(o.target().diskPath, o)
	}
	for key, fills := range f.fills {
		for _, q := range fills {
			r := row{fill: true, diskPath: key.diskPath, start: q.start, end: q.end, state: "waiting", joinable: true}
			if q.running {
				r.state = "running"
			}
			if q.isPromoted() {
				r.state += ", foreground"
			}
			rows = append(rows, r)
		}
	}
	queued := f.queuedBytes
	f.mu.Unlock()

	now := time.Now()
	lines := make([]string, 0, len(rows))
	for _, r := range rows {
		kind, state, age := "memory", r.state, ""
		if r.file {
			kind = "file"
		}
		if r.fill {
			kind = "queued fill"
		}
		if r.kept {
			age = now.Sub(r.since).Round(time.Millisecond).String()
		}
		if state == "registered" && !r.joinable {
			state = "registered, not joinable yet"
		}
		lines = append(lines, fmt.Sprintf("  %s %s [%d, %d) %s %s", kind, r.diskPath, r.start, r.end, state, age))
	}
	sort.Strings(lines)
	for _, l := range lines {
		fmt.Fprintln(w, l)
	}
	fmt.Fprintf(w, "  queued background fill bytes %d of %d\n", queued, d.fillQueueLimit())
	if g := d.server; g != nil {
		used := g.memoryInUse()
		fmt.Fprintf(w, "  memory foreground %d background %d of %d\n", used[foregroundRequest], used[backgroundRequest], g.memoryBudget())
		running, waiting := g.counts()
		fmt.Fprintf(w, "  requests of %d: foreground running %d waiting %d, background running %d of %d waiting %d\n",
			g.capacity, running[foregroundRequest], waiting[foregroundRequest],
			running[backgroundRequest], g.backgroundMax, waiting[backgroundRequest])
	}
}

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

package fileservice

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/google/pprof/profile"
	"github.com/petermattis/goid"
)

// SpanProfiler prpfiles a span in one goroutine
// multiple SpanProfilers may share one *profile.Profile
type SpanProfiler struct {
	goID    int64
	profile *profile.Profile
	info    *ProfileInfo
	spans   []*ProfileSpan
}

// NewSpanProfiler creates a new span profiler
func NewSpanProfiler() *SpanProfiler {
	profile := &profile.Profile{
		SampleType: []*profile.ValueType{
			{
				Type: "count",
				Unit: "count",
			},
			{
				Type: "time",
				Unit: "nanoseconds",
			},
		},
	}
	return &SpanProfiler{
		goID:    goid.Get(),
		profile: profile,
		info:    NewProfileInfo(),
	}
}

// Begin begins a new span
// If the calling goroutine does not match s.goID, a new profiler for the calling goroutine will be created
// The newly created profiler will share the same profile to s.profile
func (s *SpanProfiler) Begin(skip int) (profiler *SpanProfiler, end func()) {
	t0 := time.Now()
	goID := goid.Get()

	if goID != s.goID {
		// fork
		copied := *s
		profiler = &copied
		profiler.goID = goID
	} else {
		profiler = s
	}

	var locations []*profile.Location
	var pcs []uintptr
	put := pcsPool.Get(&pcs)
	defer put.Put()
	pcs = pcs[:runtime.Callers(2+skip, pcs)]
	frames := runtime.CallersFrames(pcs)
	for {
		frame, more := frames.Next()
		if frame.Function == "" {
			// unknown function, ignore
			continue
		}
		location := profiler.info.getLocation(profiler.profile, frame)
		locations = append(locations, location)
		if !more {
			break
		}
	}

	locations = append(locations, profiler.info.getTagLocation(
		profiler.profile,
		fmt.Sprintf("goroutine %d", goID),
	))

	span := &ProfileSpan{
		BeginTime: t0,
		Locations: locations,
	}
	profiler.spans = append(profiler.spans, span)

	var endOnce sync.Once
	end = func() {
		endOnce.Do(func() {

			endGoID := goid.Get()

			if endGoID != goID {
				panic("span must end in the same goroutine as begin")
			}
			if profiler.spans[len(profiler.spans)-1] != span {
				panic("interleaving span")
			}

			duration := time.Since(span.BeginTime) - span.SubSpanDuration
			if duration <= 0 {
				// no need to add sample
				return
			}

			sample := &profile.Sample{
				Value: []int64{
					1,
					int64(duration / time.Nanosecond),
				},
				Location: locations,
			}
			profiler.profile.Sample = append(profiler.profile.Sample, sample)

			profiler.spans = profiler.spans[:len(profiler.spans)-1]
			if len(profiler.spans) > 0 {
				// add duration to parent's sub span duration
				profiler.spans[len(profiler.spans)-1].SubSpanDuration += duration
			}

		})
	}

	return
}

// Write writes the packed profile to writer w
func (s *SpanProfiler) Write(w io.Writer) error {
	if len(s.profile.Sample) == 0 {
		return nil
	}
	return s.profile.Write(w)
}

type ProfileSpan struct {
	BeginTime       time.Time
	Locations       []*profile.Location
	SubSpanDuration time.Duration
}

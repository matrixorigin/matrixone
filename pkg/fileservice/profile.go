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
	"io"
	"net/http"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/pprof/profile"
)

var FSProfileHandler = NewProfileHandler()

type ProfileHandler struct {
	profiling      atomic.Bool
	nextProfilerID int64
	stateChan      chan *profileState
}

type profileState struct {
	profilers map[int64]*profiler
}

func NewProfileHandler() *ProfileHandler {
	ch := make(chan *profileState, 1)
	state := &profileState{
		profilers: make(map[int64]*profiler),
	}
	ch <- state
	return &ProfileHandler{
		stateChan: ch,
	}
}

func (p *ProfileHandler) StartProfile() (
	write func(w io.Writer),
	stop func(),
) {

	// register
	state := <-p.stateChan
	id := atomic.AddInt64(&p.nextProfilerID, 1)
	profiler := newProfiler()
	state.profilers[id] = profiler
	p.profiling.Store(true)
	p.stateChan <- state

	write = func(w io.Writer) {
		state := <-p.stateChan
		state.profilers[id].profile.Write(w)
		p.stateChan <- state
	}

	stop = func() {
		state := <-p.stateChan
		delete(state.profilers, id)
		if len(state.profilers) == 0 {
			p.profiling.Store(false)
		}
		p.stateChan <- state
	}

	return
}

func (p *ProfileHandler) AddSample(duration time.Duration, tags ...string) {
	if !p.profiling.Load() {
		return
	}
	state := <-p.stateChan
	for _, profiler := range state.profilers {
		profiler.Add(1, duration, tags...)
	}
	p.stateChan <- state
}

func (p *ProfileHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	secStr := req.URL.Query().Get("seconds")
	sec, err := strconv.Atoi(secStr)
	if err != nil || sec > 3600*24 {
		sec = 30
	}

	write, stop := p.StartProfile()
	defer stop()
	defer write(w)

	select {
	case <-req.Context().Done():
	case <-time.After(time.Second * time.Duration(sec)):
	}
}

type profiler struct {
	profile *profile.Profile
	info    *ProfileInfo
}

func newProfiler() *profiler {
	return &profiler{
		profile: &profile.Profile{
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
		},
		info: NewProfileInfo(),
	}
}

func (p *profiler) Add(skip int, duration time.Duration, tags ...string) {
	sample := &profile.Sample{
		Value: []int64{
			1,
			int64(duration / time.Nanosecond),
		},
	}

	for _, tag := range tags {
		sample.Location = append(sample.Location, p.info.getTagLocation(p.profile, tag))
	}

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
		location := p.info.getLocation(p.profile, frame)
		sample.Location = append(sample.Location, location)

		if !more {
			break
		}
	}

	p.profile.Sample = append(p.profile.Sample, sample)
}

var pcsPool = NewPool(
	1024,
	func() []uintptr {
		return make([]uintptr, 128)
	},
	nil,
	nil,
)

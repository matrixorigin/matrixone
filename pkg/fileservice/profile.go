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
	"runtime"
	"sync/atomic"

	"github.com/google/pprof/profile"
)

func StartProfile(w io.Writer) (stop func()) {
	state := <-profileChan
	id := atomic.AddInt64(&nextProfilerID, 1)
	profiler := newProfiler()
	state.profilers[id] = profiler
	profiling.Store(true)
	profileChan <- state
	return func() {
		state := <-profileChan
		profiler := state.profilers[id]
		delete(state.profilers, id)
		if len(state.profilers) == 0 {
			profiling.Store(false)
		}
		profileChan <- state
		profiler.profile.Write(w)
	}
}

var profiling atomic.Bool

var nextProfilerID int64

type profileState struct {
	profilers map[int64]*profiler
}

var profileChan = func() chan *profileState {
	ch := make(chan *profileState, 1)
	state := &profileState{
		profilers: make(map[int64]*profiler),
	}
	ch <- state
	return ch
}()

func profileAddSample() {
	if !profiling.Load() {
		return
	}
	state := <-profileChan
	for _, profiler := range state.profilers {
		profiler.addSample(1)
	}
	profileChan <- state
}

type profiler struct {
	profile        *profile.Profile
	functions      map[string]*profile.Function
	nextLocationID uint64
	nextFunctionID uint64
}

func newProfiler() *profiler {
	return &profiler{
		profile: &profile.Profile{
			SampleType: []*profile.ValueType{
				{
					Type: "count",
					Unit: "count",
				},
			},
		},
		functions: make(map[string]*profile.Function),
	}
}

func (p *profiler) getFunction(frame runtime.Frame) *profile.Function {
	if fn, ok := p.functions[frame.Function]; ok {
		return fn
	}
	p.nextFunctionID++
	fn := &profile.Function{
		ID:   p.nextFunctionID,
		Name: frame.Function,
	}
	if frame.Func != nil {
		file, line := frame.Func.FileLine(frame.Func.Entry())
		fn.Filename = file
		fn.StartLine = int64(line)
	}
	p.functions[frame.Function] = fn
	p.profile.Function = append(p.profile.Function, fn)
	return fn
}

func (p *profiler) getLocation(frame runtime.Frame) *profile.Location {
	line := profile.Line{
		Function: p.getFunction(frame),
		Line:     int64(frame.Line),
	}
	p.nextLocationID++
	loc := &profile.Location{
		ID:   p.nextLocationID,
		Line: []profile.Line{line},
	}
	p.profile.Location = append(p.profile.Location, loc)
	return loc
}

func (p *profiler) addSample(skip int) {
	sample := &profile.Sample{
		Value: []int64{
			1,
		},
	}

	pcs, put := pcsPool.Get()
	defer put()
	pcs = pcs[:runtime.Callers(2+skip, pcs)]
	frames := runtime.CallersFrames(pcs)
	for {
		frame, more := frames.Next()
		if frame.Function == "" {
			continue
		}
		location := p.getLocation(frame)
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
)

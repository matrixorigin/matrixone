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
	profileChan <- state
	return func() {
		state := <-profileChan
		profiler := state.profilers[id]
		delete(state.profilers, id)
		profileChan <- state
		profiler.profile.Write(w)
	}
}

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
	state := <-profileChan
	for _, profiler := range state.profilers {
		profiler.addSample()
	}
	profileChan <- state
}

type profiler struct {
	profile        *profile.Profile
	functionIDs    map[uint64]bool
	nextLocationID uint64
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
		functionIDs: make(map[uint64]bool),
	}
}

func (p *profiler) getFunction(function *runtime.Func, pc uintptr) *profile.Function {
	file, line := function.FileLine(pc)
	id := uint64(function.Entry())
	fn := &profile.Function{
		ID:        id,
		Name:      function.Name(),
		Filename:  file,
		StartLine: int64(line),
	}
	if _, ok := p.functionIDs[id]; !ok {
		p.profile.Function = append(p.profile.Function, fn)
		p.functionIDs[id] = true
	}
	return fn
}

func (p *profiler) getLocation(frame runtime.Frame) *profile.Location {
	p.nextLocationID++
	loc := &profile.Location{
		ID: p.nextLocationID,
		Line: []profile.Line{
			{
				Function: p.getFunction(frame.Func, frame.PC),
				Line:     int64(frame.Line),
			},
		},
	}
	p.profile.Location = append(p.profile.Location, loc)
	return loc
}

func (p *profiler) addSample() {
	p.nextLocationID++
	sample := &profile.Sample{
		Value: []int64{
			1,
		},
	}

	pcs := make([]uintptr, 1024)
	pcs = pcs[:runtime.Callers(2, pcs)]
	frames := runtime.CallersFrames(pcs)
	for {
		frame, more := frames.Next()
		if frame.Func == nil {
			// inline
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

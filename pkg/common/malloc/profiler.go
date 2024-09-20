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
	"io"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/google/pprof/profile"
)

type Profiler[T any, P interface {
	*T
	SampleValues
}] struct {
	locations sync.Map // LocationKey -> *profile.Location
	functions sync.Map // FunctionKey -> *profile.Function
	samples   sync.Map // SampleKey -> *SampleInfo[P]

	// special samples
	stackOmittedSample SampleInfo[T]

	nextID atomic.Uint64
}

type SampleInfo[T any] struct {
	Values    T
	Locations []*profile.Location
	Scale     int64
}

type SampleValues interface {
	Init()
	SampleTypes() []*profile.ValueType
	DefaultSampleType() string
	Values() []int64
}

type LocationKey struct {
	File     string
	Line     int
	Function string
}

type FunctionKey struct {
	Name string
}

type SampleKey struct {
	PCs _PCs
}

func NewProfiler[T any, P interface {
	*T
	SampleValues
}]() *Profiler[T, P] {
	ret := &Profiler[T, P]{}

	ret.stackOmittedSample.Locations = []*profile.Location{
		ret.getMockLocation("| stack omitted |"),
	}
	P(&ret.stackOmittedSample.Values).Init()

	return ret
}

func (p *Profiler[T, P]) Sample(
	skip int,
	fullStackFraction uint32,
) P {

	if fullStackFraction > 0 &&
		fastrand()%fullStackFraction > 0 {
		// omit stack
		return &p.stackOmittedSample.Values
	}

	skip += 2 // runtime.Callers, p.Sample

	// full stack
	var pcs _PCs
	runtime.Callers(skip, pcs[:])
	return p.getSampleValueFromPCs(pcs, int64(fullStackFraction))
}

func (p *Profiler[T, P]) getLocation(frame runtime.Frame) *profile.Location {
	locationKey := LocationKey{
		File:     frame.File,
		Line:     frame.Line,
		Function: frame.Function,
	}
	if v, ok := p.locations.Load(locationKey); ok {
		return v.(*profile.Location)
	}

	location := &profile.Location{
		ID:      p.nextID.Add(1),
		Address: uint64(frame.PC),
	}

	line := profile.Line{
		Function: p.getFunction(frame),
		Line:     int64(frame.Line),
	}
	location.Line = []profile.Line{line}

	v, _ := p.locations.LoadOrStore(locationKey, location)
	return v.(*profile.Location)
}

func (p *Profiler[T, P]) getMockLocation(label string) *profile.Location {
	locationKey := LocationKey{
		Function: label,
	}
	if v, ok := p.locations.Load(locationKey); ok {
		return v.(*profile.Location)
	}

	location := &profile.Location{
		ID:      p.nextID.Add(1),
		Address: 0,
	}

	line := profile.Line{
		Function: p.getMockFunction(label),
	}
	location.Line = []profile.Line{line}

	v, _ := p.locations.LoadOrStore(locationKey, location)
	return v.(*profile.Location)
}

func (p *Profiler[T, P]) getFunction(frame runtime.Frame) *profile.Function {
	functionKey := FunctionKey{
		Name: frame.Function,
	}
	if v, ok := p.functions.Load(functionKey); ok {
		return v.(*profile.Function)
	}

	fn := &profile.Function{
		ID:         p.nextID.Add(1),
		Name:       frame.Function,
		SystemName: frame.Function,
		Filename:   frame.File,
	}

	v, _ := p.functions.LoadOrStore(functionKey, fn)
	return v.(*profile.Function)
}

func (p *Profiler[T, P]) getMockFunction(label string) *profile.Function {
	functionKey := FunctionKey{
		Name: label,
	}
	if v, ok := p.functions.Load(functionKey); ok {
		return v.(*profile.Function)
	}

	fn := &profile.Function{
		ID:         p.nextID.Add(1),
		Name:       label,
		SystemName: label,
		Filename:   label,
	}

	v, _ := p.functions.LoadOrStore(functionKey, fn)
	return v.(*profile.Function)
}

func (p *Profiler[T, P]) getSampleValueFromPCs(pcs _PCs, scale int64) P {
	key := SampleKey{
		PCs: pcs,
	}

	if v, ok := p.samples.Load(key); ok {
		return v.(*SampleInfo[P]).Values
	}

	var value T
	P(&value).Init()
	v, _ := p.samples.LoadOrStore(key, &SampleInfo[P]{
		Values:    &value,
		Locations: p.getLocationsFromPCs(pcs),
		Scale:     scale,
	})

	return v.(*SampleInfo[P]).Values
}

func (p *Profiler[T, P]) getLocationsFromPCs(pcs _PCs) []*profile.Location {
	var locations []*profile.Location
	n := 0
	for i := range pcs {
		if pcs[i] != 0 {
			n++
		} else {
			break
		}
	}
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()

		if frame.Function == "" {
			// unknown function, ignore
			continue
		}

		location := p.getLocation(frame)
		locations = append(locations, location)

		if !more {
			break
		}
	}

	return locations
}

func (p *Profiler[T, P]) Write(w io.Writer) error {

	var ptr P
	prof := &profile.Profile{
		SampleType:        ptr.SampleTypes(),
		DefaultSampleType: ptr.DefaultSampleType(),
	}

	p.samples.Range(func(k, v any) bool {
		info := v.(*SampleInfo[P])
		values := info.Values.Values()
		for i := range values {
			values[i] *= info.Scale
		}
		sample := &profile.Sample{
			Location: info.Locations,
			Value:    values,
		}
		prof.Sample = append(prof.Sample, sample)
		return true
	})

	p.locations.Range(func(k, v any) bool {
		location := v.(*profile.Location)
		prof.Location = append(prof.Location, copyLocation(location))
		return true
	})

	p.functions.Range(func(k, v any) bool {
		function := v.(*profile.Function)
		prof.Function = append(prof.Function, copyFunction(function))
		return true
	})

	return prof.Write(w)
}

func copyFunction(fn *profile.Function) *profile.Function {
	ret := *fn
	return &ret
}

func copyLocation(location *profile.Location) *profile.Location {
	ret := *location
	ret.Line = slices.Clone(location.Line)
	return &ret
}

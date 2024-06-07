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
	"hash/maphash"
	"io"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/google/pprof/profile"
)

type Profiler[T any, P interface {
	*T
	SampleValues
}] struct {
	locations sync.Map // LocationKey -> *profile.Location
	functions sync.Map // FunctionKey -> *profile.Function
	samples   sync.Map // SampleKey -> *SampleInfo[P]
}

type SampleInfo[T any] struct {
	Values    T
	Locations []*profile.Location
}

type SampleValues interface {
	SampleTypes() []*profile.ValueType
	DefaultSampleType() string
	Values() []int64
}

type LocationKey struct {
	PC       uintptr
	Function string
}

type FunctionKey struct {
	Name string
}

type SampleKey struct {
	Hash uint64
}

func NewProfiler[T any, P interface {
	*T
	SampleValues
}]() *Profiler[T, P] {
	return &Profiler[T, P]{}
}

func (p *Profiler[T, P]) Sample(
	skip int,
	fullStackFraction uint32,
) P {

	if fullStackFraction > 0 &&
		fastrand()%fullStackFraction > 0 {
		// omit stack
		return p.getSampleValue(
			p.getMockLocation("|stack omitted|"),
		)
	}

	// full stack
	return p.getSampleValue(
		p.getFullStackLocations(skip)...,
	)
}

func (p *Profiler[T, P]) getFullStackLocations(skip int) []*profile.Location {

	pcs := *pcsPool.Get().(*[]uintptr)
	defer func() {
		pcs = pcs[:cap(pcs)]
		pcsPool.Put(&pcs)
	}()

	for {
		n := runtime.Callers(3+skip, pcs)
		if n == len(pcs) {
			pcs = append(pcs, make([]uintptr, len(pcs))...)
			continue
		}
		pcs = pcs[:n]
		break
	}

	var locations []*profile.Location
	frames := runtime.CallersFrames(pcs)
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

func (p *Profiler[T, P]) getLocation(frame runtime.Frame) *profile.Location {
	locationKey := LocationKey{
		PC:       frame.PC,
		Function: frame.Function,
	}
	if v, ok := p.locations.Load(locationKey); ok {
		return v.(*profile.Location)
	}

	location := &profile.Location{
		ID:      nextID.Add(1),
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
		PC:       0,
		Function: label,
	}
	if v, ok := p.locations.Load(locationKey); ok {
		return v.(*profile.Location)
	}

	location := &profile.Location{
		ID:      nextID.Add(1),
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
		ID:         nextID.Add(1),
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
		ID:         nextID.Add(1),
		Name:       label,
		SystemName: label,
		Filename:   label,
	}

	v, _ := p.functions.LoadOrStore(functionKey, fn)
	return v.(*profile.Function)
}

func (p *Profiler[T, P]) getSampleValue(locations ...*profile.Location) P {
	hasher := hasherPool.Get().(*maphash.Hash)
	defer func() {
		hasher.Reset()
		hasherPool.Put(hasher)
	}()
	for _, loc := range locations {
		hasher.Write(
			unsafe.Slice((*byte)(unsafe.Pointer(&loc.ID)), unsafe.Sizeof(loc.ID)),
		)
	}
	key := SampleKey{
		Hash: hasher.Sum64(),
	}

	if v, ok := p.samples.Load(key); ok {
		return v.(*SampleInfo[P]).Values
	}

	var value T
	v, _ := p.samples.LoadOrStore(key, &SampleInfo[P]{
		Values:    &value,
		Locations: locations,
	})

	return v.(*SampleInfo[P]).Values
}

func (p *Profiler[T, P]) Write(w io.Writer) error {

	var ptr P
	prof := &profile.Profile{
		SampleType:        ptr.SampleTypes(),
		DefaultSampleType: ptr.DefaultSampleType(),
	}

	p.samples.Range(func(k, v any) bool {
		info := v.(*SampleInfo[P])
		sample := &profile.Sample{
			Location: info.Locations,
			Value:    info.Values.Values(),
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

var (
	nextID = new(atomic.Uint64)
)

var pcsPool = sync.Pool{
	New: func() any {
		slice := make([]uintptr, 128)
		return &slice
	},
}

var hasherPool = sync.Pool{
	New: func() any {
		return new(maphash.Hash)
	},
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

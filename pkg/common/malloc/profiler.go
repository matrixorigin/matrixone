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
	SampleValues[P]
}] struct {

	// pcs -> locations -> samples
	pcsToSample       sync.Map // _PCs -> *SampleInfo[P], caching, may be flush
	locationsToSample sync.Map // hashsum([]*Locations) -> *SampleInfo[P]

	locations sync.Map // LocationKey -> *profile.Location
	functions sync.Map // FunctionKey -> *profile.Function

	// special samples
	stackOmittedSample SampleInfo[T]

	nextID atomic.Uint64
}

type SampleInfo[T any] struct {
	Values    T
	Locations []*profile.Location
}

type SampleValues[T any] interface {
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

func NewProfiler[T any, P interface {
	*T
	SampleValues[P]
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
	// get from cache
	if v, ok := p.pcsToSample.Load(pcs); ok {
		return v.(*SampleInfo[P]).Values
	}

	// get from locations
	locations := p.getLocationsFromPCs(pcs)
	locationsHashSum := hashLocations(locations)
	if v, ok := p.locationsToSample.Load(locationsHashSum); ok {
		// set cache
		p.setPCsToSample(pcs, v.(*SampleInfo[P]))
		return v.(*SampleInfo[P]).Values
	}

	// create new sample
	var value T
	P(&value).Init()
	v, _ := p.locationsToSample.LoadOrStore(locationsHashSum, &SampleInfo[P]{
		Values:    &value,
		Locations: locations,
	})
	// set cache
	p.setPCsToSample(pcs, v.(*SampleInfo[P]))

	return v.(*SampleInfo[P]).Values
}

func (p *Profiler[T, P]) setPCsToSample(pcs _PCs, info *SampleInfo[P]) {
	// flush
	if fastrand()%1024 == 0 {
		p.pcsToSample.Clear()
	}
	p.pcsToSample.Store(pcs, info)
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

var hashSeed = maphash.MakeSeed()

var hasherPool = sync.Pool{
	New: func() any {
		hasher := new(maphash.Hash)
		hasher.SetSeed(hashSeed)
		return hasher
	},
}

func hashLocations(locations []*profile.Location) uint64 {
	hasher := hasherPool.Get().(*maphash.Hash)
	defer func() {
		hasher.Reset()
		hasherPool.Put(hasher)
	}()

	for _, location := range locations {
		hasher.Write(
			unsafe.Slice(
				(*byte)(unsafe.Pointer(&location.ID)),
				unsafe.Sizeof(location.ID),
			),
		)
	}

	return hasher.Sum64()
}

func (p *Profiler[T, P]) Write(w io.Writer) error {

	var ptr P
	prof := &profile.Profile{
		SampleType:        ptr.SampleTypes(),
		DefaultSampleType: ptr.DefaultSampleType(),
	}

	prof.Sample = append(prof.Sample, &profile.Sample{
		Location: p.stackOmittedSample.Locations,
		Value:    P(&p.stackOmittedSample.Values).Values(),
	})

	p.locationsToSample.Range(func(k, v any) bool {
		info := v.(*SampleInfo[P])
		values := info.Values.Values()
		prof.Sample = append(prof.Sample, &profile.Sample{
			Location: info.Locations,
			Value:    values,
		})
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

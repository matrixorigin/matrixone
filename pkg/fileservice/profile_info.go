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
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/google/pprof/profile"
)

type ProfileInfo struct {
	sync.Mutex
	functions    map[string]*profile.Function
	tagFunctions map[string]*profile.Function
	nextID       atomic.Uint64
}

func NewProfileInfo() *ProfileInfo {
	return &ProfileInfo{
		functions:    make(map[string]*profile.Function),
		tagFunctions: make(map[string]*profile.Function),
	}
}

func (p *ProfileInfo) getFunction(prof *profile.Profile, frame runtime.Frame) *profile.Function {
	p.Lock()
	defer p.Unlock()
	if fn, ok := p.functions[frame.Function]; ok {
		return fn
	}
	fn := &profile.Function{
		ID:         p.nextID.Add(1),
		Name:       frame.Function,
		SystemName: frame.Function,
		Filename:   frame.File,
		StartLine:  int64(frame.Line),
	}
	if frame.Func != nil {
		file, line := frame.Func.FileLine(frame.Entry)
		fn.Filename = file
		fn.StartLine = int64(line)
	}
	prof.Function = append(prof.Function, fn)
	p.functions[frame.Function] = fn
	return fn
}

func (p *ProfileInfo) getTagFunction(prof *profile.Profile, tag string) *profile.Function {
	p.Lock()
	defer p.Unlock()
	if fn, ok := p.tagFunctions[tag]; ok {
		return fn
	}
	fn := &profile.Function{
		ID:         p.nextID.Add(1),
		Name:       tag,
		SystemName: tag,
	}
	prof.Function = append(prof.Function, fn)
	p.tagFunctions[tag] = fn
	return fn
}

func (p *ProfileInfo) getLocation(prof *profile.Profile, frame runtime.Frame) *profile.Location {
	line := profile.Line{
		Function: p.getFunction(prof, frame),
		Line:     int64(frame.Line),
	}
	loc := &profile.Location{
		ID:      p.nextID.Add(1),
		Address: uint64(frame.PC),
		Line:    []profile.Line{line},
	}
	prof.Location = append(prof.Location, loc)
	return loc
}

func (p *ProfileInfo) getTagLocation(prof *profile.Profile, tag string) *profile.Location {
	loc := &profile.Location{
		ID: p.nextID.Add(1),
		Line: []profile.Line{
			{
				Function: p.getTagFunction(prof, tag),
			},
		},
	}
	prof.Location = append(prof.Location, loc)
	return loc
}

// Copyright 2021 - 2024 Matrix Origin
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

package mpool

import (
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/google/pprof/profile"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

type profileSampleValues struct {
	Allocate malloc.ShardedCounter[int64, atomic.Int64, *atomic.Int64]
	Free     malloc.ShardedCounter[int64, atomic.Int64, *atomic.Int64]
	Active   malloc.ShardedCounter[int64, atomic.Int64, *atomic.Int64]

	MissingFree  malloc.ShardedCounter[int64, atomic.Int64, *atomic.Int64]
	DoubleFree   malloc.ShardedCounter[int64, atomic.Int64, *atomic.Int64]
	UseAfterFree malloc.ShardedCounter[int64, atomic.Int64, *atomic.Int64]
}

var _ malloc.SampleValues = new(profileSampleValues)

func (p *profileSampleValues) DefaultSampleType() string {
	return "active"
}

func (p *profileSampleValues) Init() {
	p.Allocate = *malloc.NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))
	p.Free = *malloc.NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))
	p.Active = *malloc.NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))

	p.MissingFree = *malloc.NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))
	p.DoubleFree = *malloc.NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))
	p.UseAfterFree = *malloc.NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))
}

func (p *profileSampleValues) SampleTypes() []*profile.ValueType {
	return []*profile.ValueType{
		{
			Type: "allocate",
			Unit: "",
		},
		{
			Type: "free",
			Unit: "",
		},
		{
			Type: "active",
			Unit: "",
		},
		{
			Type: "missing free",
			Unit: "",
		},
		{
			Type: "double free",
			Unit: "",
		},
		{
			Type: "use after free",
			Unit: "",
		},
	}
}

func (p *profileSampleValues) Values() []int64 {
	return []int64{
		p.Allocate.Load(),
		p.Free.Load(),
		p.Active.Load(),
		p.MissingFree.Load(),
		p.DoubleFree.Load(),
		p.UseAfterFree.Load(),
	}
}

var profiler = malloc.NewProfiler[profileSampleValues]()

func init() {
	http.Handle("/debug/mpool/", profiler)
}

var (
	profileValuesMap       sync.Map
	fadingProfileValuesMap sync.Map // for detecting double free
)

var nextProfileValuesKey atomic.Int64

func getProfileValue() (key int64, values *profileSampleValues) {
	key = nextProfileValuesKey.Add(1)
	values = profiler.Sample(1, 1)
	profileValuesMap.Store(key, values)
	return
}

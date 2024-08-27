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

package fileservice

import (
	"net/http"
	"sync/atomic"

	"github.com/google/pprof/profile"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

var globalProfiler = malloc.NewProfiler[profileSamples]()

func init() {
	http.HandleFunc("/debug/fs/", func(w http.ResponseWriter, req *http.Request) {
		if err := globalProfiler.Write(w); err != nil {
			http.Error(w, err.Error(), 500)
		}
	})
}

type profileSamples struct {
	Read  atomic.Int64
	Write atomic.Int64
}

var _ malloc.SampleValues = new(profileSamples)

func (p *profileSamples) DefaultSampleType() string {
	return "read"
}

func (p *profileSamples) Init() {
}

func (p *profileSamples) SampleTypes() []*profile.ValueType {
	return []*profile.ValueType{
		{
			Type: "read",
		},
		{
			Type: "write",
		},
	}
}

func (p *profileSamples) Values() []int64 {
	return []int64{
		p.Read.Load(),
		p.Write.Load(),
	}
}

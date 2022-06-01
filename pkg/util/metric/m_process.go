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

package metric

import (
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// ProcessCollector collect following information about the current MO process:
//
// - CPUTime (Sys + User) in seconds
// - open fds & max fds
// - virtual_mem_bytes、virtual_max_mem_bytes、resident_mem_bytes
// - process up time in seconds
var ProcessCollector = newProcessCollector()

func newProcessCollector() Collector {
	c := &processCollector{
		Collector: collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	}
	c.init(c)
	return c
}

type processCollector struct {
	selfAsPromCollector
	prom.Collector
}

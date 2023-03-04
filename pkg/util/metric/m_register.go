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

import prom "github.com/prometheus/client_golang/prometheus"

const constTenantKey = "account"

// this constant lable is used for sys_* and process_* table
var sysTenantID = prom.Labels{constTenantKey: "sys"}

var InitCollectors = []Collector{
	// sql metric
	StatementCounterFactory,
	StatementErrorsFactory,
	TransactionCounterFactory,
	TransactionErrorsFactory,
	// server metric
	ConnFactory,
	StorageUsageFactory,
	// process metric
	processCollector,
	// sys metric
	hardwareStatsCollector,
}

type SubSystem struct {
	Name              string
	Comment           string
	SupportUserAccess bool
}

var AllSubSystem = map[string]*SubSystem{}

func registerSubSystem(s *SubSystem) {
	AllSubSystem[s.Name] = s
}

func init() {
	registerSubSystem(&SubSystem{"sql", "base on query action", true})
	registerSubSystem(&SubSystem{"server", "MO Server status, observe from inside", true})
	registerSubSystem(&SubSystem{"process", "MO process status", false})
	registerSubSystem(&SubSystem{"sys", "OS status", false})
}

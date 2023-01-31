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

var (
	ConnFactory = NewGaugeVec(
		GaugeOpts{
			Subsystem: "server",
			Name:      "connections",
			Help:      "Number of process connections",
		},
		[]string{constTenantKey},
	)

	StorageUsageFactory = NewGaugeVec(
		GaugeOpts{
			Subsystem: "server",
			Name:      "storage_usage",
			Help:      "Storage usage of each account",
		},
		[]string{constTenantKey},
	)
)

func ConnectionCounter(account string) Gauge {
	return ConnFactory.WithLabelValues(account)
}

func StorageUsage(account string) Gauge {
	return StorageUsageFactory.WithLabelValues(account)
}

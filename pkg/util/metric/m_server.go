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

import "strconv"

var (
	ConnFactory = NewGaugeVec(
		GaugeOpts{
			Subsystem: "server",
			Name:      "connections",
			Help:      "Number of process connections",
		},
		[]string{constTenantKey, constTenantIdKey},
	)

	StorageUsageFactory = NewGaugeVec(
		GaugeOpts{
			Subsystem: "server",
			Name:      "storage_usage",
			Help:      "Storage usage of each account",
		},
		[]string{constTenantKey, constTenantIdKey},
	)

	ObjectCountFactory = NewGaugeVec(
		GaugeOpts{
			Subsystem: "server",
			Name:      "object_count",
			Help:      "object number of each account",
		},
		[]string{constTenantKey, constTenantIdKey},
	)

	SnapshotUsageFactory = NewGaugeVec(
		GaugeOpts{
			Subsystem: "server",
			Name:      "snapshot_usage",
			Help:      "Snapshot usage of each account",
		},
		[]string{constTenantKey, constTenantIdKey},
	)
)

func ConnectionCounter(account string, accountId uint32) Gauge {
	return ConnFactory.WithLabelValues(account, strconv.FormatUint(uint64(accountId), 10))
}

func StorageUsage(account string, accountId uint32) Gauge {
	return StorageUsageFactory.WithLabelValues(account, strconv.FormatUint(uint64(accountId), 10))
}

func ObjectCount(account string, accountId uint32) Gauge {
	return ObjectCountFactory.WithLabelValues(account, strconv.FormatUint(uint64(accountId), 10))
}

func SnapshotUsage(account string, accountId uint32) Gauge {
	return SnapshotUsageFactory.WithLabelValues(account, strconv.FormatUint(uint64(accountId), 10))
}

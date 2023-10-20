// Copyright 2023 Matrix Origin
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

package v2

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	S3FSCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "s3_total",
			Help:      "Total number of s3 fs io handled.",
		}, []string{"type"})

	LocalFSCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "local_total",
			Help:      "Total number of local fs io handled.",
		}, []string{"type"})

	MemFSCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "mem_fs_total",
			Help:      "Total number of mem fs io handled.",
		}, []string{"type"})
)

func GetS3FSReadCounter() prometheus.Counter {
	return S3FSCounter.WithLabelValues("read")
}

func GetS3FSWriteCounter() prometheus.Counter {
	return S3FSCounter.WithLabelValues("write")
}

func GetLocalFSReadCounter() prometheus.Counter {
	return LocalFSCounter.WithLabelValues("read")
}

func GetLocalFSWriteCounter() prometheus.Counter {
	return LocalFSCounter.WithLabelValues("write")
}

func GetMemFSReadCounter() prometheus.Counter {
	return MemFSCounter.WithLabelValues("read")
}

func GetMemFSWriteCounter() prometheus.Counter {
	return MemFSCounter.WithLabelValues("write")
}

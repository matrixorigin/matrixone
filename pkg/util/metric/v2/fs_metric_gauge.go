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
	S3IOSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "s3_io_bytes",
			Help:      "Size of s3 io size.",
		}, []string{"type"})

	LocalIOSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "local_io_bytes",
			Help:      "Size of local io size.",
		}, []string{"type"})
)

func GetS3FSWriteSizeGauge() prometheus.Gauge {
	return S3IOSizeGauge.WithLabelValues("write")
}

func GetS3FSReadSizeGauge() prometheus.Gauge {
	return S3IOSizeGauge.WithLabelValues("read")
}

func GetLocalFSWriteSizeGauge() prometheus.Gauge {
	return LocalIOSizeGauge.WithLabelValues("write")
}

func GetLocalFSReadSizeGauge() prometheus.Gauge {
	return LocalIOSizeGauge.WithLabelValues("read")
}

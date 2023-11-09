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

import "github.com/prometheus/client_golang/prometheus"

var (
	rowReadHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pipeline",
			Subsystem: "reader",
			Name:      "rows_read",
			Help:      "the count of the rows read by the reader",
			Buckets:   prometheus.ExponentialBuckets(10, 2.0, 20),
		},
		[]string{"database", "table"},
	)
	bytesReadHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pipeline",
			Subsystem: "reader",
			Name:      "bytes_read",
			Help:      "the count of the bytes read by the reader",
			Buckets:   prometheus.ExponentialBuckets(10, 2.0, 20),
		},
		[]string{"database", "table"},
	)
)

func GetRowsReadHistogram(db, table string) prometheus.Observer {
	return rowReadHistogram.WithLabelValues(db, table)
}

func GetBytesReadHistogram(db, table string) prometheus.Observer {
	return bytesReadHistogram.WithLabelValues(db, table)
}

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

package metrics

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/util/export/observability"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util/export"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"

	"github.com/prometheus/common/model"
)

type Metric struct {
	Name      string
	Timestamp time.Time
	Value     float64
	Labels    model.Metric
	SeriesId  string
}

var metricPool = &sync.Pool{New: func() any {
	return &Metric{}
}}

func NewMetric() *Metric {
	return metricPool.Get().(*Metric)
}

func (m *Metric) GetName() string {
	return observability.MetricTable.GetIdentify()
}

func (m *Metric) GetRow() *table.Row {
	return observability.MetricTable.GetRow(context.Background())
}

func (m *Metric) CsvFields(ctx context.Context, row *table.Row) []string {
	row.Reset()
	row.SetColumnVal(observability.MetricNameColumn, m.Name)
	row.SetColumnVal(observability.MetricTimestampColumn, observability.Time2DatetimeString(m.Timestamp))
	row.SetFloat64(observability.MetricValueColumn.Name, m.Value)

	labels, err := json.Marshal(&m.Labels)
	if err != nil {
		panic(err)
	}
	row.SetColumnVal(observability.MetricLabelsColumn, string(labels))
	// calculate md5
	hash := md5.New()
	if _, err := hash.Write(export.String2Bytes(m.Name)); err != nil {
		panic(err)
	}
	if _, err := hash.Write(labels); err != nil {
		panic(err)
	}
	hashed := hash.Sum(nil)
	m.SeriesId = hex.EncodeToString(hashed)
	row.SetColumnVal(observability.MetricSeriesIDColumn, m.SeriesId)
	return row.ToStrings()
}

func (m *Metric) Size() int64 {
	return int64(unsafe.Sizeof(m)) + int64(
		len(m.Name)+len(m.SeriesId),
	)
}

func (m *Metric) Free() {
	m.Name = ""
	m.Timestamp = time.Time{}
	m.Value = 0.0
	m.Labels = nil
	m.SeriesId = ""
	metricPool.Put(m)
}

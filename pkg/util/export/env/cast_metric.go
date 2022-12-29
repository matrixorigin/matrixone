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

package env

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"
)

type Metric struct {
	Name      string
	Timestamp time.Time
	Value     float64
	Labels    model.Metric
	SeriesId  string
}

func (m *Metric) GetName() string {
	return MetricTable.GetIdentify()
}

func (m *Metric) GetRow() *table.Row {
	return MetricTable.GetRow(context.Background())
}

func (m *Metric) CsvFields(ctx context.Context, row *table.Row) []string {
	row.Reset()
	row.SetColumnVal(MetricNameColumn, m.Name)
	row.SetColumnVal(MetricTimestampColumn, Time2DatetimeString(m.Timestamp))
	row.SetFloat64(MetricValueColumn.Name, m.Value)
	labels := fmt.Sprintf("%s", m.Labels)
	row.SetColumnVal(MetricLabelsColumn, labels)
	// calculate md5
	hash := md5.New()
	if _, err := hash.Write([]byte(m.Name)); err != nil {
		panic(err)
	}
	if _, err := hash.Write([]byte(labels)); err != nil {
		panic(err)
	}
	hashed := hash.Sum(nil)
	m.SeriesId = hex.EncodeToString(hashed)
	row.SetColumnVal(MetricSeriesIDColumn, m.SeriesId)
	return row.ToStrings()
}

func TransferMetrics(data []prompb.TimeSeries) []Metric {
	if len(data) == 0 {
		return nil
	}
	arr := make([]Metric, 0, len(data)*len(data[0].Samples))
	for _, ts := range data {
		metric := Metric{}
		m := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			if l.Name == model.MetricNameLabel {
				metric.Name = l.Value
				continue
			}
			m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			mm := metric
			mm.Value = s.Value
			mm.Timestamp = timestamp.Time(s.Timestamp)
			mm.Labels = m
			arr = append(arr, mm)
		}

		for _, e := range ts.Exemplars {
			metric = Metric{}
			m := make(model.Metric, len(e.Labels))
			for _, l := range e.Labels {
				if l.Name == model.MetricNameLabel {
					metric.Name = l.Value
					continue
				}
				m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
			}
			metric.Value = e.Value
			metric.Timestamp = timestamp.Time(e.Timestamp)
			metric.Labels = m
			arr = append(arr, metric)
		}

		ts.Histograms = nil // not support
	}
	return arr
}

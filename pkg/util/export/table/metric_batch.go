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

package table

import (
	"encoding/base64"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
)

const metricBatchSuffix = "-batch-"
const compactMetricBatchPrefix = "!"
const sequencedMetricBatchPrefix = "@"

// MetricBatchLogPath stores the whole UUID in a 27-byte filename. The short
// component leaves room for the object-store prefix and the disk-cache suffix.
func MetricBatchLogPath(account string, tbl *Table, ts time.Time, id uuid.UUID) string {
	return metricBatchLogPath(account, tbl, ts, id, compactMetricBatchPrefix)
}

func SequencedMetricBatchLogPath(account string, tbl *Table, ts time.Time, id uuid.UUID) string {
	return metricBatchLogPath(account, tbl, ts, id, sequencedMetricBatchPrefix)
}

func metricBatchLogPath(account string, tbl *Table, ts time.Time, id uuid.UUID, prefix string) string {
	dir := tbl.PathBuilder.Build(account, MergeLogTypeLogs, ts, tbl.Database, tbl.Table)
	name := prefix + base64.RawURLEncoding.EncodeToString(id[:]) + CsvExtension
	return path.Join(dir, name)
}

func compactMetricBatchID(name string) (uuid.UUID, bool) {
	if len(name) == 0 || (name[:1] != compactMetricBatchPrefix && name[:1] != sequencedMetricBatchPrefix) {
		return uuid.Nil, false
	}
	encoded := name[1:]
	if len(encoded) != base64.RawURLEncoding.EncodedLen(len(uuid.Nil)) {
		return uuid.Nil, false
	}
	var id uuid.UUID
	n, err := base64.RawURLEncoding.Strict().Decode(id[:], []byte(encoded))
	return id, err == nil && n == len(id) && id != uuid.Nil && base64.RawURLEncoding.EncodeToString(id[:]) == encoded
}

func (tbl *Table) UsesMetricBatchReceipt() bool {
	return tbl.Database == "system_metrics" && (tbl.Table == "metric" || tbl.Table == "sql_statement_cu")
}

// MetricBatchPath attaches a batch ID to a verified undelivered legacy CSV.
func MetricBatchPath(filePath string, id uuid.UUID) string {
	return strings.TrimSuffix(filePath, CsvExtension) + metricBatchSuffix + id.String() + CsvExtension
}

func MetricBatchID(filePath string) (uuid.UUID, bool) {
	name, ok := strings.CutSuffix(path.Base(filePath), CsvExtension)
	if !ok {
		return uuid.Nil, false
	}
	if id, ok := compactMetricBatchID(name); ok {
		return id, true
	}
	index := strings.LastIndex(name, metricBatchSuffix)
	if index < 0 {
		return uuid.Nil, false
	}
	suffix := name[index+len(metricBatchSuffix):]
	id, err := uuid.Parse(suffix)
	return id, err == nil && id != uuid.Nil && suffix == id.String()
}

func IsSequencedMetricBatch(filePath string) bool {
	return strings.HasPrefix(path.Base(filePath), sequencedMetricBatchPrefix)
}

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

// MetricBatchLogPath stores the whole UUID in a 27-byte filename. The short
// component leaves room for the object-store prefix and the disk-cache suffix.
func MetricBatchLogPath(account string, tbl *Table, ts time.Time, id uuid.UUID) string {
	dir := tbl.PathBuilder.Build(account, MergeLogTypeLogs, ts, tbl.Database, tbl.Table)
	name := compactMetricBatchPrefix + base64.RawURLEncoding.EncodeToString(id[:]) + CsvExtension
	return path.Join(dir, name)
}

func compactMetricBatchID(name string) (uuid.UUID, bool) {
	encoded, ok := strings.CutPrefix(name, compactMetricBatchPrefix)
	if !ok || len(encoded) != base64.RawURLEncoding.EncodedLen(len(uuid.Nil)) {
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
	if strings.HasPrefix(name, compactMetricBatchPrefix) {
		return compactMetricBatchID(name)
	}
	index := strings.LastIndex(name, metricBatchSuffix)
	if index < 0 {
		return uuid.Nil, false
	}
	suffix := name[index+len(metricBatchSuffix):]
	id, err := uuid.Parse(suffix)
	return id, err == nil && id != uuid.Nil && suffix == id.String()
}

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
	"path"
	"strings"

	"github.com/google/uuid"
)

const metricBatchSuffix = "-batch-"

func (tbl *Table) UsesMetricBatchReceipt() bool {
	return tbl.Database == "system_metrics" && (tbl.Table == "metric" || tbl.Table == "sql_statement_cu")
}

// MetricBatchPath keeps the existing four-part log filename and CSV body.
// Older filenames have no batch identity and cannot prove the first write.
func MetricBatchPath(filePath string, id uuid.UUID) string {
	return strings.TrimSuffix(filePath, CsvExtension) + metricBatchSuffix + id.String() + CsvExtension
}

func MetricBatchID(filePath string) (uuid.UUID, bool) {
	name, ok := strings.CutSuffix(path.Base(filePath), CsvExtension)
	if !ok {
		return uuid.Nil, false
	}
	index := strings.LastIndex(name, metricBatchSuffix)
	if index < 0 {
		return uuid.Nil, false
	}
	suffix := name[index+len(metricBatchSuffix):]
	id, err := uuid.Parse(suffix)
	return id, err == nil && id != uuid.Nil && suffix == id.String()
}

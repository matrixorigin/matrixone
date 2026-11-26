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
	"crypto/sha256"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
)

const metricBatchSuffix = "-batch-"
const maxMetricFilenameBytes = 255

func (c FilePathCfg) MetricBatchLogPath(account string, tbl *Table, ts time.Time, id uuid.UUID) string {
	filePath := MetricBatchPath(c.LogsFilePathFactory(account, tbl, ts), id)
	if len(path.Base(filePath)) <= maxMetricFilenameBytes {
		return filePath
	}
	// Local ETL files must fit in one filesystem component. Hash only the
	// node portion so the log format and the entire batch UUID survive.
	c.NodeUUID = fmt.Sprintf("%x", sha256.Sum256([]byte(c.NodeUUID)))
	return MetricBatchPath(c.LogsFilePathFactory(account, tbl, ts), id)
}

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

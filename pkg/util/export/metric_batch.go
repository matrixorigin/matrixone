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

package export

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"time"
	"unicode/utf8"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var errMetricFileRetained = moerr.NewInvalidStateNoCtx("metric file retained for reconciliation")

func (m *Merge) mergeMetricFile(ctx context.Context, filePath string) error {
	id, ok := table.MetricBatchID(filePath)
	if !ok {
		// The old format carries no evidence about the first write.
		m.logger.Warn("retain metric file without batch identity", logutil.PathField(filePath))
		return errMetricFileRetained
	}
	vec := fileservice.IOVector{
		FilePath: filePath,
		Entries:  []fileservice.IOEntry{{Size: -1}},
	}
	defer vec.Release()
	if err := m.fs.Read(ctx, &vec); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil
		}
		v2.TraceETLMergeOpenFailedCounter.Inc()
		return err
	}

	payload := vec.Entries[0].Data
	if err := validateMetricCSV(payload, m.table); err != nil {
		v2.TraceETLMergeParseFailedCounter.Inc()
		m.logger.Warn("retain incompatible metric file", logutil.PathField(filePath), zap.Error(err))
		return errMetricFileRetained
	}
	if err := etl.ReplayMetricBatch(ctx, m.table, payload, id); err != nil {
		v2.TraceETLMergeWriteFailedCounter.Inc()
		return err
	}
	if err := m.fs.Delete(ctx, filePath); err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		v2.TraceETLMergeDeleteFailedCounter.Inc()
		return err
	}
	v2.TraceETLMergeSuccessCounter.Inc()
	return nil
}

func validateMetricCSV(payload []byte, tbl *table.Table) error {
	reader := csv.NewReader(bytes.NewReader(payload))
	reader.FieldsPerRecord = len(tbl.Columns)
	reader.ReuseRecord = true
	hasRows := false
	for {
		fields, err := reader.Read()
		if errors.Is(err, io.EOF) {
			if !hasRows {
				return moerr.NewInvalidInputNoCtx("empty metric CSV")
			}
			return nil
		}
		if err != nil {
			return err
		}
		hasRows = true
		for idx, column := range tbl.Columns {
			switch column.ColType {
			case table.TDatetime:
				_, err = time.Parse("2006-01-02 15:04:05.000000", fields[idx])
			case table.TUint32:
				_, err = strconv.ParseUint(fields[idx], 10, 32)
			case table.TFloat64:
				_, err = strconv.ParseFloat(fields[idx], 64)
			case table.TVarchar:
				if !utf8.ValidString(fields[idx]) || utf8.RuneCountInString(fields[idx]) > column.Scale {
					return moerr.NewInvalidInputNoCtxf("invalid metric CSV field %s", column.Name)
				}
			}
			if err != nil {
				// Do not include the row value in the diagnostic.
				return moerr.NewInvalidInputNoCtxf("invalid metric CSV field %s", column.Name)
			}
		}
	}
}

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
	"encoding/binary"
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	dbholder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var errMetricFileRetained = moerr.NewInvalidStateNoCtx("metric file retained for reconciliation")

// Sequenced files contain a 16-byte publisher UUID and an 8-byte sequence,
// followed by the unchanged CSV payload. Only actual publications consume
// sequence numbers, so acknowledged SQL writes leave no holes.
const metricFileHeaderSize = len(uuid.Nil) + 8

type metricPublisher struct {
	id   uuid.UUID
	next atomic.Int64
}

type metricFileWriter struct {
	ctx       context.Context
	fs        fileservice.FileService
	path      string
	publisher *metricPublisher
	sequence  int64
}

func (w *metricFileWriter) WriteString(payload string) (int, error) {
	if w.sequence == 0 {
		w.sequence = w.publisher.next.Add(1)
	}
	var header [metricFileHeaderSize]byte
	copy(header[:], w.publisher.id[:])
	binary.BigEndian.PutUint64(header[len(w.publisher.id):], uint64(w.sequence))
	err := w.fs.Write(w.ctx, fileservice.IOVector{
		FilePath: w.path,
		Entries: []fileservice.IOEntry{
			{Size: int64(metricFileHeaderSize), Data: header[:]},
			{Offset: int64(metricFileHeaderSize), Size: int64(len(payload)), Data: util.UnsafeStringToBytes(payload)},
		},
	})
	return len(payload), err
}

func metricFilePublication(payload []byte) (dbholder.MetricFilePublication, bool) {
	var publication dbholder.MetricFilePublication
	if len(payload) < metricFileHeaderSize {
		return publication, false
	}
	copy(publication.Publisher[:], payload[:len(publication.Publisher)])
	publication.Sequence = int64(binary.BigEndian.Uint64(payload[len(publication.Publisher):metricFileHeaderSize]))
	return publication, publication.Publisher != uuid.Nil && publication.Sequence > 0
}

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
	var publication dbholder.MetricFilePublication
	if table.IsSequencedMetricBatch(filePath) {
		var valid bool
		publication, valid = metricFilePublication(payload)
		if !valid {
			v2.TraceETLMergeParseFailedCounter.Inc()
			m.logger.Warn("retain metric file with invalid publication header", logutil.PathField(filePath))
			return errMetricFileRetained
		}
		payload = payload[metricFileHeaderSize:]
	}
	if err := validateMetricCSV(ctx, payload, m.table); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		v2.TraceETLMergeParseFailedCounter.Inc()
		m.logger.Warn("retain incompatible metric file", logutil.PathField(filePath), zap.Error(err))
		return errMetricFileRetained
	}
	if err := etl.ReplayMetricFile(ctx, m.table, payload, id, publication); err != nil {
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

func validateMetricCSV(ctx context.Context, payload []byte, tbl *table.Table) error {
	reader := csv.NewReader(bytes.NewReader(payload))
	reader.FieldsPerRecord = len(tbl.Columns)
	reader.ReuseRecord = true
	hasRows := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
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

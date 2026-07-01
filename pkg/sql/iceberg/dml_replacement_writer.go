// Copyright 2026 Matrix Origin
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

package iceberg

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	icebergwrite "github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

type DMLReplacementDataFilesRequest struct {
	TableLocation       string
	Operation           dml.Operation
	Base                dml.CommitBase
	SnapshotID          int64
	Schema              api.Schema
	PartitionSpec       api.PartitionSpec
	Attrs               []string
	Batch               *batch.Batch
	TargetFileSizeBytes int64
	TimeZone            *time.Location
	OutputFactory       icebergwrite.DataFileOutputFactory
	ObjectWriter        dml.DeleteObjectWriter
	FileSequence        int
}

func WriteDMLReplacementDataFiles(ctx context.Context, req DMLReplacementDataFilesRequest) ([]api.DataFile, error) {
	tableLocation := strings.TrimRight(strings.TrimSpace(req.TableLocation), "/")
	if tableLocation == "" {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML replacement writer requires table location", map[string]string{
			"table": req.Base.Table,
		}))
	}
	if req.SnapshotID <= 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML replacement writer requires snapshot id", map[string]string{
			"table": req.Base.Table,
		}))
	}
	operation, err := normalizedDMLOperation(ctx, dmlOperationOrDefault(req.Operation, dml.OperationUpdate), req.Base.Table)
	if err != nil {
		return nil, err
	}
	statementKey := firstNonEmpty(req.Base.StatementID, req.Base.IdempotencyKey)
	if statementKey == "" {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML replacement writer requires statement id or idempotency key", map[string]string{
			"table": req.Base.Table,
		}))
	}
	factory := req.OutputFactory
	if factory == nil {
		if req.ObjectWriter == nil {
			return nil, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML replacement writer requires an output factory or object writer", map[string]string{
				"table": req.Base.Table,
			}))
		}
		factory = bufferedDataFileOutputFactory{Writer: req.ObjectWriter}
	}
	writerID := "replace-" + api.PathHash(statementKey)
	if req.FileSequence > 0 {
		writerID += "-" + strconv.Itoa(req.FileSequence)
	}
	writer, err := icebergwrite.NewFanoutParquetDataWriter(ctx, icebergwrite.FanoutWriterConfig{
		Schema:              req.Schema,
		PartitionSpec:       req.PartitionSpec,
		TableLocation:       tableLocation,
		DataDir:             dmlReplacementDataDir(operation, statementKey, req.SnapshotID),
		WriterID:            writerID,
		TargetFileSizeBytes: req.TargetFileSizeBytes,
		TimeZone:            req.TimeZone,
	}, factory)
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	if err := writer.WriteBatch(ctx, req.Attrs, req.Batch); err != nil {
		_ = writer.Abort(ctx)
		return nil, api.ToMOErr(ctx, err)
	}
	files, err := writer.Close(ctx)
	if err != nil {
		_ = writer.Abort(ctx)
		return nil, api.ToMOErr(ctx, err)
	}
	return files, nil
}

func dmlReplacementDataDir(operation, statementKey string, snapshotID int64) string {
	return joinDMLObjectPath("data", "mo-dml", operation, "stmt-"+api.PathHash(statementKey), "replacement", "snap-"+strconv.FormatInt(snapshotID, 10))
}

type bufferedDataFileOutputFactory struct {
	Writer dml.DeleteObjectWriter
}

func (f bufferedDataFileOutputFactory) CreateDataFile(ctx context.Context, location string) (io.WriteCloser, error) {
	if f.Writer == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg DML buffered data file factory requires object writer", nil)
	}
	return &bufferedDataFileObject{
		ctx:      ctx,
		location: location,
		writer:   f.Writer,
	}, nil
}

type bufferedDataFileObject struct {
	ctx      context.Context
	location string
	writer   dml.DeleteObjectWriter
	buf      bytes.Buffer
	closed   bool
}

func (o *bufferedDataFileObject) Write(p []byte) (int, error) {
	if o.closed {
		return 0, api.NewError(api.ErrObjectIO, "Iceberg DML buffered data file is already closed", map[string]string{
			"location": api.RedactPath(o.location),
		})
	}
	return o.buf.Write(p)
}

func (o *bufferedDataFileObject) Close() error {
	if o.closed {
		return nil
	}
	o.closed = true
	if o.writer == nil {
		return api.NewError(api.ErrConfigInvalid, "Iceberg DML buffered data file requires object writer", nil)
	}
	return o.writer.WriteObject(o.ctx, o.location, o.buf.Bytes())
}

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
	"context"
	"errors"
	"io"
	"math"
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
	MemoryBudget        *dmlMemoryBudget
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
		factory = bufferedDataFileOutputFactory{
			Writer:                req.ObjectWriter,
			MemoryBudget:          req.MemoryBudget,
			RetainedMetadataBytes: retainedDMLDataFileMetadataBytesForSchema(req.Schema, req.PartitionSpec),
		}
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
		return nil, api.ToMOErr(ctx, errors.Join(err, writer.Abort(ctx)))
	}
	files, err := writer.Close(ctx)
	if err != nil {
		return nil, api.ToMOErr(ctx, errors.Join(err, writer.Abort(ctx)))
	}
	return files, nil
}

func dmlReplacementDataDir(operation, statementKey string, snapshotID int64) string {
	return joinDMLObjectPath("data", "mo-dml", operation, "stmt-"+api.PathHash(statementKey), "replacement", "snap-"+strconv.FormatInt(snapshotID, 10))
}

type bufferedDataFileOutputFactory struct {
	Writer                dml.DeleteObjectWriter
	MemoryBudget          *dmlMemoryBudget
	RetainedMetadataBytes int64
}

const retainedDMLDataFileMetadataBytes int64 = 2048

func (f bufferedDataFileOutputFactory) CreateDataFile(ctx context.Context, location string) (io.WriteCloser, error) {
	if f.Writer == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg DML buffered data file factory requires object writer", nil)
	}
	// Closed files leave DataFile metadata (or an orphan path on failure) in the
	// coordinator until commit/abort. Reserve that retained state at creation so
	// tiny target-file settings cannot generate an unbounded file list.
	metadataBytes := f.RetainedMetadataBytes
	if metadataBytes <= 0 {
		metadataBytes = retainedDMLDataFileMetadataBytes
	}
	if err := f.MemoryBudget.reserve(ctx, metadataBytes); err != nil {
		return nil, err
	}
	return &bufferedDataFileObject{
		ctx:                 ctx,
		location:            location,
		writer:              f.Writer,
		budget:              f.MemoryBudget,
		metadataReservation: metadataBytes,
	}, nil
}

func retainedDMLDataFileMetadataBytesForSchema(schema api.Schema, spec api.PartitionSpec) int64 {
	// Each column may retain lower/upper bounds and several metric-map entries;
	// the file list is cloned across fanout, coordinator and commit request. The
	// writer caps each bound at 4 KiB, so 40 KiB per column covers those copies
	// plus map/header overhead. This may be tightened if those handoffs become
	// ownership transfers instead of defensive clones.
	perColumn := int64(40 << 10)
	columns := int64(len(schema.Fields))
	partitions := int64(len(spec.Fields))
	return saturatingDMLAdd(retainedDMLDataFileMetadataBytes,
		saturatingDMLAdd(saturatingDMLMul(columns, perColumn), saturatingDMLMul(partitions, 1024)))
}

type bufferedDataFileObject struct {
	ctx                 context.Context
	location            string
	writer              dml.DeleteObjectWriter
	data                []byte
	budget              *dmlMemoryBudget
	reserved            int64
	metadataReservation int64
	closed              bool
}

func (o *bufferedDataFileObject) Write(p []byte) (int, error) {
	if o.closed {
		return 0, api.NewError(api.ErrObjectIO, "Iceberg DML buffered data file is already closed", map[string]string{
			"location": api.RedactPath(o.location),
		})
	}
	if len(p) == 0 {
		return 0, nil
	}
	if len(o.data) > math.MaxInt-len(p) {
		return 0, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg DML buffered data file exceeds the addressable memory limit", nil)
	}
	required := len(o.data) + len(p)
	if required > cap(o.data) {
		oldCapacity := cap(o.data)
		nextCapacity := required
		if oldCapacity <= math.MaxInt/2 && oldCapacity*2 > nextCapacity {
			nextCapacity = oldCapacity * 2
		}
		if nextCapacity < 64<<10 {
			nextCapacity = 64 << 10
		}
		// make+copy keeps the old backing array live until the copy completes.
		// Reserve the full new capacity first, then release the old capacity;
		// accounting only the delta would understate the real growth peak by
		// almost 2x for a geometrically growing buffer.
		if err := o.budget.reserve(o.ctx, int64(nextCapacity)); err != nil {
			return 0, err
		}
		next := make([]byte, len(o.data), nextCapacity)
		copy(next, o.data)
		o.data = next
		o.budget.release(int64(oldCapacity))
		o.reserved = int64(nextCapacity)
	}
	start := len(o.data)
	o.data = o.data[:required]
	copy(o.data[start:], p)
	return len(p), nil
}

func (o *bufferedDataFileObject) Close() error {
	if o.closed {
		return nil
	}
	o.closed = true
	if o.writer == nil {
		o.releaseBuffer()
		o.releaseMetadataReservation()
		return api.NewError(api.ErrConfigInvalid, "Iceberg DML buffered data file requires object writer", nil)
	}
	// ObjectWriter is a byte-slice API and some FileService adapters must copy
	// the payload before returning. Reserve that second live copy explicitly;
	// streaming ObjectWriter support can remove this conservative allowance.
	copyBytes := int64(len(o.data))
	if err := o.budget.reserve(o.ctx, copyBytes); err != nil {
		o.releaseBuffer()
		return err
	}
	err := o.writer.WriteObject(o.ctx, o.location, o.data)
	o.budget.release(copyBytes)
	o.releaseBuffer()
	return err
}

func (o *bufferedDataFileObject) Abort() error {
	if o == nil || o.closed {
		return nil
	}
	o.closed = true
	o.releaseBuffer()
	o.releaseMetadataReservation()
	return nil
}

func (o *bufferedDataFileObject) releaseBuffer() {
	if o == nil {
		return
	}
	o.data = nil
	o.budget.release(o.reserved)
	o.reserved = 0
}

func (o *bufferedDataFileObject) releaseMetadataReservation() {
	if o == nil || o.metadataReservation <= 0 {
		return
	}
	o.budget.release(o.metadataReservation)
	o.metadataReservation = 0
}

var _ icebergwrite.DataFileOutputAborter = (*bufferedDataFileObject)(nil)

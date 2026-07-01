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

package write

import (
	"context"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const defaultTargetFileSizeBytes int64 = 128 * 1024 * 1024

type DataFileOutputFactory interface {
	CreateDataFile(ctx context.Context, location string) (io.WriteCloser, error)
}

type DataFileOutputFactoryFunc func(ctx context.Context, location string) (io.WriteCloser, error)

func (f DataFileOutputFactoryFunc) CreateDataFile(ctx context.Context, location string) (io.WriteCloser, error) {
	return f(ctx, location)
}

type FanoutWriterConfig struct {
	Schema              api.Schema
	PartitionSpec       api.PartitionSpec
	TableLocation       string
	DataDir             string
	WriterID            string
	TargetFileSizeBytes int64
	TimeZone            *time.Location
}

type FanoutParquetDataWriter struct {
	cfg     FanoutWriterConfig
	factory DataFileOutputFactory
	active  map[string]*activeDataFileWriter
	files   []api.DataFile
	seq     int64
	closed  bool
}

type activeDataFileWriter struct {
	location string
	sink     io.WriteCloser
	writer   *ParquetDataWriter
}

func NewFanoutParquetDataWriter(ctx context.Context, cfg FanoutWriterConfig, factory DataFileOutputFactory) (*FanoutParquetDataWriter, error) {
	if strings.TrimSpace(cfg.TableLocation) == "" {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg fanout writer requires a table location", nil)
	}
	if factory == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg fanout writer requires an output factory", nil)
	}
	if cfg.TargetFileSizeBytes <= 0 {
		cfg.TargetFileSizeBytes = defaultTargetFileSizeBytes
	}
	if cfg.TimeZone == nil {
		cfg.TimeZone = time.UTC
	}
	return &FanoutParquetDataWriter{
		cfg:     cfg,
		factory: factory,
		active:  make(map[string]*activeDataFileWriter),
	}, nil
}

func (w *FanoutParquetDataWriter) WriteBatch(ctx context.Context, attrs []string, bat *batch.Batch) error {
	if w == nil {
		return api.NewError(api.ErrConfigInvalid, "Iceberg fanout writer is not initialized", nil)
	}
	if w.closed {
		return api.NewError(api.ErrConfigInvalid, "Iceberg fanout writer is already closed", nil)
	}
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if len(attrs) != len(bat.Vecs) {
		return moerr.NewInvalidInputf(ctx, "Iceberg fanout writer attrs/vector mismatch: attrs=%d vectors=%d", len(attrs), len(bat.Vecs))
	}
	for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
		partition, err := partitionForBatchRow(ctx, w.cfg.Schema, w.cfg.PartitionSpec, attrs, bat, rowIdx, w.cfg.TimeZone)
		if err != nil {
			return err
		}
		key := partitionKey(partition, w.cfg.PartitionSpec)
		active, err := w.openForPartition(ctx, key, partition)
		if err != nil {
			return err
		}
		if err := active.writer.WriteRows(ctx, attrs, bat, []int{rowIdx}); err != nil {
			return err
		}
		if active.writer.ApproxSize() >= w.cfg.TargetFileSizeBytes {
			if err := w.closeActive(ctx, key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *FanoutParquetDataWriter) Close(ctx context.Context) ([]api.DataFile, error) {
	if w == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg fanout writer is not initialized", nil)
	}
	if w.closed {
		return cloneDataFiles(w.files), nil
	}
	keys := make([]string, 0, len(w.active))
	for key := range w.active {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := w.closeActive(ctx, key); err != nil {
			return nil, err
		}
	}
	w.closed = true
	return cloneDataFiles(w.files), nil
}

func (w *FanoutParquetDataWriter) Abort(ctx context.Context) error {
	if w == nil || w.closed {
		return nil
	}
	var firstErr error
	for key, active := range w.active {
		if err := active.sink.Close(); err != nil && firstErr == nil {
			firstErr = api.WrapError(api.ErrObjectIO, "Iceberg fanout writer failed to close aborted data file", map[string]string{"location": api.RedactPath(active.location)}, err)
		}
		delete(w.active, key)
	}
	w.closed = true
	return firstErr
}

func (w *FanoutParquetDataWriter) openForPartition(ctx context.Context, key string, partition map[string]any) (*activeDataFileWriter, error) {
	if active := w.active[key]; active != nil {
		return active, nil
	}
	partitionPath := partitionPathForSpec(partition, w.cfg.PartitionSpec)
	location := w.nextDataFileLocation(partitionPath)
	sink, err := w.factory.CreateDataFile(ctx, location)
	if err != nil {
		return nil, api.WrapError(api.ErrObjectIO, "Iceberg fanout writer failed to create data file", map[string]string{"location": api.RedactPath(location)}, err)
	}
	writer, err := NewParquetDataWriter(ctx, DataWriterConfig{
		Schema:        w.cfg.Schema,
		PartitionSpec: w.cfg.PartitionSpec,
		FilePath:      location,
		TimeZone:      w.cfg.TimeZone,
	}, sink)
	if err != nil {
		_ = sink.Close()
		return nil, err
	}
	active := &activeDataFileWriter{location: location, sink: sink, writer: writer}
	w.active[key] = active
	return active, nil
}

func (w *FanoutParquetDataWriter) closeActive(ctx context.Context, key string) error {
	active := w.active[key]
	if active == nil {
		return nil
	}
	file, writerErr := active.writer.Close(ctx)
	closeErr := active.sink.Close()
	delete(w.active, key)
	if writerErr != nil {
		return writerErr
	}
	if closeErr != nil {
		return api.WrapError(api.ErrObjectIO, "Iceberg fanout writer failed to close data file", map[string]string{"location": api.RedactPath(active.location)}, closeErr)
	}
	if file.RecordCount > 0 {
		w.files = append(w.files, file)
	}
	return nil
}

func (w *FanoutParquetDataWriter) nextDataFileLocation(partitionPath string) string {
	w.seq++
	dataDir := firstNonEmpty(w.cfg.DataDir, "data")
	fileName := "part-" + safePathToken(firstNonEmpty(w.cfg.WriterID, "mo")) + "-" + strconv.FormatInt(w.seq, 10) + ".parquet"
	parts := []string{dataDir}
	if partitionPath != "" {
		parts = append(parts, strings.Split(partitionPath, "/")...)
	}
	parts = append(parts, fileName)
	return joinObjectPath(w.cfg.TableLocation, parts...)
}

func partitionForBatchRow(ctx context.Context, schema api.Schema, spec api.PartitionSpec, attrs []string, bat *batch.Batch, rowIdx int, loc *time.Location) (map[string]any, error) {
	if len(spec.Fields) == 0 {
		return nil, nil
	}
	byName := make(map[string]api.SchemaField, len(schema.Fields))
	for _, field := range schema.Fields {
		byName[strings.ToLower(field.Name)] = field
	}
	rowValues := make(map[int]any, len(attrs))
	for colIdx, attr := range attrs {
		field, ok := byName[strings.ToLower(attr)]
		if !ok {
			continue
		}
		value, isNull, err := vectorValue(ctx, bat.Vecs[colIdx], rowIdx, field.Type, loc)
		if err != nil {
			return nil, err
		}
		if isNull {
			rowValues[field.ID] = nil
			continue
		}
		rowValues[field.ID] = value
	}
	return partitionTuple(ctx, spec, schema.Fields, rowValues)
}

func partitionPathForSpec(partition map[string]any, spec api.PartitionSpec) string {
	if len(partition) == 0 {
		return ""
	}
	parts := make([]string, 0, len(spec.Fields))
	for _, field := range spec.Fields {
		value, ok := partition[field.Name]
		if !ok {
			continue
		}
		parts = append(parts, safePathToken(field.Name)+"="+safePathToken(partitionValueString(value)))
	}
	return strings.Join(parts, "/")
}

func joinObjectPath(base string, parts ...string) string {
	out := strings.TrimRight(strings.TrimSpace(base), "/")
	for _, part := range parts {
		part = strings.Trim(part, "/")
		if part == "" {
			continue
		}
		out += "/" + part
	}
	return out
}

func safePathToken(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "_"
	}
	return url.PathEscape(value)
}

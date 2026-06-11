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

// Package externalwrite implements writing rows of a query/LOAD into the
// backing files of a writable external table. A table becomes writable when it
// is created with a WRITE_FILE_PATTERN option; each parallel write pipeline
// owns one ExternalWriter and produces exactly one output file.
package externalwrite

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/stage/stageutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	FormatCSV      = "csv"
	FormatJSONLine = "jsonline"
)

// WriterConfig describes how one external-table write pipeline should encode and
// place its output file.
type WriterConfig struct {
	// Pattern is the WRITE_FILE_PATTERN strftime template. It must resolve to a
	// stage:// URL after expansion.
	Pattern string
	// Format is "csv" or "jsonline".
	Format string
	// Attrs are the column names in output order (used for the CSV header and as
	// JSONLine object keys).
	Attrs []string

	// CSV formatting. Defaults mirror LOAD/SELECT INTO OUTFILE defaults.
	FieldTerminator []byte // default ","
	LineTerminator  []byte // default "\n"
	EnclosedBy      byte   // default '"' (the external reader's enclosure)
	Header          bool   // write a CSV header line

	// Stmt is the timestamp the pattern is evaluated against (statement start).
	Stmt time.Time
	// TimeZone is used to render TIMESTAMP values; defaults to UTC.
	TimeZone *time.Location
}

// ExternalWriter encodes batches and appends them to a single backing file.
type ExternalWriter interface {
	// WriteBatch encodes every row of bat and appends it to the output file.
	// The file is created lazily on the first non-empty batch, so a pipeline
	// that never receives rows produces no file.
	WriteBatch(ctx context.Context, bat *batch.Batch) error
	// Close flushes and finalizes the output file and returns the number of
	// rows written. It is a no-op (rowsWritten == 0) if no file was opened.
	Close(ctx context.Context) (rowsWritten uint64, err error)
}

type externalWriter struct {
	proc *process.Process
	cfg  WriterConfig

	fw           *fileservice.FileServiceWriter
	rowsWritten  uint64
	opened       bool
	expandedPath string
}

var _ ExternalWriter = (*externalWriter)(nil)

// NewExternalWriter builds an ExternalWriter for one pipeline. Defaults are
// filled for any unset CSV formatting option.
func NewExternalWriter(proc *process.Process, cfg WriterConfig) ExternalWriter {
	if len(cfg.FieldTerminator) == 0 {
		cfg.FieldTerminator = []byte(",")
	}
	if len(cfg.LineTerminator) == 0 {
		cfg.LineTerminator = []byte("\n")
	}
	if cfg.EnclosedBy == 0 {
		// The external CSV reader always parses with an enclosure, defaulting to
		// '"' when ENCLOSED BY is absent (an explicit '' also falls back to '"').
		// Write with the same enclosure so strings containing the field/line
		// terminator or quotes round-trip.
		cfg.EnclosedBy = '"'
	}
	if cfg.TimeZone == nil {
		cfg.TimeZone = time.UTC
	}
	if cfg.Format == "" {
		cfg.Format = FormatCSV
	}
	return &externalWriter{proc: proc, cfg: cfg}
}

// open lazily resolves the destination file and starts the streaming write.
func (w *externalWriter) open(ctx context.Context) error {
	if w.opened {
		return nil
	}

	stageURL, err := ExpandFilePattern(w.cfg.Pattern, w.cfg.Stmt)
	if err != nil {
		return err
	}
	w.expandedPath = stageURL

	sdef, err := stageutil.UrlToStageDef(stageURL, w.proc)
	if err != nil {
		return err
	}
	moPath, _, err := sdef.ToPath()
	if err != nil {
		return err
	}

	fw, err := fileservice.NewFileServiceWriter(moPath, ctx)
	if err != nil {
		return err
	}
	w.fw = fw
	w.opened = true

	if w.cfg.Format == FormatCSV && w.cfg.Header {
		if err := w.writeCSVHeader(); err != nil {
			return err
		}
	}
	return nil
}

func (w *externalWriter) WriteBatch(ctx context.Context, bat *batch.Batch) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if err := w.open(ctx); err != nil {
		return err
	}

	var (
		data []byte
		err  error
	)
	switch w.cfg.Format {
	case FormatCSV:
		data, err = w.encodeCSV(bat)
	case FormatJSONLine:
		data, err = w.encodeJSONLine(bat)
	default:
		return moerr.NewNotSupportedf(ctx, "external write format %q", w.cfg.Format)
	}
	if err != nil {
		return err
	}

	if _, err := w.fw.Write(data); err != nil {
		return err
	}
	w.rowsWritten += uint64(bat.RowCount())
	return nil
}

func (w *externalWriter) Close(ctx context.Context) (uint64, error) {
	if !w.opened || w.fw == nil {
		return 0, nil
	}
	err := w.fw.Close()
	w.fw = nil
	return w.rowsWritten, err
}

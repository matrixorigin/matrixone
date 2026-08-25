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

package externalwrite

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// TestNewExternalWriterDefaults checks that unset formatting options are filled.
func TestNewExternalWriterDefaults(t *testing.T) {
	w := NewExternalWriter(nil, WriterConfig{}).(*externalWriter)
	require.Equal(t, []byte(","), w.cfg.FieldTerminator)
	require.Equal(t, []byte("\n"), w.cfg.LineTerminator)
	require.Equal(t, byte('"'), w.cfg.EnclosedBy)
	require.Equal(t, time.UTC, w.cfg.TimeZone)
	require.Equal(t, FormatCSV, w.cfg.Format)

	// Explicit values are preserved.
	w2 := NewExternalWriter(nil, WriterConfig{
		Format:          FormatJSONLine,
		FieldTerminator: []byte("|"),
		LineTerminator:  []byte("\r\n"),
		EnclosedBy:      '\'',
		TimeZone:        time.FixedZone("X", 3600),
	}).(*externalWriter)
	require.Equal(t, []byte("|"), w2.cfg.FieldTerminator)
	require.Equal(t, []byte("\r\n"), w2.cfg.LineTerminator)
	require.Equal(t, byte('\''), w2.cfg.EnclosedBy)
	require.Equal(t, FormatJSONLine, w2.cfg.Format)
	require.Equal(t, "X", w2.cfg.TimeZone.String())
}

// TestWriteBatchNilEmpty: nil or empty batches never open a file.
func TestWriteBatchNilEmpty(t *testing.T) {
	w := NewExternalWriter(nil, WriterConfig{Format: FormatCSV}).(*externalWriter)
	require.NoError(t, w.WriteBatch(context.Background(), nil, nil))

	empty := batch.New([]string{"v"})
	empty.SetRowCount(0)
	require.NoError(t, w.WriteBatch(context.Background(), empty, nil))

	require.False(t, w.opened)
	require.False(t, w.observedData)
}

func TestWriteBatchObservesNonEmptyWriterOnce(t *testing.T) {
	require.True(t, fault.Enable())
	t.Cleanup(func() { fault.Disable() })
	require.NoError(t, fault.AddFaultPoint(
		context.Background(), FaultPointNonEmptyWriter, ":::", "return", 0, "", false))

	mp := mpool.MustNewZero()
	bat := testBatch(t, mp)
	defer bat.Clean(mp)

	fw, err := fileservice.NewFileServiceWriter(
		filepath.Join(t.TempDir(), "observed.csv"), context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { fw.Abort(errors.New("external writer test cleanup")) })
	w := NewExternalWriter(nil, WriterConfig{}).(*externalWriter)
	// Bypass stage resolution, which is outside this writer-lifecycle test.
	w.fw = fw
	w.opened = true
	analyzer := process.NewAnalyzer(0, false, false, "external-write-test")
	require.NoError(t, w.WriteBatch(context.Background(), bat, analyzer))
	require.NoError(t, w.WriteBatch(context.Background(), bat, analyzer))
	rows, err := w.Close(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(4), rows)

	// A writer may receive many non-empty batches, but contributes exactly once.
	count, ok := fault.GetFaultPointCount(FaultPointNonEmptyWriter)
	require.True(t, ok)
	require.Equal(t, int64(1), count)
}

// TestCloseNoOp: Close before any file is opened returns 0 rows, no error.
func TestCloseNoOp(t *testing.T) {
	w := NewExternalWriter(nil, WriterConfig{Format: FormatCSV}).(*externalWriter)
	rows, err := w.Close(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(0), rows)
}

// TestWriteCSVHeaderContent verifies the CSV header bytes are formatted from Attrs.
func TestWriteCSVHeaderContent(t *testing.T) {
	// Drive the header formatting directly through the field writer: a header is
	// the Attrs joined like a CSV row.
	w := NewExternalWriter(nil, WriterConfig{
		Format:     FormatCSV,
		Attrs:      []string{"id", "name"},
		EnclosedBy: '"',
	}).(*externalWriter)

	buf := &bytes.Buffer{}
	ncol := len(w.cfg.Attrs)
	for j, name := range w.cfg.Attrs {
		w.writeCSVField(buf, []byte(name), w.cfg.EnclosedBy != 0, j == ncol-1)
	}
	require.Equal(t, `"id","name"`+"\n", buf.String())
}

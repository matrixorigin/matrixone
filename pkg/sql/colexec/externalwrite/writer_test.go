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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/stretchr/testify/require"
)

// TestNewExternalWriterDefaults checks that unset formatting options are filled.
func TestNewExternalWriterDefaults(t *testing.T) {
	w := NewExternalWriter(nil, WriterConfig{}).(*externalWriter)
	require.Equal(t, []byte(","), w.cfg.FieldTerminator)
	require.Equal(t, []byte("\n"), w.cfg.LineTerminator)
	require.Equal(t, time.UTC, w.cfg.TimeZone)
	require.Equal(t, FormatCSV, w.cfg.Format)

	// Explicit values are preserved.
	w2 := NewExternalWriter(nil, WriterConfig{
		Format:          FormatJSONLine,
		FieldTerminator: []byte("|"),
		LineTerminator:  []byte("\r\n"),
		TimeZone:        time.FixedZone("X", 3600),
	}).(*externalWriter)
	require.Equal(t, []byte("|"), w2.cfg.FieldTerminator)
	require.Equal(t, []byte("\r\n"), w2.cfg.LineTerminator)
	require.Equal(t, FormatJSONLine, w2.cfg.Format)
	require.Equal(t, "X", w2.cfg.TimeZone.String())
}

// TestWriteBatchNilEmpty: nil or empty batches never open a file.
func TestWriteBatchNilEmpty(t *testing.T) {
	w := NewExternalWriter(nil, WriterConfig{Format: FormatCSV}).(*externalWriter)
	require.NoError(t, w.WriteBatch(context.Background(), nil))

	empty := batch.New([]string{"v"})
	empty.SetRowCount(0)
	require.NoError(t, w.WriteBatch(context.Background(), empty))

	require.False(t, w.opened)
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

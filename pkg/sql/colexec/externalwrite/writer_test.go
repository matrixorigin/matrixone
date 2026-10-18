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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"net/url"
	"os"
	"path/filepath"
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

func TestExternalWriterChecksResolvedStageAuthority(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.BuildPipelineContext(t.Context())
	proc.Base.SessionInfo.Account = "tenant"
	proc.Base.SessionInfo.Role = "accountadmin"
	dir := filepath.Join(t.TempDir(), "not-created")
	proc.GetStageCache().Set("owned", stage.StageDef{Name: "owned", Url: &url.URL{Scheme: stage.FILE_PROTOCOL, Path: dir}})
	bat := batch.NewWithSize(1)
	bat.Attrs = []string{"id"}
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(7), false, proc.Mp()))
	bat.SetRowCount(1)
	analyzer := process.NewAnalyzer(0, false, false, "externalwrite")
	w := NewExternalWriter(proc, WriterConfig{Pattern: "stage://owned/output.csv", Attrs: bat.Attrs})
	t.Cleanup(func() { w.Abort(t.Context()) })
	require.ErrorContains(t, w.WriteBatch(t.Context(), bat, analyzer), "only the sys account moadmin role")
	_, err := os.Stat(dir)
	require.ErrorIs(t, err, os.ErrNotExist)
	proc.Base.SessionInfo.Account = "sys"
	proc.Base.SessionInfo.Role = "moadmin"
	require.NoError(t, w.WriteBatch(t.Context(), bat, analyzer))
	rows, err := w.Close(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(1), rows)
	data, err := os.ReadFile(filepath.Join(dir, "output.csv"))
	require.NoError(t, err)
	require.Equal(t, "7\n", string(data))
}

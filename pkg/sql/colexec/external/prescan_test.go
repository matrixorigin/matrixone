// Copyright 2024 Matrix Origin
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

package external

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type prescanTrackingFS struct {
	fileservice.ETLFileService
	t           *testing.T
	readers     []*prescanTrackingReader
	readErr     error
	failReadAt  int
	readCalls   int
	partialRead bool
	closeErr    error
	onRead      func()
}

type prescanTrackingReader struct {
	io.ReadCloser
	readErr     error
	closes      int
	partialRead bool
	closeErr    error
	onRead      func()
}

func (r *prescanTrackingReader) Read(p []byte) (int, error) {
	if r.onRead != nil {
		defer r.onRead()
	}
	if r.readErr != nil {
		if r.partialRead {
			n, _ := r.ReadCloser.Read(p[:1])
			return n, r.readErr
		}
		return 0, r.readErr
	}
	return r.ReadCloser.Read(p)
}

func (r *prescanTrackingReader) Close() error {
	r.closes++
	return errors.Join(r.ReadCloser.Close(), r.closeErr)
}

func (fs *prescanTrackingFS) Read(ctx context.Context, vec *fileservice.IOVector) error {
	for _, r := range fs.readers {
		require.Equal(fs.t, 1, r.closes, "previous probe must close before opening the next")
	}
	fs.readCalls++
	if fs.readCalls == fs.failReadAt {
		return context.Canceled
	}
	if err := fs.ETLFileService.Read(ctx, vec); err != nil {
		return err
	}
	for _, entry := range vec.Entries {
		if entry.ReadCloserForRead == nil {
			continue
		}
		r := &prescanTrackingReader{ReadCloser: *entry.ReadCloserForRead, readErr: fs.readErr, partialRead: fs.partialRead, closeErr: fs.closeErr, onRead: fs.onRead}
		fs.readers = append(fs.readers, r)
		fs.t.Cleanup(func() {
			if r.closes == 0 {
				_ = r.ReadCloser.Close()
			}
		})
		*entry.ReadCloserForRead = r
	}
	return nil
}

func newPrescanTrackingFS(t *testing.T, data string) *prescanTrackingFS {
	ctx := context.Background()
	base, err := fileservice.NewLocalETLFS("etl", t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { base.Close(ctx) })
	require.NoError(t, base.Write(ctx, fileservice.IOVector{FilePath: "etl:/prescan.csv", Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(data)), ReaderForWrite: strings.NewReader(data)}}}))
	return &prescanTrackingFS{ETLFileService: base, t: t}
}

func TestReadFileOffsetClosesTailScanReaders(t *testing.T) {
	for _, tc := range []struct {
		name        string
		format      string
		strict      bool
		readErr     error
		missing     bool
		cpus        int
		wantOffsets []int64
		wantReaders int
		failReadAt  int
		partialRead bool
		wantErr     bool
	}{
		{name: "multiple probes", cpus: 4, wantOffsets: []int64{0, 4, 4, 8, 8, -1}, wantReaders: 3},
		{name: "strict multiple probes", strict: true, cpus: 4, wantOffsets: []int64{0, 6, 6, -1}, wantReaders: 2},
		{name: "single worker", cpus: 1, wantOffsets: []int64{0, -1}},
		{name: "strict EOF", strict: true, readErr: io.EOF, cpus: 4, wantOffsets: []int64{0, -1}, wantReaders: 1},
		{name: "strict scan error", strict: true, readErr: errors.New("scan failed"), cpus: 4, wantOffsets: []int64{0, -1}, wantReaders: 1},
		{name: "nonstrict scan error", readErr: errors.New("scan failed"), cpus: 4, wantErr: true, wantReaders: 1},
		// CSV framing propagates the first storage error without probing again.
		{name: "partial read error", readErr: errors.New("partial scan failed"), partialRead: true, cpus: 4, wantErr: true, wantReaders: 1},
		// Physical scanning can consume a buffered newline before exposing the
		// accompanying error. Keep its later-probe cleanup coverage separately.
		{name: "JSONLINE partial read error", format: tree.JSONLINE, readErr: errors.New("partial scan failed"), partialRead: true, cpus: 4, wantErr: true, wantReaders: 3},
		{name: "open error", missing: true, cpus: 4},
		{name: "later open error", failReadAt: 2, cpus: 4, wantReaders: 1},
		{name: "strict canceled scan", strict: true, readErr: context.Canceled, cpus: 4, wantErr: true, wantReaders: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := "a\nb\nc\nd\ne\nf\n"
			if tc.partialRead {
				data = "a\nb\nc\nd\ne\nfX"
			}
			fs := newPrescanTrackingFS(t, data)
			fs.readErr = tc.readErr
			fs.failReadAt = tc.failReadAt
			fs.partialRead = tc.partialRead
			format := tc.format
			if format == "" {
				format = tree.CSV
			}
			param := &tree.ExternParam{
				ExParamConst: tree.ExParamConst{Filepath: "etl:/prescan.csv", Format: format, ScanType: tree.INFILE, CompressType: tree.NOCOMPRESS, Tail: &tree.TailParameter{}},
				ExParam:      tree.ExParam{FileService: fs, Ctx: fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin"), Strict: tc.strict},
			}
			if tc.missing {
				param.Filepath = "etl:/missing.csv"
			}
			offsets, err := ReadFileOffset(param, tc.cpus, int64(len(data)), nil)
			if tc.missing || tc.failReadAt > 0 || tc.wantErr {
				require.Error(t, err)
				require.Nil(t, offsets)
				if tc.failReadAt > 0 {
					require.ErrorIs(t, err, context.Canceled)
				}
				if tc.wantErr {
					require.ErrorIs(t, err, tc.readErr)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantOffsets, offsets)
			}
			require.Len(t, fs.readers, tc.wantReaders)
			wantReadCalls := tc.wantReaders
			if tc.missing || tc.failReadAt > 0 {
				wantReadCalls++
			}
			require.Equal(t, wantReadCalls, fs.readCalls, "no probe may open after a terminal error")
			for _, r := range fs.readers {
				require.Equal(t, 1, r.closes)
			}
		})
	}
}

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

package plan

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
	t       *testing.T
	readers []*prescanTrackingReader
	readErr error
}

type prescanTrackingReader struct {
	io.ReadCloser
	readErr error
	closes  int
}

func (r *prescanTrackingReader) Read(p []byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	return r.ReadCloser.Read(p)
}

func (r *prescanTrackingReader) Close() error {
	r.closes++
	return r.ReadCloser.Close()
}

func (fs *prescanTrackingFS) Read(ctx context.Context, vec *fileservice.IOVector) error {
	for _, r := range fs.readers {
		require.Equal(fs.t, 1, r.closes, "previous probe must close before opening the next")
	}
	if err := fs.ETLFileService.Read(ctx, vec); err != nil {
		return err
	}
	for _, entry := range vec.Entries {
		if entry.ReadCloserForRead == nil {
			continue
		}
		r := &prescanTrackingReader{ReadCloser: *entry.ReadCloserForRead, readErr: fs.readErr}
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

func TestIgnoredLinesClosesPrescanReaders(t *testing.T) {
	scanErr := errors.New("prescan read failed")
	for _, tc := range []struct {
		name             string
		ignored          uint64
		readErr          error
		invalidDelimiter bool
		missing          bool
		wantOffset       int64
		wantErr          bool
	}{
		{name: "success", ignored: 1, wantOffset: 7},
		{name: "zero ignored", ignored: 0},
		{name: "EOF", ignored: 3, wantOffset: 11},
		{name: "scan error", ignored: 1, readErr: scanErr, wantErr: true},
		{name: "canceled scan", ignored: 1, readErr: context.Canceled, wantErr: true},
		{name: "parser initialization error", ignored: 1, invalidDelimiter: true, wantErr: true},
		{name: "open error", ignored: 1, missing: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newPrescanTrackingFS(t, "header\nrow\n")
			fs.readErr = tc.readErr
			param := &tree.ExternParam{
				ExParamConst: tree.ExParamConst{Filepath: "etl:/prescan.csv", Format: tree.CSV, ScanType: tree.INFILE, Tail: &tree.TailParameter{IgnoredLines: tc.ignored}},
				ExParam:      tree.ExParam{FileService: fs},
			}
			if tc.missing {
				param.Filepath = "etl:/missing.csv"
			}
			if tc.invalidDelimiter {
				param.Tail.Fields = &tree.Fields{Terminated: &tree.Terminated{Value: "\n"}}
			}
			compCtx, _, _ := newLoadGlobTestCtx(t)
			offset, err := IgnoredLines(param, compCtx)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantOffset, offset)
				require.Zero(t, param.Tail.IgnoredLines)
				require.Nil(t, param.Ctx)
			}
			if tc.readErr != nil {
				require.ErrorIs(t, err, tc.readErr)
			}
			if tc.missing {
				require.Empty(t, fs.readers)
			} else {
				require.Len(t, fs.readers, 1)
				require.Equal(t, 1, fs.readers[0].closes)
			}
		})
	}
}

func TestIgnoredLinesJSONLineWhitespace(t *testing.T) {
	for _, tc := range []struct {
		name, data, format string
		ignore             uint64
		offset             int64
	}{
		{"JSONLINE LF", "\n \n{\"a\":2}\n{\"a\":3}\n", tree.JSONLINE, 1, 11},
		{"JSONLINE CRLF", "\r\n \r\n[2]\r\n[3]", tree.JSONLINE, 1, 10},
		{"JSONLINE empty EOF", " \n\u00a0", tree.JSONLINE, 1, 4},
		{"JSONLINE ignore beyond EOF", " \n[2]", tree.JSONLINE, 2, 5},
		{"CSV whitespace is a record", "\n \nx\n", tree.CSV, 1, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newPrescanTrackingFS(t, tc.data)
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: "etl:/prescan.csv", Format: tc.format, ScanType: tree.INFILE, Tail: &tree.TailParameter{IgnoredLines: tc.ignore}}, ExParam: tree.ExParam{FileService: fs}}
			compCtx, _, _ := newLoadGlobTestCtx(t)
			offset, err := IgnoredLines(param, compCtx)
			require.NoError(t, err)
			require.Equal(t, tc.offset, offset)
			require.Zero(t, param.Tail.IgnoredLines)
			require.Nil(t, param.Ctx)
			require.Len(t, fs.readers, 1)
			require.Equal(t, 1, fs.readers[0].closes)
		})
	}
}

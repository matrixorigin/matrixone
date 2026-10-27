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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type prescanTrackingFS struct {
	fileservice.ETLFileService
	t         *testing.T
	readers   []*prescanTrackingReader
	readErr   error
	readLimit int
}

type prescanTrackingReader struct {
	io.ReadCloser
	readErr   error
	readLimit int
	closes    int
}

func (r *prescanTrackingReader) Read(p []byte) (int, error) {
	if r.readErr != nil {
		if r.readLimit == 0 {
			return 0, r.readErr
		}
		if len(p) > r.readLimit {
			p = p[:r.readLimit]
		}
		n, err := r.ReadCloser.Read(p)
		r.readLimit -= n
		return n, err
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
		r := &prescanTrackingReader{ReadCloser: *entry.ReadCloserForRead, readErr: fs.readErr, readLimit: fs.readLimit}
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

func TestIgnoredLinesFailureAfterComment(t *testing.T) {
	scanErr := errors.New("read failed after comment")
	fs := newPrescanTrackingFS(t, "#note\n1\n2\n")
	fs.readErr, fs.readLimit = scanErr, len("#note\n")
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: "etl:/prescan.csv", Format: tree.CSV, ScanType: tree.INFILE, Option: []string{"comment", "#"}, Tail: &tree.TailParameter{IgnoredLines: 1}}, ExParam: tree.ExParam{FileService: fs}}
	compCtx, _, _ := newLoadGlobTestCtx(t)
	_, err := IgnoredLines(param, compCtx)
	require.ErrorIs(t, err, scanErr)
	require.Equal(t, uint64(1), param.Tail.IgnoredLines)
	require.Len(t, fs.readers, 1)
	require.Zero(t, fs.readers[0].readLimit, "comment must be read before the injected error")
	require.Equal(t, 1, fs.readers[0].closes)
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
		comment          string
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
		{name: "scan error with comment", comment: "#", ignored: 1, readErr: scanErr, wantErr: true},
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
			if tc.comment != "" {
				param.Option = []string{"comment", tc.comment}
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
				require.Equal(t, tc.ignored, param.Tail.IgnoredLines)
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

func TestIgnoredLinesCSVGrammar(t *testing.T) {
	for _, tc := range []struct {
		name, data, comment string
		ignore              uint64
		offset              int64
	}{
		{"witness", "#note\n1\n2\n3\n", "#", 1, 8},
		{"interleaved", "#a\n1\n#b\n2\n3\n", "#", 2, 10},
		{"multi marker", "REM note\n1\n2\n", "REM", 1, 11},
		{"partial marker is data", "RE\nREM comment\n2\n", "REM", 1, 3},
		{"partial marker at EOF", "RE", "REM", 1, 2},
		{"quoted multiline marker", "\"#a\nb\"\n2\n", "#", 1, 7},
		{"quoted marker", "\"#x\"\n2\n", "#", 1, 5},
		{"middle marker", "a,#x\n2\n", "#", 1, 5},
		{"default comma quoted newline", "a,\"b\nc\"\n2,x\n", "", 1, 8},
		{"comments EOF", "#a\n#b", "#", 1, 5},
		{"beyond EOF", "#a\n1", "#", 2, 4},
		{"zero", "#a\n1\n", "#", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newPrescanTrackingFS(t, tc.data)
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: "etl:/prescan.csv", Format: tree.CSV, ScanType: tree.INFILE, Tail: &tree.TailParameter{IgnoredLines: tc.ignore}, Option: []string{"comment", tc.comment}}, ExParam: tree.ExParam{FileService: fs}}
			compCtx, _, _ := newLoadGlobTestCtx(t)
			offset, err := IgnoredLines(param, compCtx)
			require.NoError(t, err)
			require.Equal(t, tc.offset, offset)
			require.Zero(t, param.Tail.IgnoredLines)
			require.Equal(t, 1, fs.readers[0].closes)
		})
	}
}

func TestLoadCommentIgnorePlanEntries(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy%v", legacy), func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "data.csv")
			require.NoError(t, os.WriteFile(file, []byte("#note\n1\n2\n3\n"), 0600))
			proc := newPlanTestProcess(t)
			proc.Base.SessionInfo.Account, proc.Base.SessionInfo.Role = "sys", "moadmin"
			ctx := NewMockCompilerContext(true, proc)
			ctx.SetContext(t.Context())
			stmt, err := mysql.ParseOne(t.Context(), fmt.Sprintf("load data infile '%s' into table nation ignore 1 lines parallel 'true'", file), 1)
			require.NoError(t, err)
			defer stmt.Free()
			load := stmt.(*tree.Load)
			load.Param.Option = append(load.Param.Option, "comment", "#")
			var result *planpb.Plan
			if legacy {
				result, err = buildLoad(load, ctx, false)
			} else {
				result, err = BuildPlan(ctx, stmt, false)
			}
			require.NoError(t, err)
			var scan *planpb.Node
			for _, node := range result.GetQuery().Nodes {
				if node.NodeType == planpb.Node_EXTERNAL_SCAN {
					require.Nil(t, scan)
					scan = node
				}
			}
			require.NotNil(t, scan)
			var param tree.ExternParam
			require.NoError(t, json.Unmarshal([]byte(scan.TableDef.Createsql), &param))
			require.Equal(t, int64(8), param.FileStartOff)
			require.Zero(t, param.Tail.IgnoredLines)
			require.False(t, param.Parallel)
		})
	}
}

func TestCSVConfigForLoadOptions(t *testing.T) {
	p := &tree.ExternParam{ExParamConst: tree.ExParamConst{Tail: &tree.TailParameter{}}}
	c := CSVConfigForLoad(p)
	require.Equal(t, ",", c.FieldsTerminatedBy)
	require.Equal(t, "\"", c.FieldsEnclosedBy)
	require.Equal(t, "\\", c.FieldsEscapedBy)
	require.Equal(t, "\n", c.LinesTerminatedBy)
	require.Equal(t, []string{`\N`}, c.Null)
	require.True(t, c.UnescapedQuote)
	require.Empty(t, c.Comment)
	p.Tail.Fields = tree.NewFields("|", false, '\'', 0)
	p.Tail.Lines = tree.NewLines("start:", "\r\n")
	p.Option = []string{"comment", "REM"}
	c = CSVConfigForLoad(p)
	require.Equal(t, "|", c.FieldsTerminatedBy)
	require.Equal(t, "'", c.FieldsEnclosedBy)
	require.Empty(t, c.FieldsEscapedBy)
	require.Equal(t, "\r\n", c.LinesTerminatedBy)
	require.Equal(t, "start:", c.LinesStartingBy)
	require.Equal(t, "REM", c.Comment)
	p.Format = tree.JSONLINE
	p.Ctx = context.Background()
	c = CSVConfigForLoad(p)
	require.Equal(t, "\t", c.FieldsTerminatedBy)
	require.Empty(t, c.FieldsEscapedBy)
	require.True(t, c.SkipWhitespaceRecords)
	require.Equal(t, p.Ctx, c.Context)
}

func TestIgnoredLinesCommentWithStartingAndTerminator(t *testing.T) {
	for _, tc := range []struct {
		name, data, starting, terminator string
		offset                           int64
	}{
		{"starting prefix", "#start:skip\nstart:1\nstart:2\n", "start:", "\n", 20},
		{"CRLF", "#skip\r\n1\r\n2\r\n", "", "\r\n", 10},
		{"custom terminator", "#skip|1|2|", "", "|", 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newPrescanTrackingFS(t, tc.data)
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: "etl:/prescan.csv", Format: tree.CSV, ScanType: tree.INFILE, Option: []string{"comment", "#"}, Tail: &tree.TailParameter{IgnoredLines: 1, Lines: tree.NewLines(tc.starting, tc.terminator)}}, ExParam: tree.ExParam{FileService: fs}}
			compCtx, _, _ := newLoadGlobTestCtx(t)
			offset, err := IgnoredLines(param, compCtx)
			require.NoError(t, err)
			require.Equal(t, tc.offset, offset)
			require.Zero(t, param.Tail.IgnoredLines)
			require.Equal(t, 1, fs.readers[0].closes)
		})
	}
}

func TestPrepareLoadScanPrescanAndFailure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		path       string
		readErr    error
		wantOffset int64
		wantErr    bool
	}{
		{name: "prescan", path: "etl:/prescan.csv", wantOffset: 7},
		{name: "glob skips prescan", path: "etl:/part-*.csv"},
		{name: "read error", path: "etl:/prescan.csv", readErr: errors.New("read failed"), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newPrescanTrackingFS(t, "header\nrow\n")
			fs.readErr = tc.readErr
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: tc.path, Format: tree.CSV, ScanType: tree.INFILE, FileSize: 1 << 30, Tail: &tree.TailParameter{IgnoredLines: 1, ColumnList: []tree.LoadColumn{&tree.VarExpr{Name: "v"}}}}, ExParam: tree.ExParam{FileService: fs, Parallel: true}}
			table := &planpb.TableDef{Createsql: "unchanged"}
			ctx, _, _ := newLoadGlobTestCtx(t)
			offset, err := prepareLoadScan(param, true, ctx, table)
			if tc.wantErr {
				require.ErrorIs(t, err, tc.readErr)
				require.Equal(t, "unchanged", table.Createsql)
				require.Len(t, param.Tail.ColumnList, 1)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantOffset, offset)
				require.Nil(t, param.Tail.ColumnList)
				require.NotEqual(t, "unchanged", table.Createsql)
			}
			for _, r := range fs.readers {
				require.Equal(t, 1, r.closes)
			}
		})
	}
}

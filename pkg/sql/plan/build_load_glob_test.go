// Copyright 2021 - 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newLoadGlobTestCtx(t *testing.T) (externalStatsTestCtx, *process.Process, context.Context) {
	t.Helper()
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	return externalStatsTestCtx{
		MockCompilerContext: &MockCompilerContext{ctx: ctx},
		proc:                proc,
	}, proc, ctx
}

func writeLoadGlobFile(t *testing.T, proc *process.Process, ctx context.Context, path, data string) {
	t.Helper()
	require.NoError(t, proc.GetFileService().Write(ctx, fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   []byte(data),
		}},
	}))
}

func loadGlobParam(proc *process.Process, filepath, format string) *tree.ExternParam {
	return &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE,
			Format:   format,
			Option:   []string{"filepath", filepath, "format", format},
			Tail:     &tree.TailParameter{},
		},
		ExParam: tree.ExParam{
			FileService: proc.GetFileService(),
		},
	}
}

func TestCheckFileExistCsvGlobListsEveryMatch(t *testing.T) {
	compCtx, proc, ctx := newLoadGlobTestCtx(t)
	writeLoadGlobFile(t, proc, ctx, "etl:/glob/part-0.csv", "1,a\n")
	writeLoadGlobFile(t, proc, ctx, "etl:/glob/part-1.csv", "2,bb\n3,ccc\n")
	writeLoadGlobFile(t, proc, ctx, "etl:/glob/other.txt", "ignored\n")

	param := loadGlobParam(proc, "etl:/glob/part-*.csv", tree.CSV)
	name, err := checkFileExist(param, compCtx)
	require.NoError(t, err)

	// The pattern is kept so compile re-resolves the same set, and FileSize is
	// the sum over all matches -- it gates the load-level size thresholds.
	require.Equal(t, "etl:/glob/part-*.csv", name)
	require.Equal(t, "etl:/glob/part-*.csv", param.Filepath)
	require.Equal(t, int64(len("1,a\n")+len("2,bb\n3,ccc\n")), param.FileSize)
}

func TestCheckFileExistGlobMatchingOneFileCollapsesToThatFile(t *testing.T) {
	compCtx, proc, ctx := newLoadGlobTestCtx(t)
	writeLoadGlobFile(t, proc, ctx, "etl:/single/only-0.csv", "1,a\n")

	param := loadGlobParam(proc, "etl:/single/only-*.csv", tree.CSV)
	name, err := checkFileExist(param, compCtx)
	require.NoError(t, err)

	// A single match must stop looking like a pattern: otherwise the byte-offset
	// split stays disabled and the IGNORE-lines prescan is skipped for a file
	// that would be split into chunks, each re-skipping the header.
	require.Equal(t, "etl:/single/only-0.csv", name)
	require.Equal(t, "etl:/single/only-0.csv", param.Filepath)
	require.Equal(t, int64(len("1,a\n")), param.FileSize)
	require.False(t, LoadFilepathHasGlob(param))
}

func TestCheckFileExistGlobMatchingNothingErrors(t *testing.T) {
	compCtx, proc, ctx := newLoadGlobTestCtx(t)
	writeLoadGlobFile(t, proc, ctx, "etl:/empty/keep.txt", "x\n")

	param := loadGlobParam(proc, "etl:/empty/part-*.csv", tree.CSV)
	_, err := checkFileExist(param, compCtx)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Contains(t, err.Error(), "the file does not exist in load flow")
}

func TestCheckFileExistJsonlineGlobListsEveryMatch(t *testing.T) {
	compCtx, proc, ctx := newLoadGlobTestCtx(t)
	writeLoadGlobFile(t, proc, ctx, "etl:/jl/a-0.jl", "{\"a\":1}\n")
	writeLoadGlobFile(t, proc, ctx, "etl:/jl/a-1.jl", "{\"a\":2}\n")

	param := loadGlobParam(proc, "etl:/jl/a-*.jl", tree.JSONLINE)
	param.JsonData = tree.OBJECT
	param.Option = append(param.Option, "jsondata", tree.OBJECT)
	name, err := checkFileExist(param, compCtx)
	require.NoError(t, err)
	require.Equal(t, "etl:/jl/a-*.jl", name)
	require.Equal(t, int64(2*len("{\"a\":1}\n")), param.FileSize)
}

func TestCheckFileExistCompressedGlobSumsCompressedSizes(t *testing.T) {
	compCtx, proc, ctx := newLoadGlobTestCtx(t)
	writeLoadGlobFile(t, proc, ctx, "etl:/z/part-0.csv.lz4", "not-really-lz4")
	writeLoadGlobFile(t, proc, ctx, "etl:/z/part-1.csv.lz4", "also-not")

	param := loadGlobParam(proc, "etl:/z/part-*.csv.lz4", tree.CSV)
	name, err := checkFileExist(param, compCtx)
	require.NoError(t, err)
	require.Equal(t, "etl:/z/part-*.csv.lz4", name)
	require.Equal(t, int64(len("not-really-lz4")+len("also-not")), param.FileSize)
}

func TestEstimateLoadRowsizeSkipsGlobSource(t *testing.T) {
	_, proc, ctx := newLoadGlobTestCtx(t)
	writeLoadGlobFile(t, proc, ctx, "etl:/est/part-0.csv", "1,a\n")

	// A pattern names no single file to sample; the estimate must decline
	// rather than issue a read that can only fail.
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: "etl:/est/part-*.csv",
			FileSize: 4,
			Format:   tree.CSV,
			Tail:     &tree.TailParameter{},
		},
		ExParam: tree.ExParam{
			FileService: proc.GetFileService(),
			Ctx:         ctx,
		},
	}
	require.Equal(t, float64(0), estimateLoadRowsizeFromFirstLine(param, 4, 0, ctx))
}

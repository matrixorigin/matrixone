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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type parquetLoadTestCtx struct {
	CompilerContext
	ctx context.Context
}

func (c parquetLoadTestCtx) GetContext() context.Context {
	return c.ctx
}

type externalStatsTestCtx struct {
	*MockCompilerContext
	proc *process.Process
}

func (c externalStatsTestCtx) GetProcess() *process.Process {
	return c.proc
}

func TestValidateLoadParquetOptionsRejectsUnsupportedOptions(t *testing.T) {
	ctx := parquetLoadTestCtx{ctx: context.Background()}

	cases := []struct {
		name      string
		mutate    func(*tree.ExternParam)
		wantCode  uint16
		wantError string
	}{
		{
			name: "compression option",
			mutate: func(param *tree.ExternParam) {
				param.Option = []string{"compression", "gzip"}
			},
			wantCode:  moerr.ErrBadConfig,
			wantError: "compression option",
		},
		{
			name: "compression option upper value",
			mutate: func(param *tree.ExternParam) {
				param.Option = []string{"compression", "GZIP"}
			},
			wantCode:  moerr.ErrBadConfig,
			wantError: "compression option",
		},
		{
			name: "compression option empty value",
			mutate: func(param *tree.ExternParam) {
				param.Option = []string{"compression", ""}
			},
			wantCode:  moerr.ErrBadConfig,
			wantError: "compression option",
		},
		{
			name: "resolved compression field",
			mutate: func(param *tree.ExternParam) {
				param.CompressType = tree.GZIP
			},
			wantCode:  moerr.ErrBadConfig,
			wantError: "compression option",
		},
		{
			name: "fields option",
			mutate: func(param *tree.ExternParam) {
				param.Tail.Fields = &tree.Fields{}
			},
			wantCode:  moerr.ErrBadConfig,
			wantError: "FIELDS option",
		},
		{
			name: "lines option",
			mutate: func(param *tree.ExternParam) {
				param.Tail.Lines = &tree.Lines{}
			},
			wantCode:  moerr.ErrBadConfig,
			wantError: "LINES option",
		},
		{
			name: "ignore lines",
			mutate: func(param *tree.ExternParam) {
				param.Tail.IgnoredLines = 1
			},
			wantCode:  moerr.ErrBadConfig,
			wantError: "IGNORE LINES",
		},
		{
			name: "jsondata option",
			mutate: func(param *tree.ExternParam) {
				param.Option = []string{"jsondata", "object"}
			},
			wantCode:  moerr.ErrBadConfig,
			wantError: "jsondata option",
		},
		{
			name: "jsondata value",
			mutate: func(param *tree.ExternParam) {
				param.JsonData = tree.OBJECT
			},
			wantCode:  moerr.ErrBadConfig,
			wantError: "jsondata option",
		},
		{
			name: "format parquet followed by jsondata",
			mutate: func(param *tree.ExternParam) {
				param.Option = []string{"format", "parquet", "jsondata", "object"}
				param.Format = tree.JSONLINE
				param.JsonData = tree.OBJECT
			},
			wantCode:  moerr.ErrBadConfig,
			wantError: "jsondata option",
		},
		{
			name: "hive partitioning option",
			mutate: func(param *tree.ExternParam) {
				param.Option = []string{"hive_partitioning", "true"}
			},
			wantCode:  moerr.ErrBadConfig,
			wantError: "hive partitioning options",
		},
		{
			name: "hive partition columns option",
			mutate: func(param *tree.ExternParam) {
				param.Option = []string{"hive_partition_columns", "dt,region"}
			},
			wantCode:  moerr.ErrBadConfig,
			wantError: "hive partitioning options",
		},
		{
			name: "hive partitioning value",
			mutate: func(param *tree.ExternParam) {
				param.HivePartitioning = true
			},
			wantCode:  moerr.ErrBadConfig,
			wantError: "hive partitioning options",
		},
		{
			name: "local",
			mutate: func(param *tree.ExternParam) {
				param.Local = true
			},
			wantCode:  moerr.ErrNYI,
			wantError: "load parquet local",
		},
		{
			name: "user variable",
			mutate: func(param *tree.ExternParam) {
				param.Tail.ColumnList = []tree.LoadColumn{&tree.VarExpr{Name: "v"}}
			},
			wantCode:  moerr.ErrNYI,
			wantError: "parquet load with @variables in column list",
		},
		{
			name: "user variable mixed with regular column",
			mutate: func(param *tree.ExternParam) {
				param.Tail.ColumnList = []tree.LoadColumn{
					tree.NewUnresolvedColName("id"),
					&tree.VarExpr{Name: "v"},
				}
			},
			wantCode:  moerr.ErrNYI,
			wantError: "parquet load with @variables in column list",
		},
		{
			name: "set clause",
			mutate: func(param *tree.ExternParam) {
				param.Tail.Assignments = tree.UpdateExprs{&tree.UpdateExpr{}}
			},
			wantCode:  moerr.ErrNYI,
			wantError: "parquet load with SET clause",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			param := &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					Format: tree.PARQUET,
					Tail:   &tree.TailParameter{},
				},
			}
			tc.mutate(param)

			err := validateLoadParquetOptions(param, ctx)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, tc.wantCode), "unexpected error: %v", err)
			require.Contains(t, err.Error(), tc.wantError)
		})
	}
}

func TestValidateLoadParquetOptionsAllowsPlainParquet(t *testing.T) {
	ctx := parquetLoadTestCtx{ctx: context.Background()}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format: tree.PARQUET,
			Tail:   &tree.TailParameter{},
			Option: []string{"format", "parquet"},
		},
	}

	require.NoError(t, validateLoadParquetOptions(param, ctx))
}

func TestValidateLoadParquetOptionsAllowsStageDefaultCompressionAuto(t *testing.T) {
	ctx := parquetLoadTestCtx{ctx: context.Background()}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format:       tree.PARQUET,
			CompressType: tree.AUTO,
			Tail:         &tree.TailParameter{},
		},
	}

	require.NoError(t, validateLoadParquetOptions(param, ctx))
}

func TestValidateLoadParquetOptionsUsesResolvedFormatForS3(t *testing.T) {
	ctx := parquetLoadTestCtx{ctx: context.Background()}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.S3,
			Format:   tree.PARQUET,
			Tail:     &tree.TailParameter{},
			Option:   []string{"jsondata", "object"},
		},
	}

	err := validateLoadParquetOptions(param, ctx)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
	require.Contains(t, err.Error(), "jsondata option")
}

func TestValidateLoadParquetOptionsUsesOptionFormatForLocal(t *testing.T) {
	ctx := parquetLoadTestCtx{ctx: context.Background()}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format: tree.CSV,
			Tail:   &tree.TailParameter{},
			Option: []string{"filepath", "/tmp/data.parquet", "format", "parquet"},
		},
		ExParam: tree.ExParam{
			Local: true,
		},
	}

	err := validateLoadParquetOptions(param, ctx)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNYI))
	require.Contains(t, err.Error(), "load parquet local")
}

func TestValidateLoadParquetOptionsIgnoresNonParquet(t *testing.T) {
	ctx := parquetLoadTestCtx{ctx: context.Background()}
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format: tree.JSONLINE,
			Tail:   &tree.TailParameter{},
			Option: []string{"jsondata", "object"},
		},
		ExParam: tree.ExParam{
			JsonData: tree.OBJECT,
		},
	}

	require.NoError(t, validateLoadParquetOptions(param, ctx))
}

func TestMakeLoadExternalStatsUsesInputBytes(t *testing.T) {
	tableDef := &TableDef{
		Cols: []*ColDef{
			{Name: "a", Typ: Type{Id: int32(types.T_int64), Width: 64}},
			{Name: "b", Typ: Type{Id: int32(types.T_char), Width: 1}},
		},
	}
	stats := makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{FileSize: 100},
	}, tableDef, 25, context.Background())
	require.GreaterOrEqual(t, stats.Cost, float64(1))
	require.GreaterOrEqual(t, stats.Rowsize, float64(1))
	requireLoadByteHint(t, stats, 75)
	require.Equal(t, stats.Cost, stats.TableCnt)
	require.Equal(t, stats.Cost, stats.Outcnt)

	stats = makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{FileSize: 10},
	}, tableDef, 20, context.Background())
	require.Equal(t, float64(0), stats.Cost)
	require.Equal(t, float64(1), stats.Rowsize)
	require.Equal(t, int32(0), stats.BlockNum)

	stats = makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INLINE,
			Format:   tree.CSV,
			Data:     "1,2\n3,4\n",
		},
	}, tableDef, 0, context.Background())
	require.Equal(t, float64(4), stats.Rowsize)
	require.Equal(t, float64(2), stats.Cost)
	requireLoadByteHint(t, stats, 8)

	stats = makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INLINE,
			Format:   tree.CSV,
			Data:     "a\nb\nc\n",
		},
	}, tableDef, 0, context.Background())
	require.Equal(t, float64(2), stats.Rowsize)
	require.Equal(t, float64(3), stats.Cost)
	requireLoadByteHint(t, stats, 6)

	stats = makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INLINE,
			Format:   tree.CSV,
			Data:     "a|b|c|",
			Tail: &tree.TailParameter{
				Lines: &tree.Lines{
					TerminatedBy: &tree.Terminated{Value: "|"},
				},
			},
		},
	}, tableDef, 0, context.Background())
	require.Equal(t, float64(2), stats.Rowsize)
	require.Equal(t, float64(3), stats.Cost)
	requireLoadByteHint(t, stats, 6)

	rowSize := GetRowSizeFromTableDef(tableDef, true) * 0.8
	stats = makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{FileSize: int64(float64(options.DefaultBlockMaxRows) * rowSize)},
	}, tableDef, 0, context.Background())
	require.Equal(t, int32(1), stats.BlockNum)
}

func TestMakeLoadExternalStatsKeepsLargeLoadMultiCN(t *testing.T) {
	tableDef := &TableDef{
		Cols: []*ColDef{
			{Name: "a", Typ: Type{Id: int32(types.T_int64), Width: 64}},
			{Name: "b", Typ: Type{Id: int32(types.T_char), Width: 1}},
		},
	}
	inputSize := int64(float64(options.DefaultBlockMaxRows) * GetRowSizeFromTableDef(tableDef, true) * 0.8 * float64(BlockThresholdForOneCN+1))
	stats := makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{FileSize: inputSize},
	}, tableDef, 0, context.Background())
	require.Greater(t, stats.BlockNum, int32(BlockThresholdForOneCN))
	require.Greater(t, stats.Cost, float64(costThresholdForOneCN))
	require.Equal(t, ExecTypeAP_MULTICN, GetExecType(&Query{
		Nodes: []*Node{{
			NodeType: pbplan.Node_EXTERNAL_SCAN,
			Stats:    stats,
		}},
		Steps: []int32{0},
	}, false, false))
}

func TestMakeLoadExternalStatsUsesFirstLineForTextLoad(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	filePath := fileservice.JoinPath(fs.Name(), "wide.csv")
	data := strings.Repeat("1,2\n", loadFirstLineSampleSize/len("1,2\n")+1)
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: filePath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   []byte(data),
		}},
	}))

	tableDef := &TableDef{
		Cols: []*ColDef{
			{Name: "a", Typ: Type{Id: int32(types.T_varchar), Width: 65535}},
			{Name: "b", Typ: Type{Id: int32(types.T_varchar), Width: 65535}},
		},
	}
	inputSize := int64(4 * options.DefaultBlockMaxRows * (BlockThresholdForOneCN + 1))
	stats := makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: filePath,
			FileSize: inputSize,
			Format:   tree.CSV,
		},
		ExParam: tree.ExParam{
			FileService: fs,
			Ctx:         ctx,
		},
	}, tableDef, 0, context.Background())

	require.Equal(t, float64(4), stats.Rowsize)
	require.Greater(t, stats.BlockNum, int32(BlockThresholdForOneCN))
	require.Equal(t, ExecTypeAP_MULTICN, GetExecType(&Query{
		Nodes: []*Node{{
			NodeType: pbplan.Node_EXTERNAL_SCAN,
			Stats:    stats,
		}},
		Steps: []int32{0},
	}, false, false))

	crlfPath := fileservice.JoinPath(fs.Name(), "crlf.csv")
	crlfData := strings.Repeat("1,2\r\n", loadFirstLineSampleSize/len("1,2\r\n")+1)
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: crlfPath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(crlfData)),
			Data:   []byte(crlfData),
		}},
	}))
	stats = makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: crlfPath,
			FileSize: inputSize,
			Format:   tree.CSV,
			Tail: &tree.TailParameter{
				Lines: &tree.Lines{
					TerminatedBy: &tree.Terminated{Value: "\r\n"},
				},
			},
		},
		ExParam: tree.ExParam{
			FileService: fs,
			Ctx:         ctx,
		},
	}, tableDef, 0, context.Background())
	require.Equal(t, float64(5), stats.Rowsize)

	ignoredPath := fileservice.JoinPath(fs.Name(), "ignored.csv")
	ignoredData := "long_header_value\n" + strings.Repeat("1,2\n", loadFirstLineSampleSize/len("1,2\n")+1)
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: ignoredPath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(ignoredData)),
			Data:   []byte(ignoredData),
		}},
	}))
	stats = makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: ignoredPath,
			FileSize: inputSize,
			Format:   tree.CSV,
			Tail: &tree.TailParameter{
				IgnoredLines: 1,
			},
		},
		ExParam: tree.ExParam{
			FileService: fs,
			Ctx:         ctx,
		},
	}, tableDef, 0, context.Background())
	require.Equal(t, float64(4), stats.Rowsize)

	schemaRowSize := GetRowSizeFromTableDef(tableDef, true) * 0.8
	stats = makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath:     filePath,
			FileSize:     inputSize,
			Format:       tree.CSV,
			CompressType: tree.GZIP,
		},
		ExParam: tree.ExParam{
			FileService: fs,
			Ctx:         ctx,
		},
	}, tableDef, 0, context.Background())
	require.Equal(t, schemaRowSize, stats.Rowsize)

	stats = makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: filePath,
			FileSize: inputSize,
			Format:   tree.CSV,
			Tail: &tree.TailParameter{
				Lines: &tree.Lines{
					TerminatedBy: &tree.Terminated{Value: "|"},
				},
			},
		},
		ExParam: tree.ExParam{
			FileService: fs,
			Ctx:         ctx,
		},
	}, tableDef, 0, context.Background())
	require.Equal(t, schemaRowSize, stats.Rowsize)
}

func TestMakeLoadExternalStatsUsesBoundedFirstLineSample(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	filePath := fileservice.JoinPath(fs.Name(), "long-row.csv")
	data := strings.Repeat("a", loadFirstLineSampleSize+128)
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: filePath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   []byte(data),
		}},
	}))

	tableDef := &TableDef{
		Cols: []*ColDef{
			{Name: "a", Typ: Type{Id: int32(types.T_int32)}},
			{Name: "b", Typ: Type{Id: int32(types.T_varchar), Width: 96}},
		},
	}
	schemaRowSize := GetRowSizeFromTableDef(tableDef, true) * 0.8
	stats := makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: filePath,
			FileSize: int64(len(data)),
			Format:   tree.CSV,
		},
		ExParam: tree.ExParam{
			FileService: fs,
			Ctx:         ctx,
		},
	}, tableDef, 0, ctx)

	require.Equal(t, schemaRowSize, stats.Rowsize)
	requireLoadByteHint(t, stats, int64(len(data)))
}

func TestReadExternalFirstLineSizeUsesBoundedRange(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	filePath := fileservice.JoinPath(fs.Name(), "with-newline.csv")
	data := "abcd\n" + strings.Repeat("x", loadFirstLineSampleSize+128)
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: filePath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   []byte(data),
		}},
	}))

	size := readExternalFirstLineSize(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: filePath,
			FileSize: int64(len(data)),
			Format:   tree.CSV,
		},
		ExParam: tree.ExParam{
			FileService: fs,
			Ctx:         ctx,
		},
	}, int64(len(data)), 0, ctx)

	require.Equal(t, len("abcd\n"), size)
}

func TestReadExternalFirstLineSizeUsesSmallFileWithoutTrailingNewline(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	filePath := fileservice.JoinPath(fs.Name(), "small.csv")
	data := "abcd"
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: filePath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   []byte(data),
		}},
	}))

	size := readExternalFirstLineSize(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath: filePath,
			FileSize: int64(len(data)),
			Format:   tree.CSV,
		},
		ExParam: tree.ExParam{
			FileService: fs,
			Ctx:         ctx,
		},
	}, int64(len(data)), 0, ctx)

	require.Equal(t, len(data), size)
}

func TestGetExternalStatsUsesKnownFileSizeForSmallFile(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	filePath := "etl:/small.csv"
	data := "abcd"
	require.NoError(t, proc.GetFileService().Write(ctx, fileservice.IOVector{
		FilePath: filePath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   []byte(data),
		}},
	}))

	builder := NewQueryBuilder(pbplan.Query_SELECT, externalStatsTestCtx{
		MockCompilerContext: &MockCompilerContext{ctx: ctx},
		proc:                proc,
	}, false, false)
	node := &pbplan.Node{
		NodeType: pbplan.Node_EXTERNAL_SCAN,
		ExternScan: &pbplan.ExternScan{
			Type: int32(pbplan.ExternType_EXTERNAL_TB),
		},
		TableDef: &pbplan.TableDef{
			Createsql: mustMarshalExternParam(t, &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					Option: []string{
						"filepath", filePath,
						"format", tree.CSV,
					},
				},
			}),
		},
	}

	stats := getExternalStats(node, builder)

	require.Equal(t, float64(len(data)), stats.Rowsize)
	require.Equal(t, float64(1), stats.Outcnt)
}

func mustMarshalExternParam(t *testing.T, param *tree.ExternParam) string {
	t.Helper()
	data, err := json.Marshal(param)
	require.NoError(t, err)
	return string(data)
}

func requireLoadByteHint(t *testing.T, stats *Stats, inputSize int64) {
	t.Helper()
	estimatedBytes := stats.Cost * stats.Rowsize
	require.GreaterOrEqual(t, estimatedBytes, float64(inputSize))
	require.Less(t, estimatedBytes, float64(inputSize)+stats.Rowsize)
}

func TestLoadParquetMayListFiles(t *testing.T) {
	require.True(t, loadParquetMayListFiles(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format:   tree.PARQUET,
			Filepath: "/data/*.parquet",
		},
	}))
	require.True(t, loadParquetMayListFiles(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format:   tree.PARQUET,
			Filepath: "/data/part-??.parquet",
		},
	}))
	require.False(t, loadParquetMayListFiles(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format:   tree.PARQUET,
			Filepath: "/data/part-000.parquet",
		},
	}))
	require.False(t, loadParquetMayListFiles(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format:   tree.CSV,
			Filepath: "/data/*.csv",
		},
	}))
}

func TestTotalLoadFileSize(t *testing.T) {
	require.Equal(t, int64(0), totalLoadFileSize(nil))
	require.Equal(t, int64(30), totalLoadFileSize([]int64{10, -1, 20, 0}))
}

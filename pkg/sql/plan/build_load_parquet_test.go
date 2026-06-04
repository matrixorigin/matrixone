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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type parquetLoadTestCtx struct {
	CompilerContext
	ctx context.Context
}

func (c parquetLoadTestCtx) GetContext() context.Context {
	return c.ctx
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
	stats := makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{FileSize: 100},
	}, 25)
	require.Equal(t, float64(75), stats.Cost)
	require.Equal(t, float64(1), stats.Rowsize)
	require.Equal(t, int32(1), stats.BlockNum)
	require.Equal(t, float64(1), stats.TableCnt)
	require.Equal(t, float64(1), stats.Outcnt)

	stats = makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{FileSize: 10},
	}, 20)
	require.Equal(t, float64(0), stats.Cost)
	require.Equal(t, float64(1), stats.Rowsize)
	require.Equal(t, int32(0), stats.BlockNum)

	stats = makeLoadExternalStats(&tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INLINE,
			Data:     "1,2\n3,4\n",
		},
	}, 0)
	require.Equal(t, float64(8), stats.Cost)
	require.Equal(t, float64(1), stats.Rowsize)
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

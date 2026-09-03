// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestValidateLoadArrowOptionsAcceptsFileStreamAndAuto(t *testing.T) {
	ctx := parquetLoadTestCtx{ctx: context.Background()}
	for _, container := range []string{"", tree.ARROW_CONTAINER_AUTO, tree.ARROW_CONTAINER_FILE, tree.ARROW_CONTAINER_STREAM} {
		t.Run("container="+container, func(t *testing.T) {
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
				Format: tree.ARROW, ArrowContainer: container, Tail: &tree.TailParameter{},
			}}
			require.NoError(t, validateLoadColumnarOptions(param, ctx))
			if container == "" {
				require.Equal(t, tree.ARROW_CONTAINER_AUTO, param.ArrowContainer)
			}
		})
	}
}

func TestValidateLoadArrowOptionsRejectsConflictingSurface(t *testing.T) {
	ctx := parquetLoadTestCtx{ctx: context.Background()}
	tests := []struct {
		name string
		edit func(*tree.ExternParam)
		text string
	}{
		{"local", func(p *tree.ExternParam) { p.Local = true }, "non-LOCAL"},
		{"inline", func(p *tree.ExternParam) { p.ScanType = tree.INLINE }, "file-backed"},
		{"compression-key", func(p *tree.ExternParam) { p.Option = []string{"compression", "auto"} }, "compression"},
		{"compression-state", func(p *tree.ExternParam) { p.CompressType = tree.GZIP }, "compression"},
		{"json", func(p *tree.ExternParam) { p.JsonData = tree.OBJECT }, "jsondata"},
		{"hive", func(p *tree.ExternParam) { p.HivePartitioning = true }, "hive"},
		{"fields", func(p *tree.ExternParam) { p.Tail.Fields = &tree.Fields{} }, "FIELDS"},
		{"lines", func(p *tree.ExternParam) { p.Tail.Lines = &tree.Lines{} }, "LINES"},
		{"ignore", func(p *tree.ExternParam) { p.Tail.IgnoredLines = 1 }, "IGNORE"},
		{"variable", func(p *tree.ExternParam) { p.Tail.ColumnList = []tree.LoadColumn{&tree.VarExpr{Name: "v"}} }, "@variables"},
		{"set", func(p *tree.ExternParam) { p.Tail.Assignments = tree.UpdateExprs{&tree.UpdateExpr{}} }, "SET"},
		{"container", func(p *tree.ExternParam) { p.ArrowContainer = "flight" }, "arrow_container"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
				Format: tree.ARROW, Tail: &tree.TailParameter{},
			}}
			test.edit(param)
			err := validateLoadColumnarOptions(param, ctx)
			require.Error(t, err)
			require.Contains(t, err.Error(), test.text)
		})
	}
}

func TestLoadArrowFormatDetectionAndListing(t *testing.T) {
	require.True(t, isLoadArrowFormat(&tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.ARROW}}))
	require.True(t, isLoadArrowFormat(&tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV, Option: []string{"FORMAT", "ARROW"},
	}}))
	require.False(t, isLoadArrowFormat(&tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.PARQUET}}))
	require.True(t, loadColumnarMayListFiles(&tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.ARROW, Filepath: "etl:data/*.arrow",
	}}))
	require.False(t, loadColumnarMayListFiles(&tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.ARROW, Filepath: "etl:data/one.arrow",
	}}))
}

func TestValidateLoadColumnarOptionsLeavesCSVUntouched(t *testing.T) {
	ctx := parquetLoadTestCtx{ctx: context.Background()}
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV, Tail: &tree.TailParameter{Fields: &tree.Fields{}},
	}}
	require.NoError(t, validateLoadColumnarOptions(param, ctx))
}

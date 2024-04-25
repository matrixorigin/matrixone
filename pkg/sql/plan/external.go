// Copyright 2022 Matrix Origin
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
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	STATEMENT_ACCOUNT = "account"
)

//this file is duplicate with colexec/external/external.go , to avoid import cycle

func filterFileList(ctx context.Context, node *plan.Node, proc *process.Process, fileList []string, fileSize []int64) ([]string, []int64, error) {
	return filterByAccountAndFilename(ctx, node, proc, fileList, fileSize)
}

func containColname(col string) bool {
	return strings.Contains(col, STATEMENT_ACCOUNT) || strings.Contains(col, catalog.ExternalFilePath)
}

func judgeContainColname(expr *plan.Expr) bool {
	expr_F, ok := expr.Expr.(*plan.Expr_F)
	if !ok {
		return false
	}
	if expr_F.F.Func.ObjName == "or" {
		flag := true
		for i := 0; i < len(expr_F.F.Args); i++ {
			flag = flag && judgeContainColname(expr_F.F.Args[i])
		}
		return flag
	}
	expr_Col, ok := expr_F.F.Args[0].Expr.(*plan.Expr_Col)
	if ok && containColname(expr_Col.Col.Name) {
		return true
	}
	for _, arg := range expr_F.F.Args {
		if judgeContainColname(arg) {
			return true
		}
	}
	return false
}

func filterByAccountAndFilename(ctx context.Context, node *plan.Node, proc *process.Process, fileList []string, fileSize []int64) ([]string, []int64, error) {
	_, span := trace.Start(ctx, "filterByAccountAndFilename")
	defer span.End()
	filterList := make([]*plan.Expr, 0)
	filterList2 := make([]*plan.Expr, 0)
	for i := 0; i < len(node.FilterList); i++ {
		if judgeContainColname(node.FilterList[i]) {
			filterList = append(filterList, node.FilterList[i])
		} else {
			filterList2 = append(filterList2, node.FilterList[i])
		}
	}
	if len(filterList) == 0 {
		return fileList, fileSize, nil
	}
	bat := makeFilepathBatch(node, proc, filterList, fileList)
	filter := colexec.RewriteFilterExprList(filterList)

	vec, err := colexec.EvalExpressionOnce(proc, filter, []*batch.Batch{bat})
	if err != nil {
		return nil, fileSize, err
	}

	fileListTmp := make([]string, 0)
	fileSizeTmp := make([]int64, 0)
	bs := vector.MustFixedCol[bool](vec)
	for i := 0; i < len(bs); i++ {
		if bs[i] {
			fileListTmp = append(fileListTmp, fileList[i])
			fileSizeTmp = append(fileSizeTmp, fileSize[i])
		}
	}
	vec.Free(proc.Mp())
	node.FilterList = filterList2
	return fileListTmp, fileSizeTmp, nil
}

func makeFilepathBatch(node *plan.Node, proc *process.Process, filterList []*plan.Expr, fileList []string) *batch.Batch {
	num := len(node.TableDef.Cols)
	bat := &batch.Batch{
		Attrs: make([]string, num),
		Vecs:  make([]*vector.Vector, num),
		Cnt:   1,
	}
	for i := 0; i < num; i++ {
		bat.Attrs[i] = node.TableDef.Cols[i].Name
		if bat.Attrs[i] == STATEMENT_ACCOUNT {
			typ := types.New(types.T(node.TableDef.Cols[i].Typ.Id), node.TableDef.Cols[i].Typ.Width, node.TableDef.Cols[i].Typ.Scale)
			vec, _ := proc.AllocVectorOfRows(typ, len(fileList), nil)
			//vec.SetOriginal(false)
			for j := 0; j < len(fileList); j++ {
				vector.SetStringAt(vec, j, getAccountCol(fileList[j]), proc.Mp())
			}
			bat.Vecs[i] = vec
		} else if bat.Attrs[i] == catalog.ExternalFilePath {
			typ := types.T_varchar.ToType()
			vec, _ := proc.AllocVectorOfRows(typ, len(fileList), nil)
			//vec.SetOriginal(false)
			for j := 0; j < len(fileList); j++ {
				vector.SetStringAt(vec, j, fileList[j], proc.Mp())
			}
			bat.Vecs[i] = vec
		}
	}
	bat.SetRowCount(len(fileList))
	return bat
}

func getAccountCol(filepath string) string {
	pathDir := strings.Split(filepath, "/")
	if len(pathDir) < 2 {
		return ""
	}
	return pathDir[1]
}

func getExternalStats(node *plan.Node, builder *QueryBuilder) *Stats {
	param := &tree.ExternParam{}
	err := json.Unmarshal([]byte(node.TableDef.Createsql), param)
	if err != nil || param.Local || param.ScanType == tree.S3 {
		return DefaultHugeStats()
	}

	if param.ScanType == tree.S3 {
		if err = InitS3Param(param); err != nil {
			return DefaultHugeStats()
		}
	} else {
		if err = InitInfileParam(param); err != nil {
			return DefaultHugeStats()
		}
	}

	param.FileService = builder.compCtx.GetProcess().FileService
	param.Ctx = builder.compCtx.GetProcess().Ctx
	_, spanReadDir := trace.Start(param.Ctx, "ReCalcNodeStats.ReadDir")
	fileList, fileSize, err := ReadDir(param)
	spanReadDir.End()
	if err != nil {
		return DefaultHugeStats()
	}
	fileList, fileSize, err = filterFileList(param.Ctx, node, builder.compCtx.GetProcess(), fileList, fileSize)
	if err != nil {
		return DefaultHugeStats()
	}
	if param.LoadFile && len(fileList) == 0 {
		// all files filtered, return a default small stats
		return DefaultStats()
	}
	var cost float64
	for i := range fileSize {
		cost += float64(fileSize[i])
	}

	//read one line
	fs, readPath, err := GetForETLWithType(param, param.Filepath)
	if err != nil {
		return DefaultHugeStats()
	}
	var r io.ReadCloser
	vec := fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            0,
				Size:              -1,
				ReadCloserForRead: &r,
			},
		},
	}
	if err = fs.Read(param.Ctx, &vec); err != nil {
		return DefaultHugeStats()
	}
	r2 := bufio.NewReader(r)
	line, _ := r2.ReadString('\n')
	size := len(line)
	cost = cost / float64(size)

	return &plan.Stats{
		Outcnt:      cost,
		Cost:        cost,
		Selectivity: 1,
		TableCnt:    cost,
		BlockNum:    int32(cost / float64(options.DefaultBlockMaxRows)),
	}
}

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

package external

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/pierrec/lz4/v4"
)

var (
	OneBatchMaxRow   = int(options.DefaultBlockMaxRows)
	S3ParallelMaxnum = 10
)

var (
	STATEMENT_ACCOUNT = "account"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("external output")
}

func Prepare(proc *process.Process, arg any) error {
	_, span := trace.Start(proc.Ctx, "ExternalPrepare")
	defer span.End()
	param := arg.(*Argument).Es
	if proc.Lim.MaxMsgSize == 0 {
		param.maxBatchSize = uint64(morpc.GetMessageSize())
	} else {
		param.maxBatchSize = proc.Lim.MaxMsgSize
	}
	param.maxBatchSize = uint64(float64(param.maxBatchSize) * 0.6)
	if param.Extern == nil {
		param.Extern = &tree.ExternParam{}
		if err := json.Unmarshal([]byte(param.CreateSql), param.Extern); err != nil {
			return err
		}
		if err := plan2.InitS3Param(param.Extern); err != nil {
			return err
		}
		param.Extern.FileService = proc.FileService
	}
	if param.Extern.Format == tree.JSONLINE {
		if param.Extern.JsonData != tree.OBJECT && param.Extern.JsonData != tree.ARRAY {
			param.Fileparam.End = true
			return moerr.NewNotSupported(proc.Ctx, "the jsonline format '%s' is not supported now", param.Extern.JsonData)
		}
	}
	param.IgnoreLineTag = int(param.Extern.Tail.IgnoredLines)
	param.IgnoreLine = param.IgnoreLineTag
	if len(param.FileList) == 0 {
		logutil.Warnf("no such file '%s'", param.Extern.Filepath)
		param.Fileparam.End = true
	}
	param.Fileparam.FileCnt = len(param.FileList)
	param.Ctx = proc.Ctx
	param.Zoneparam = &ZonemapFileparam{}
	name2ColIndex := make(map[string]int32, len(param.Cols))
	for i := 0; i < len(param.Cols); i++ {
		name2ColIndex[param.Cols[i].Name] = int32(i)
	}
	param.tableDef = &plan.TableDef{
		Name2ColIndex: name2ColIndex,
	}
	var columns []int
	param.Filter.columnMap, columns, _, param.Filter.maxCol = plan2.GetColumnsByExpr(param.Filter.FilterExpr, param.tableDef)
	param.Filter.columns = make([]uint16, len(columns))
	param.Filter.defColumns = make([]uint16, len(columns))
	for i := 0; i < len(columns); i++ {
		col := param.Cols[columns[i]]
		param.Filter.columns[i] = uint16(param.Name2ColIndex[col.Name])
		param.Filter.defColumns[i] = uint16(columns[i])
	}

	param.Filter.exprMono = plan2.CheckExprIsMonotonic(proc.Ctx, param.Filter.FilterExpr)
	param.Filter.File2Size = make(map[string]int64)
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ctx, span := trace.Start(proc.Ctx, "ExternalCall")
	defer span.End()
	select {
	case <-proc.Ctx.Done():
		proc.SetInputBatch(nil)
		return true, nil
	default:
	}
	t1 := time.Now()
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer func() {
		anal.Stop()
		anal.AddScanTime(t1)
	}()
	anal.Input(nil, isFirst)
	param := arg.(*Argument).Es
	if param.Fileparam.End {
		proc.SetInputBatch(nil)
		return true, nil
	}
	if param.plh == nil {
		if param.Fileparam.FileIndex >= len(param.FileList) {
			proc.SetInputBatch(nil)
			return true, nil
		}
		param.Fileparam.Filepath = param.FileList[param.Fileparam.FileIndex]
		param.Fileparam.FileIndex++
	}
	bat, err := ScanFileData(ctx, param, proc)
	if err != nil {
		param.Fileparam.End = true
		return false, err
	}
	proc.SetInputBatch(bat)
	if bat != nil {
		anal.Output(bat, isLast)
		anal.Alloc(int64(bat.Size()))
	}
	return false, nil
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

func getAccountCol(filepath string) string {
	pathDir := strings.Split(filepath, "/")
	if len(pathDir) < 2 {
		return ""
	}
	return pathDir[1]
}

func makeFilepathBatch(node *plan.Node, proc *process.Process, fileList []string) *batch.Batch {
	num := len(node.TableDef.Cols)
	bat := &batch.Batch{
		Attrs: make([]string, num),
		Vecs:  make([]*vector.Vector, num),
		Zs:    make([]int64, len(fileList)),
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
	for k := 0; k < len(fileList); k++ {
		bat.Zs[k] = 1
	}
	return bat
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
	bat := makeFilepathBatch(node, proc, fileList)
	filter := colexec.RewriteFilterExprList(filterList)

	executor, err := colexec.NewExpressionExecutor(proc, filter)
	if err != nil {
		return nil, nil, err
	}
	vec, err := executor.Eval(proc, []*batch.Batch{bat})
	if err != nil {
		executor.Free()
		return nil, nil, err
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
	executor.Free()
	node.FilterList = filterList2
	return fileListTmp, fileSizeTmp, nil
}

func FilterFileList(ctx context.Context, node *plan.Node, proc *process.Process, fileList []string, fileSize []int64) ([]string, []int64, error) {
	return filterByAccountAndFilename(ctx, node, proc, fileList, fileSize)
}

func readFile(param *ExternalParam, proc *process.Process) (io.ReadCloser, error) {
	if param.Extern.Local {
		return io.NopCloser(proc.LoadLocalReader), nil
	}
	fs, readPath, err := plan2.GetForETLWithType(param.Extern, param.Fileparam.Filepath)
	if err != nil {
		return nil, err
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
	if 2*param.Idx >= len(param.FileOffsetTotal[param.Fileparam.FileIndex-1].Offset) {
		return nil, nil
	}
	param.FileOffset = param.FileOffsetTotal[param.Fileparam.FileIndex-1].Offset[2*param.Idx : 2*param.Idx+2]
	if param.Extern.Parallel {
		vec.Entries[0].Offset = param.FileOffset[0]
		vec.Entries[0].Size = param.FileOffset[1] - param.FileOffset[0]
	}
	if vec.Entries[0].Size == 0 || vec.Entries[0].Offset >= param.FileSize[param.Fileparam.FileIndex-1] {
		return nil, nil
	}
	err = fs.Read(param.Ctx, &vec)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func ReadFileOffset(param *tree.ExternParam, mcpu int, fileSize int64) ([]int64, error) {
	arr := make([]int64, 0)

	fs, readPath, err := plan2.GetForETLWithType(param, param.Filepath)
	if err != nil {
		return nil, err
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
	var tailSize []int64
	var offset []int64
	for i := 0; i < mcpu; i++ {
		vec.Entries[0].Offset = int64(i) * (fileSize / int64(mcpu))
		if err = fs.Read(param.Ctx, &vec); err != nil {
			return nil, err
		}
		r2 := bufio.NewReader(r)
		line, _ := r2.ReadString('\n')
		tailSize = append(tailSize, int64(len(line)))
		offset = append(offset, vec.Entries[0].Offset)
	}

	start := int64(0)
	for i := 0; i < mcpu; i++ {
		if i+1 < mcpu {
			arr = append(arr, start)
			arr = append(arr, offset[i+1]+tailSize[i+1])
			start = offset[i+1] + tailSize[i+1]
		} else {
			arr = append(arr, start)
			arr = append(arr, -1)
		}
	}
	return arr, nil
}

func GetCompressType(param *tree.ExternParam, filepath string) string {
	if param.CompressType != "" && param.CompressType != tree.AUTO {
		return param.CompressType
	}
	index := strings.LastIndex(filepath, ".")
	if index == -1 {
		return tree.NOCOMPRESS
	}
	tail := string([]byte(filepath)[index+1:])
	switch tail {
	case "gz", "gzip":
		return tree.GZIP
	case "bz2", "bzip2":
		return tree.BZIP2
	case "lz4":
		return tree.LZ4
	default:
		return tree.NOCOMPRESS
	}
}

func getUnCompressReader(param *tree.ExternParam, filepath string, r io.ReadCloser) (io.ReadCloser, error) {
	switch strings.ToLower(GetCompressType(param, filepath)) {
	case tree.NOCOMPRESS:
		return r, nil
	case tree.GZIP, tree.GZ:
		return gzip.NewReader(r)
	case tree.BZIP2, tree.BZ2:
		return io.NopCloser(bzip2.NewReader(r)), nil
	case tree.FLATE:
		return flate.NewReader(r), nil
	case tree.ZLIB:
		return zlib.NewReader(r)
	case tree.LZ4:
		return io.NopCloser(lz4.NewReader(r)), nil
	case tree.LZW:
		return nil, moerr.NewInternalError(param.Ctx, "the compress type '%s' is not support now", param.CompressType)
	default:
		return nil, moerr.NewInternalError(param.Ctx, "the compress type '%s' is not support now", param.CompressType)
	}
}

func makeType(typ *plan.Type, flag bool) types.Type {
	if flag {
		return types.New(types.T_varchar, 0, 0)
	}
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
}

func makeBatch(param *ExternalParam, batchSize int, proc *process.Process) *batch.Batch {
	bat := batch.New(false, param.Attrs)
	//alloc space for vector
	for i := range param.Attrs {
		typ := makeType(param.Cols[i].Typ, param.ParallelLoad)
		bat.Vecs[i] = proc.GetVector(typ)
		bat.Vecs[i].PreExtend(batchSize, proc.Mp())
		bat.Vecs[i].SetLength(batchSize)
	}
	return bat
}

func deleteEnclosed(param *ExternalParam, plh *ParseLineHandler) {
	close := param.Close
	if close == '"' || close == 0 {
		return
	}
	for rowIdx := 0; rowIdx < plh.batchSize; rowIdx++ {
		line := plh.moCsvLineArray[rowIdx]
		for i := 0; i < len(line); i++ {
			len := len(line[i])
			if len < 2 {
				continue
			}
			if line[i][0] == close && line[i][len-1] == close {
				line[i] = line[i][1 : len-1]
			}
		}
	}
}

func getRealAttrCnt(attrs []string, cols []*plan.ColDef) int {
	cnt := 0
	for i := 0; i < len(attrs); i++ {
		if catalog.ContainExternalHidenCol(attrs[i]) || cols[i].Hidden {
			cnt++
		}
	}
	return len(attrs) - cnt
}

func getBatchData(param *ExternalParam, plh *ParseLineHandler, proc *process.Process) (*batch.Batch, error) {
	bat := makeBatch(param, plh.batchSize, proc)
	var err error
	deleteEnclosed(param, plh)
	unexpectEOF := false
	for rowIdx := 0; rowIdx < plh.batchSize; rowIdx++ {
		line := plh.moCsvLineArray[rowIdx]
		if param.Extern.Format == tree.JSONLINE {
			line, err = transJson2Lines(proc.Ctx, line[0], param.Attrs, param.Cols, param.Extern.JsonData, param)
			if err != nil {
				if errors.Is(err, io.ErrUnexpectedEOF) {
					logutil.Infof("unexpected EOF, wait for next batch")
					unexpectEOF = true
					continue
				}
				return nil, err
			}
			plh.moCsvLineArray[rowIdx] = line
		}
		if param.ClusterTable != nil && param.ClusterTable.GetIsClusterTable() {
			//the column account_id of the cluster table do need to be filled here
			if len(line)+1 < getRealAttrCnt(param.Attrs, param.Cols) {
				return nil, moerr.NewInternalError(proc.Ctx, ColumnCntLargerErrorInfo)
			}
		} else {
			if !param.Extern.SysTable && len(line) < getRealAttrCnt(param.Attrs, param.Cols) {
				return nil, moerr.NewInternalError(proc.Ctx, ColumnCntLargerErrorInfo)
			}
		}
		err = getOneRowData(bat, line, rowIdx, param, proc.Mp())
		if err != nil {
			return nil, err
		}
	}

	n := bat.Vecs[0].Length()
	if unexpectEOF && n > 0 {
		n--
		for i := 0; i < bat.VectorCount(); i++ {
			vec := bat.GetVector(int32(i))
			vec.SetLength(n)
		}
	}
	bat.SetZs(n, proc.Mp())
	return bat, nil
}

// getMOCSVReader get file reader from external file
func getMOCSVReader(param *ExternalParam, proc *process.Process) (*ParseLineHandler, error) {
	var err error
	param.reader, err = readFile(param, proc)
	if err != nil || param.reader == nil {
		return nil, err
	}
	param.reader, err = getUnCompressReader(param.Extern, param.Fileparam.Filepath, param.reader)
	if err != nil {
		return nil, err
	}

	var cma byte
	if param.Extern.Tail.Fields == nil {
		cma = ','
		param.Close = 0
	} else {
		cma = param.Extern.Tail.Fields.Terminated[0]
		param.Close = param.Extern.Tail.Fields.EnclosedBy
	}
	if param.Extern.Format == tree.JSONLINE {
		cma = '\t'
	}
	plh := &ParseLineHandler{
		moCsvReader:    newReaderWithOptions(param.reader, rune(cma), '#', true, false),
		moCsvLineArray: make([][]string, OneBatchMaxRow),
	}
	return plh, nil
}

func scanCsvFile(ctx context.Context, param *ExternalParam, proc *process.Process) (*batch.Batch, error) {
	var bat *batch.Batch
	var err error
	var cnt int
	_, span := trace.Start(ctx, "scanCsvFile")
	defer span.End()
	if param.plh == nil {
		param.IgnoreLine = param.IgnoreLineTag
		param.plh, err = getMOCSVReader(param, proc)
		if err != nil || param.plh == nil {
			return nil, err
		}
	}
	plh := param.plh
	finish := false
	cnt, finish, err = plh.moCsvReader.readLimitSize(proc.Ctx, param.maxBatchSize, plh.moCsvLineArray)
	if err != nil {
		logutil.Errorf("read external file meet error: %s", err.Error())
		return nil, err
	}

	if finish {
		err := param.reader.Close()
		if err != nil {
			logutil.Errorf("close file failed. err:%v", err)
		}
		param.plh = nil
		param.Fileparam.FileFin++
		if param.Fileparam.FileFin >= param.Fileparam.FileCnt {
			param.Fileparam.End = true
		}
	}
	if param.IgnoreLine != 0 {
		if !param.Extern.Parallel || param.FileOffset[0] == 0 {
			if cnt >= param.IgnoreLine {
				plh.moCsvLineArray = plh.moCsvLineArray[param.IgnoreLine:cnt]
				cnt -= param.IgnoreLine
				plh.moCsvLineArray = append(plh.moCsvLineArray, make([]string, param.IgnoreLine))
			} else {
				plh.moCsvLineArray = nil
				cnt = 0
			}
			param.IgnoreLine = 0
		}
	}
	plh.batchSize = cnt
	bat, err = getBatchData(param, plh, proc)
	if err != nil {
		return nil, err
	}
	return bat, nil
}

func getBatchFromZonemapFile(ctx context.Context, param *ExternalParam, proc *process.Process, objectReader *blockio.BlockReader) (*batch.Batch, error) {
	ctx, span := trace.Start(ctx, "getBatchFromZonemapFile")
	defer span.End()
	bat := makeBatch(param, 0, proc)
	if param.Zoneparam.offset >= len(param.Zoneparam.bs) {
		return bat, nil
	}

	rows := 0

	idxs := make([]uint16, len(param.Attrs))
	meta := param.Zoneparam.bs[param.Zoneparam.offset].GetMeta()
	colCnt := meta.BlockHeader().ColumnCount()
	for i := 0; i < len(param.Attrs); i++ {
		idxs[i] = uint16(param.Name2ColIndex[param.Attrs[i]])
		if param.Extern.SysTable && idxs[i] >= colCnt {
			idxs[i] = 0
		}
	}

	tmpBat, err := objectReader.LoadColumns(ctx, idxs, nil, param.Zoneparam.bs[param.Zoneparam.offset].BlockHeader().BlockID().Sequence(), proc.GetMPool())
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(param.Attrs); i++ {
		var vecTmp *vector.Vector
		if param.Extern.SysTable && uint16(param.Name2ColIndex[param.Attrs[i]]) >= colCnt {
			vecTmp, err = proc.AllocVectorOfRows(makeType(param.Cols[i].Typ, false), rows, nil)
			if err != nil {
				return nil, err
			}
			for j := 0; j < rows; j++ {
				nulls.Add(vecTmp.GetNulls(), uint64(j))
			}
		} else if catalog.ContainExternalHidenCol(param.Attrs[i]) {
			if rows == 0 {
				vecTmp = tmpBat.Vecs[i]
				if err != nil {
					return nil, err
				}
				rows = vecTmp.Length()
			}
			vecTmp, err = proc.AllocVectorOfRows(makeType(param.Cols[i].Typ, false), rows, nil)
			if err != nil {
				return nil, err
			}
			for j := 0; j < rows; j++ {
				err := vector.SetStringAt(vecTmp, j, param.Fileparam.Filepath, proc.GetMPool())
				if err != nil {
					return nil, err
				}
			}
		} else {
			vecTmp = tmpBat.Vecs[i]
			if err != nil {
				return nil, err
			}
			rows = vecTmp.Length()
		}
		sels := make([]int32, vecTmp.Length())
		for j := 0; j < len(sels); j++ {
			sels[j] = int32(j)
		}
		bat.Vecs[i].Union(vecTmp, sels, proc.GetMPool())
	}

	n := bat.Vecs[0].Length()
	sels := proc.Mp().GetSels()
	if n > cap(sels) {
		proc.Mp().PutSels(sels)
		sels = make([]int64, n)
	}
	bat.Zs = sels[:n]
	for k := 0; k < n; k++ {
		bat.Zs[k] = 1
	}
	if !param.Extern.QueryResult {
		param.Zoneparam.offset++
	}
	return bat, nil
}

func needRead(ctx context.Context, param *ExternalParam, proc *process.Process) bool {
	_, span := trace.Start(ctx, "needRead")
	defer span.End()

	expr := param.Filter.FilterExpr
	if expr == nil {
		return true
	}
	if param.Zoneparam.offset >= len(param.Zoneparam.bs) {
		return true
	}

	notReportErrCtx := errutil.ContextWithNoReport(proc.Ctx, true)

	meta := param.Zoneparam.bs[param.Zoneparam.offset]
	columnMap := param.Filter.columnMap
	var (
		zms  []objectio.ZoneMap
		vecs []*vector.Vector
	)

	if isMonoExpr := plan2.CheckExprIsMonotonic(proc.Ctx, expr); isMonoExpr {
		cnt := plan2.AssignAuxIdForExpr(expr, 0)
		zms = make([]objectio.ZoneMap, cnt)
		vecs = make([]*vector.Vector, cnt)
	}

	return colexec.EvaluateFilterByZoneMap(
		notReportErrCtx, proc, expr, meta, columnMap, zms, vecs)
}

func getZonemapBatch(ctx context.Context, param *ExternalParam, proc *process.Process, objectReader *blockio.BlockReader) (*batch.Batch, error) {
	var err error
	if param.Extern.QueryResult {
		param.Zoneparam.bs, err = objectReader.LoadAllBlocks(param.Ctx, proc.GetMPool())
		if err != nil {
			return nil, err
		}
	} else if param.Zoneparam.bs == nil {
		param.plh = &ParseLineHandler{}
		var err error
		param.Zoneparam.bs, err = objectReader.LoadAllBlocks(param.Ctx, proc.GetMPool())
		if err != nil {
			return nil, err
		}
	}
	if param.Zoneparam.offset >= len(param.Zoneparam.bs) {
		bat := makeBatch(param, 0, proc)
		return bat, nil
	}

	if param.Filter.exprMono {
		for !needRead(ctx, param, proc) {
			param.Zoneparam.offset++
		}
		return getBatchFromZonemapFile(ctx, param, proc, objectReader)
	} else {
		return getBatchFromZonemapFile(ctx, param, proc, objectReader)
	}
}

func scanZonemapFile(ctx context.Context, param *ExternalParam, proc *process.Process) (*batch.Batch, error) {
	if param.Filter.blockReader == nil || param.Extern.QueryResult {
		dir, _ := filepath.Split(param.Fileparam.Filepath)
		var service fileservice.FileService
		var err error
		var p fileservice.Path

		if param.Extern.QueryResult {
			service = param.Extern.FileService
		} else {

			// format filepath for local file
			fp := param.Extern.Filepath
			if p, err = fileservice.ParsePath(param.Extern.Filepath); err != nil {
				return nil, err
			} else if p.Service == "" {
				if os.IsPathSeparator(filepath.Clean(param.Extern.Filepath)[0]) {
					// absolute path
					fp = "/"
				} else {
					// relative path.
					// PS: this loop never trigger, caused by ReadDir() only support local file with absolute path
					fp = "."
				}
			}

			service, _, err = plan2.GetForETLWithType(param.Extern, fp)
			if err != nil {
				return nil, err
			}
		}
		_, ok := param.Filter.File2Size[param.Fileparam.Filepath]
		if !ok && param.Extern.QueryResult {
			e, err := service.StatFile(proc.Ctx, param.Fileparam.Filepath)
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
					return nil, moerr.NewResultFileNotFound(ctx, param.Fileparam.Filepath)
				}
				return nil, err
			}
			param.Filter.File2Size[param.Fileparam.Filepath] = e.Size
		} else if !ok {
			fs := objectio.NewObjectFS(service, dir)
			dirs, err := fs.ListDir(dir)
			if err != nil {
				return nil, err
			}
			for i := 0; i < len(dirs); i++ {
				param.Filter.File2Size[dir+dirs[i].Name] = dirs[i].Size
			}
		}

		param.Filter.blockReader, err = blockio.NewFileReader(service, param.Fileparam.Filepath)
		if err != nil {
			return nil, err
		}
	}

	_, ok := param.Filter.File2Size[param.Fileparam.Filepath]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("can' t find the filepath %s", param.Fileparam.Filepath)
	}
	bat, err := getZonemapBatch(ctx, param, proc, param.Filter.blockReader)
	if err != nil {
		return nil, err
	}

	if param.Zoneparam.offset >= len(param.Zoneparam.bs) {
		param.Filter.blockReader = nil
		param.Zoneparam.bs = nil
		param.plh = nil
		param.Fileparam.FileFin++
		if param.Fileparam.FileFin >= param.Fileparam.FileCnt {
			param.Fileparam.End = true
		}
		param.Zoneparam.offset = 0
	}
	return bat, nil
}

// ScanFileData read batch data from external file
func ScanFileData(ctx context.Context, param *ExternalParam, proc *process.Process) (*batch.Batch, error) {
	if param.Extern.QueryResult {
		return scanZonemapFile(ctx, param, proc)
	} else {
		return scanCsvFile(ctx, param, proc)
	}
}

func transJson2Lines(ctx context.Context, str string, attrs []string, cols []*plan.ColDef, jsonData string, param *ExternalParam) ([]string, error) {
	switch jsonData {
	case tree.OBJECT:
		return transJsonObject2Lines(ctx, str, attrs, cols, param)
	case tree.ARRAY:
		return transJsonArray2Lines(ctx, str, attrs, cols, param)
	default:
		return nil, moerr.NewNotSupported(ctx, "the jsonline format '%s' is not support now", jsonData)
	}
}

func transJsonObject2Lines(ctx context.Context, str string, attrs []string, cols []*plan.ColDef, param *ExternalParam) ([]string, error) {
	var (
		err error
		res = make([]string, 0, len(attrs))
	)
	if param.prevStr != "" {
		str = param.prevStr + str
		param.prevStr = ""
	}
	var jsonMap map[string]interface{}
	var decoder = json.NewDecoder(bytes.NewReader([]byte(str)))
	decoder.UseNumber()
	err = decoder.Decode(&jsonMap)
	if err != nil {
		logutil.Errorf("json unmarshal err:%v", err)
		param.prevStr = str
		return nil, err
	}
	if len(jsonMap) < getRealAttrCnt(attrs, cols) {
		return nil, moerr.NewInternalError(ctx, ColumnCntLargerErrorInfo)
	}
	for idx, attr := range attrs {
		if cols[idx].Hidden {
			continue
		}
		if val, ok := jsonMap[attr]; ok {
			if val == nil {
				res = append(res, NULL_FLAG)
				continue
			}
			tp := cols[idx].Typ.Id
			if tp != int32(types.T_json) {
				res = append(res, fmt.Sprintf("%v", val))
				continue
			}
			var bj bytejson.ByteJson
			err = bj.UnmarshalObject(val)
			if err != nil {
				return nil, err
			}
			dt, err := bj.Marshal()
			if err != nil {
				return nil, err
			}
			res = append(res, string(dt))
		} else {
			return nil, moerr.NewInvalidInput(ctx, "the attr %s is not in json", attr)
		}
	}
	return res, nil
}

func transJsonArray2Lines(ctx context.Context, str string, attrs []string, cols []*plan.ColDef, param *ExternalParam) ([]string, error) {
	var (
		err error
		res = make([]string, 0, len(attrs))
	)
	if param.prevStr != "" {
		str = param.prevStr + str
		param.prevStr = ""
	}
	var jsonArray []interface{}
	var decoder = json.NewDecoder(bytes.NewReader([]byte(str)))
	decoder.UseNumber()
	err = decoder.Decode(&jsonArray)
	if err != nil {
		param.prevStr = str
		return nil, err
	}
	if len(jsonArray) < getRealAttrCnt(attrs, cols) {
		return nil, moerr.NewInternalError(ctx, ColumnCntLargerErrorInfo)
	}
	for idx, val := range jsonArray {
		if val == nil {
			res = append(res, NULL_FLAG)
			continue
		}
		tp := cols[idx].Typ.Id
		if tp != int32(types.T_json) {
			res = append(res, fmt.Sprintf("%v", val))
			continue
		}
		var bj bytejson.ByteJson
		err = bj.UnmarshalObject(val)
		if err != nil {
			return nil, err
		}
		dt, err := bj.Marshal()
		if err != nil {
			return nil, err
		}
		res = append(res, string(dt))
	}
	return res, nil
}

func getNullFlag(param *ExternalParam, attr, field string) bool {
	list := param.Extern.NullMap[attr]
	for i := 0; i < len(list); i++ {
		field = strings.ToLower(field)
		if list[i] == field {
			return true
		}
	}
	return false
}

const NULL_FLAG = "\\N"

func getStrFromLine(line []string, colIdx int, param *ExternalParam) string {
	if catalog.ContainExternalHidenCol(param.Attrs[colIdx]) {
		return param.Fileparam.Filepath
	} else {
		str := line[param.Name2ColIndex[param.Attrs[colIdx]]]
		if param.Close != 0 {
			tmp := strings.TrimSpace(str)
			if len(tmp) >= 2 && tmp[0] == param.Close && tmp[len(tmp)-1] == param.Close {
				return tmp[1 : len(tmp)-1]
			}
		}
		return str
	}
}

func getOneRowData(bat *batch.Batch, line []string, rowIdx int, param *ExternalParam, mp *mpool.MPool) error {
	for colIdx := range param.Attrs {
		vec := bat.Vecs[colIdx]
		if param.Cols[colIdx].Hidden {
			nulls.Add(vec.GetNulls(), uint64(rowIdx))
			continue
		}
		field := getStrFromLine(line, colIdx, param)
		id := types.T(param.Cols[colIdx].Typ.Id)
		if id != types.T_char && id != types.T_varchar && id != types.T_json &&
			id != types.T_binary && id != types.T_varbinary && id != types.T_blob && id != types.T_text {
			field = strings.TrimSpace(field)
		}
		isNullOrEmpty := field == NULL_FLAG
		if id != types.T_char && id != types.T_varchar &&
			id != types.T_binary && id != types.T_varbinary && id != types.T_json && id != types.T_blob && id != types.T_text {
			isNullOrEmpty = isNullOrEmpty || len(field) == 0
		}
		isNullOrEmpty = isNullOrEmpty || (getNullFlag(param, param.Attrs[colIdx], field))
		if isNullOrEmpty {
			nulls.Add(vec.GetNulls(), uint64(rowIdx))
			continue
		}
		if param.ParallelLoad {
			err := vector.SetStringAt(vec, rowIdx, field, mp)
			if err != nil {
				return err
			}
			continue
		}

		switch id {
		case types.T_bool:
			b, err := types.ParseBool(field)
			if err != nil {
				return moerr.NewInternalError(param.Ctx, "the input value '%s' is not bool type for column %d", field, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, b); err != nil {
				return err
			}
		case types.T_int8:
			d, err := strconv.ParseInt(field, 10, 8)
			if err == nil {
				if err := vector.SetFixedAt(vec, rowIdx, int8(d)); err != nil {
					return err
				}
			} else {
				f, err := strconv.ParseFloat(field, 64)
				if err != nil || f < math.MinInt8 || f > math.MaxInt8 {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int8 type for column %d", field, colIdx)
				}
				if err := vector.SetFixedAt(vec, rowIdx, int8(f)); err != nil {
					return err
				}
			}
		case types.T_int16:
			d, err := strconv.ParseInt(field, 10, 16)
			if err == nil {
				if err := vector.SetFixedAt(vec, rowIdx, int16(d)); err != nil {
					return err
				}
			} else {
				f, err := strconv.ParseFloat(field, 64)
				if err != nil || f < math.MinInt16 || f > math.MaxInt16 {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int16 type for column %d", field, colIdx)
				}
				if err := vector.SetFixedAt(vec, rowIdx, int16(f)); err != nil {
					return err
				}
			}
		case types.T_int32:
			d, err := strconv.ParseInt(field, 10, 32)
			if err == nil {
				if err := vector.SetFixedAt(vec, rowIdx, int32(d)); err != nil {
					return err
				}
			} else {
				f, err := strconv.ParseFloat(field, 64)
				if err != nil || f < math.MinInt32 || f > math.MaxInt32 {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int32 type for column %d", field, colIdx)
				}
				if err := vector.SetFixedAt(vec, rowIdx, int32(f)); err != nil {
					return err
				}
			}
		case types.T_int64:
			d, err := strconv.ParseInt(field, 10, 64)
			if err == nil {
				if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
					return err
				}
			} else {
				f, err := strconv.ParseFloat(field, 64)
				if err != nil || f < math.MinInt64 || f > math.MaxInt64 {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int64 type for column %d", field, colIdx)
				}
				if err := vector.SetFixedAt(vec, rowIdx, int64(f)); err != nil {
					return err
				}
			}
		case types.T_uint8:
			d, err := strconv.ParseUint(field, 10, 8)
			if err == nil {
				if err := vector.SetFixedAt(vec, rowIdx, uint8(d)); err != nil {
					return err
				}
			} else {
				f, err := strconv.ParseFloat(field, 64)
				if err != nil || f < 0 || f > math.MaxUint8 {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint8 type for column %d", field, colIdx)
				}
				if err := vector.SetFixedAt(vec, rowIdx, uint8(f)); err != nil {
					return err
				}
			}
		case types.T_uint16:
			d, err := strconv.ParseUint(field, 10, 16)
			if err == nil {
				if err := vector.SetFixedAt(vec, rowIdx, uint16(d)); err != nil {
					return err
				}
			} else {
				f, err := strconv.ParseFloat(field, 64)
				if err != nil || f < 0 || f > math.MaxUint16 {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint16 type for column %d", field, colIdx)
				}
				if err := vector.SetFixedAt(vec, rowIdx, uint16(f)); err != nil {
					return err
				}
			}
		case types.T_uint32:
			d, err := strconv.ParseUint(field, 10, 32)
			if err == nil {
				if err := vector.SetFixedAt(vec, rowIdx, uint32(d)); err != nil {
					return err
				}
			} else {
				f, err := strconv.ParseFloat(field, 64)
				if err != nil || f < 0 || f > math.MaxUint32 {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint32 type for column %d", field, colIdx)
				}
				if err := vector.SetFixedAt(vec, rowIdx, uint32(f)); err != nil {
					return err
				}
			}
		case types.T_uint64:
			d, err := strconv.ParseUint(field, 10, 64)
			if err == nil {
				if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
					return err
				}
			} else {
				f, err := strconv.ParseFloat(field, 64)
				if err != nil || f < 0 || f > math.MaxUint64 {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint64 type for column %d", field, colIdx)
				}
				if err := vector.SetFixedAt(vec, rowIdx, uint64(f)); err != nil {
					return err
				}
			}
		case types.T_float32:
			// origin float32 data type
			if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
				d, err := strconv.ParseFloat(field, 32)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not float32 type for column %d", field, colIdx)
				}
				if err := vector.SetFixedAt(vec, rowIdx, float32(d)); err != nil {
					return err
				}
			} else {
				d, err := types.ParseDecimal128(field, vec.GetType().Width, vec.GetType().Scale)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not float32 type for column %d", field, colIdx)
				}
				if err := vector.SetFixedAt(vec, rowIdx, float32(types.Decimal128ToFloat64(d, vec.GetType().Scale))); err != nil {
					return err
				}
			}
		case types.T_float64:
			// origin float64 data type
			if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not float64 type for column %d", field, colIdx)
				}
				if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
					return err
				}
			} else {
				d, err := types.ParseDecimal128(field, vec.GetType().Width, vec.GetType().Scale)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not float32 type for column %d", field, colIdx)
				}
				if err := vector.SetFixedAt(vec, rowIdx, types.Decimal128ToFloat64(d, vec.GetType().Scale)); err != nil {
					return err
				}
			}
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
			// XXX Memory accounting?
			err := vector.SetStringAt(vec, rowIdx, field, mp)
			if err != nil {
				return err
			}
		case types.T_json:
			var jsonBytes []byte
			if param.Extern.Format != tree.CSV {
				jsonBytes = []byte(field)
			} else {
				byteJson, err := types.ParseStringToByteJson(field)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not json type for column %d", field, colIdx)
				}
				jsonBytes, err = types.EncodeJson(byteJson)
				if err != nil {
					logutil.Errorf("encode json[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not json type for column %d", field, colIdx)
				}
			}

			err := vector.SetBytesAt(vec, rowIdx, jsonBytes, mp)
			if err != nil {
				return err
			}
		case types.T_date:
			d, err := types.ParseDateCast(field)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not Date type for column %d", field, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
				return err
			}
		case types.T_time:
			d, err := types.ParseTime(field, vec.GetType().Scale)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not Time type for column %d", field, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
				return err
			}
		case types.T_datetime:
			d, err := types.ParseDatetime(field, vec.GetType().Scale)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not Datetime type for column %d", field, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
				return err
			}
		case types.T_decimal64:
			d, err := types.ParseDecimal64(field, vec.GetType().Width, vec.GetType().Scale)
			if err != nil {
				// we tolerate loss of digits.
				if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is invalid Decimal64 type for column %d", field, colIdx)
				}
			}
			if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
				return err
			}
		case types.T_decimal128:
			d, err := types.ParseDecimal128(field, vec.GetType().Width, vec.GetType().Scale)
			if err != nil {
				// we tolerate loss of digits.
				if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is invalid Decimal128 type for column %d", field, colIdx)
				}
			}
			if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
				return err
			}
		case types.T_timestamp:
			t := time.Local
			d, err := types.ParseTimestamp(t, field, vec.GetType().Scale)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not Timestamp type for column %d", field, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
				return err
			}
		case types.T_uuid:
			d, err := types.ParseUuid(field)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uuid type for column %d", field, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
				return err
			}
		default:
			return moerr.NewInternalError(param.Ctx, "the value type %d is not support now", param.Cols[rowIdx].Typ.Id)
		}
	}
	return nil
}

// Read reads len count records from r.
// Each record is a slice of fields.
// A successful call returns err == nil, not err == io.EOF. Because ReadAll is
// defined to read until EOF, it does not treat end of file as an error to be
// reported.
func (r *moCSVReader) readLimitSize(ctx context.Context, size uint64, records [][]string) (int, bool, error) {
	return readCountStringLimitSize(r.rCsv, ctx, size, records)
}

func readCountStringLimitSize(r *csv.Reader, ctx context.Context, size uint64, records [][]string) (int, bool, error) {
	var curBatchSize uint64 = 0
	for i := 0; i < OneBatchMaxRow; i++ {
		select {
		case <-ctx.Done():
			return i, true, nil
		default:
		}
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				return i, true, nil
			}
			return i, true, err
		}
		records[i] = record
		for j := 0; j < len(record); j++ {
			curBatchSize += uint64(len(record[j]))
		}
		if curBatchSize >= size {
			return i + 1, false, nil
		}
	}
	return OneBatchMaxRow, false, nil
}

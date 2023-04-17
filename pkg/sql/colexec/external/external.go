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
	"sync/atomic"
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
	"github.com/pierrec/lz4"
)

var (
	ONE_BATCH_MAX_ROW  = int(options.DefaultBlockMaxRows)
	S3_PARALLEL_MAXNUM = 10
)

var (
	STATEMENT_ACCOUNT = "account"
)

func String(arg any, buf *bytes.Buffer) {
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
	param.Filter.columnMap, columns, param.Filter.maxCol = plan2.GetColumnsByExpr(param.Filter.FilterExpr, param.tableDef)
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

func makeFilepathBatch(node *plan.Node, proc *process.Process, filterList []*plan.Expr, fileList []string) *batch.Batch {
	num := len(node.TableDef.Cols)
	bat := &batch.Batch{
		Attrs: make([]string, num),
		Vecs:  make([]*vector.Vector, num),
		Zs:    make([]int64, len(fileList)),
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
	bat := makeFilepathBatch(node, proc, filterList, fileList)
	filter := colexec.RewriteFilterExprList(filterList)
	vec, err := colexec.EvalExpr(bat, proc, filter)
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
	node.FilterList = filterList2
	return fileListTmp, fileSizeTmp, nil
}

func FilterFileList(ctx context.Context, node *plan.Node, proc *process.Process, fileList []string, fileSize []int64) ([]string, []int64, error) {
	return filterByAccountAndFilename(ctx, node, proc, fileList, fileSize)
}

func IsSysTable(dbName string, tableName string) bool {
	if dbName == "system" {
		return tableName == "statement_info" || tableName == "rawlog"
	} else if dbName == "system_metrics" {
		return tableName == "metric"
	}
	return false
}

func ReadFile(param *ExternalParam, proc *process.Process) (io.ReadCloser, error) {
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
		vec.Entries[0].Offset = int64(param.FileOffset[0])
		vec.Entries[0].Size = int64(param.FileOffset[1] - param.FileOffset[0])
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

func ReadFileOffset(param *tree.ExternParam, proc *process.Process, mcpu int, fileSize int64) ([]int64, error) {
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

func getCompressType(param *tree.ExternParam, filepath string) string {
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
	switch strings.ToLower(getCompressType(param, filepath)) {
	case tree.NOCOMPRESS:
		return r, nil
	case tree.GZIP, tree.GZ:
		r, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		return r, nil
	case tree.BZIP2, tree.BZ2:
		return io.NopCloser(bzip2.NewReader(r)), nil
	case tree.FLATE:
		r = flate.NewReader(r)
		return r, nil
	case tree.ZLIB:
		r, err := zlib.NewReader(r)
		if err != nil {
			return nil, err
		}
		return r, nil
	case tree.LZ4:
		return io.NopCloser(lz4.NewReader(r)), nil
	case tree.LZW:
		return nil, moerr.NewInternalError(param.Ctx, "the compress type '%s' is not support now", param.CompressType)
	default:
		return nil, moerr.NewInternalError(param.Ctx, "the compress type '%s' is not support now", param.CompressType)
	}
}

func makeType(Cols []*plan.ColDef, index int) types.Type {
	return types.New(types.T(Cols[index].Typ.Id), Cols[index].Typ.Width, Cols[index].Typ.Scale)
}

func makeBatch(param *ExternalParam, batchSize int, proc *process.Process) *batch.Batch {
	batchData := batch.New(true, param.Attrs)
	//alloc space for vector
	for i := 0; i < len(param.Attrs); i++ {
		typ := makeType(param.Cols, i)
		vec, _ := proc.AllocVectorOfRows(typ, batchSize, nil)
		//vec.SetOriginal(false)
		batchData.Vecs[i] = vec
	}
	return batchData
}

func deleteEnclosed(param *ExternalParam, plh *ParseLineHandler) {
	close := param.Extern.Tail.Fields.EnclosedBy
	if close == '"' || close == 0 {
		return
	}
	for rowIdx := 0; rowIdx < plh.batchSize; rowIdx++ {
		Line := plh.moCsvLineArray[rowIdx]
		for i := 0; i < len(Line); i++ {
			len := len(Line[i])
			if len < 2 {
				continue
			}
			if Line[i][0] == close && Line[i][len-1] == close {
				Line[i] = Line[i][1 : len-1]
			}
		}
	}
}

func getRealAttrCnt(attrs []string) int {
	cnt := 0
	for i := 0; i < len(attrs); i++ {
		if catalog.ContainExternalHidenCol(attrs[i]) {
			cnt++
		}
	}
	return len(attrs) - cnt
}

func GetBatchData(param *ExternalParam, plh *ParseLineHandler, proc *process.Process) (*batch.Batch, error) {
	bat := makeBatch(param, plh.batchSize, proc)
	var (
		Line []string
		err  error
	)
	deleteEnclosed(param, plh)
	unexpectEOF := false
	for rowIdx := 0; rowIdx < plh.batchSize; rowIdx++ {
		Line = plh.moCsvLineArray[rowIdx]
		if param.Extern.Format == tree.JSONLINE {
			Line, err = transJson2Lines(proc.Ctx, Line[0], param.Attrs, param.Cols, param.Extern.JsonData, param)
			if err != nil {
				if errors.Is(err, io.ErrUnexpectedEOF) {
					logutil.Infof("unexpected EOF, wait for next batch")
					unexpectEOF = true
					continue
				}
				return nil, err
			}
			plh.moCsvLineArray[rowIdx] = Line
		}
		if param.ClusterTable != nil && param.ClusterTable.GetIsClusterTable() {
			//the column account_id of the cluster table do need to be filled here
			if len(Line)+1 < getRealAttrCnt(param.Attrs) {
				return nil, moerr.NewInternalError(proc.Ctx, ColumnCntLargerErrorInfo())
			}
		} else {
			if !param.Extern.SysTable && len(Line) < getRealAttrCnt(param.Attrs) {
				return nil, moerr.NewInternalError(proc.Ctx, ColumnCntLargerErrorInfo())
			}
		}
		err = getOneRowData(bat, Line, rowIdx, param, proc.Mp())
		if err != nil {
			return nil, err
		}
	}

	n := bat.Vecs[0].Length()
	if unexpectEOF && n > 0 {
		n--
		for i := 0; i < len(bat.Vecs); i++ {
			newVec, err := proc.AllocVectorOfRows(*bat.Vecs[i].GetType(), n, nil)
			if err != nil {
				return nil, err
			}
			nulls.Set(newVec.GetNulls(), bat.Vecs[i].GetNulls())
			for j := int64(0); j < int64(n); j++ {
				if newVec.GetNulls().Contains(uint64(j)) {
					continue
				}
				err := newVec.Copy(bat.Vecs[i], j, j, proc.Mp())
				if err != nil {
					return nil, err
				}
			}
			bat.Vecs[i].Free(proc.Mp())
			bat.Vecs[i] = newVec
		}
	}
	sels := proc.Mp().GetSels()
	if n > cap(sels) {
		proc.Mp().PutSels(sels)
		sels = make([]int64, n)
	}
	bat.Zs = sels[:n]
	for k := 0; k < n; k++ {
		bat.Zs[k] = 1
	}
	return bat, nil
}

// GetmocsvReader get file reader from external file
func GetMOcsvReader(param *ExternalParam, proc *process.Process) (*ParseLineHandler, error) {
	var err error
	param.reader, err = ReadFile(param, proc)
	if err != nil || param.reader == nil {
		return nil, err
	}
	param.reader, err = getUnCompressReader(param.Extern, param.Fileparam.Filepath, param.reader)
	if err != nil {
		return nil, err
	}

	channelSize := 100
	plh := &ParseLineHandler{}
	plh.moCsvGetParsedLinesChan = atomic.Value{}
	plh.moCsvGetParsedLinesChan.Store(make(chan LineOut, channelSize))
	if param.Extern.Tail.Fields == nil {
		param.Extern.Tail.Fields = &tree.Fields{Terminated: ","}
	}
	if param.Extern.Format == tree.JSONLINE {
		param.Extern.Tail.Fields.Terminated = "\t"
	}
	plh.moCsvReader = NewReaderWithOptions(param.reader,
		rune(param.Extern.Tail.Fields.Terminated[0]),
		'#',
		true,
		false)
	plh.moCsvLineArray = make([][]string, ONE_BATCH_MAX_ROW)
	return plh, nil
}

func ScanCsvFile(ctx context.Context, param *ExternalParam, proc *process.Process) (*batch.Batch, error) {
	var bat *batch.Batch
	var err error
	var cnt int
	_, span := trace.Start(ctx, "ScanCsvFile")
	defer span.End()
	if param.plh == nil {
		param.IgnoreLine = param.IgnoreLineTag
		param.plh, err = GetMOcsvReader(param, proc)
		if err != nil || param.plh == nil {
			return nil, err
		}
	}
	plh := param.plh
	finish := false
	cnt, finish, err = plh.moCsvReader.ReadLimitSize(ONE_BATCH_MAX_ROW, proc.Ctx, param.maxBatchSize, plh.moCsvLineArray)
	if err != nil {
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
			} else {
				plh.moCsvLineArray = nil
				cnt = 0
			}
			param.IgnoreLine = 0
		}
	}
	plh.batchSize = cnt
	bat, err = GetBatchData(param, plh, proc)
	if err != nil {
		return nil, err
	}
	bat.Cnt = 1
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

	tmpBat, err := objectReader.LoadColumns(ctx, idxs, param.Zoneparam.bs[param.Zoneparam.offset].BlockHeader().BlockID().Sequence(), proc.GetMPool())
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(param.Attrs); i++ {
		var vecTmp *vector.Vector
		if param.Extern.SysTable && uint16(param.Name2ColIndex[param.Attrs[i]]) >= colCnt {
			vecTmp, err = proc.AllocVectorOfRows(makeType(param.Cols, i), rows, nil)
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
			vecTmp, err = proc.AllocVectorOfRows(makeType(param.Cols, i), rows, nil)
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

func needRead(ctx context.Context, param *ExternalParam, proc *process.Process, objectReader *blockio.BlockReader) bool {
	_, span := trace.Start(ctx, "needRead")
	defer span.End()
	if param.Zoneparam.offset >= len(param.Zoneparam.bs) {
		return true
	}
	indexes, err := objectReader.LoadZoneMap(context.Background(), param.Filter.columns,
		param.Zoneparam.bs[param.Zoneparam.offset], proc.GetMPool())
	if err != nil {
		return true
	}

	notReportErrCtx := errutil.ContextWithNoReport(proc.Ctx, true)
	// if expr match no columns, just eval expr
	if len(param.Filter.columns) == 0 {
		bat := batch.NewWithSize(0)
		defer bat.Clean(proc.Mp())
		ifNeed, err := plan2.EvalFilterExpr(notReportErrCtx, param.Filter.FilterExpr, bat, proc)
		if err != nil {
			return true
		}
		return ifNeed
	}

	dataLength := len(param.Filter.columns)
	datas := make([][2]any, dataLength)
	dataTypes := make([]uint8, dataLength)
	for i := 0; i < dataLength; i++ {
		idx := param.Filter.defColumns[i]
		dataTypes[i] = uint8(param.Cols[idx].Typ.Id)

		zm := indexes[i]
		if err != nil {
			return true
		}
		min := zm.GetMin()
		max := zm.GetMax()
		if min == nil || max == nil {
			return true
		}
		datas[i] = [2]any{min, max}
	}
	// use all min/max data to build []vectors.
	buildVectors := plan2.BuildVectorsByData(datas, dataTypes, proc.Mp())
	bat := batch.NewWithSize(param.Filter.maxCol + 1)
	defer bat.Clean(proc.Mp())
	for k, v := range param.Filter.columnMap {
		for i, realIdx := range param.Filter.defColumns {
			if int(realIdx) == v {
				bat.SetVector(int32(k), buildVectors[i])
				break
			}
		}
	}
	bat.SetZs(buildVectors[0].Length(), proc.Mp())

	ifNeed, err := plan2.EvalFilterExpr(notReportErrCtx, param.Filter.FilterExpr, bat, proc)
	if err != nil {
		return true
	}
	return ifNeed
}

func getZonemapBatch(ctx context.Context, param *ExternalParam, proc *process.Process, size int64, objectReader *blockio.BlockReader) (*batch.Batch, error) {
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
		for !needRead(ctx, param, proc, objectReader) {
			param.Zoneparam.offset++
		}
		return getBatchFromZonemapFile(ctx, param, proc, objectReader)
	} else {
		return getBatchFromZonemapFile(ctx, param, proc, objectReader)
	}
}

func ScanZonemapFile(ctx context.Context, param *ExternalParam, proc *process.Process) (*batch.Batch, error) {
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

	size, ok := param.Filter.File2Size[param.Fileparam.Filepath]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("can' t find the filepath %s", param.Fileparam.Filepath)
	}
	bat, err := getZonemapBatch(ctx, param, proc, size, param.Filter.blockReader)
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
	if strings.HasSuffix(param.Fileparam.Filepath, ".tae") || param.Extern.QueryResult {
		return ScanZonemapFile(ctx, param, proc)
	} else {
		return ScanCsvFile(ctx, param, proc)
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
	if len(jsonMap) < len(attrs) {
		return nil, moerr.NewInternalError(ctx, ColumnCntLargerErrorInfo())
	}
	for idx, attr := range attrs {
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
	if len(jsonArray) < len(attrs) {
		return nil, moerr.NewInternalError(ctx, ColumnCntLargerErrorInfo())
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

func judgeInteger(field string) bool {
	for i := 0; i < len(field); i++ {
		if field[i] == '-' || field[i] == '+' {
			continue
		}
		if field[i] > '9' || field[i] < '0' {
			return false
		}
	}
	return true
}

func getStrFromLine(Line []string, colIdx int, param *ExternalParam) string {
	if catalog.ContainExternalHidenCol(param.Attrs[colIdx]) {
		return param.Fileparam.Filepath
	} else {
		var str string
		if param.Extern.SysTable && int(param.Name2ColIndex[param.Attrs[colIdx]]) >= len(Line) {
			str = "\\N"
		} else {
			str = Line[param.Name2ColIndex[param.Attrs[colIdx]]]
		}
		if param.Extern.Tail.Fields.EnclosedBy != 0 {
			tmp := strings.TrimSpace(str)
			if len(tmp) >= 2 && tmp[0] == param.Extern.Tail.Fields.EnclosedBy && tmp[len(tmp)-1] == param.Extern.Tail.Fields.EnclosedBy {
				return tmp[1 : len(tmp)-1]
			}
		}
		return str
	}
}

func getOneRowData(bat *batch.Batch, Line []string, rowIdx int, param *ExternalParam, mp *mpool.MPool) error {
	for colIdx := range param.Attrs {
		//for cluster table, the column account_id need not be filled here
		if param.ClusterTable.GetIsClusterTable() && int(param.ClusterTable.GetColumnIndexOfAccountId()) == colIdx {
			continue
		}
		field := getStrFromLine(Line, colIdx, param)
		id := types.T(param.Cols[colIdx].Typ.Id)
		if id != types.T_char && id != types.T_varchar && id != types.T_json &&
			id != types.T_binary && id != types.T_varbinary && id != types.T_blob && id != types.T_text {
			field = strings.TrimSpace(field)
		}
		vec := bat.Vecs[colIdx]
		isNullOrEmpty := field == NULL_FLAG
		if id != types.T_char && id != types.T_varchar &&
			id != types.T_binary && id != types.T_varbinary && id != types.T_json && id != types.T_blob && id != types.T_text {
			isNullOrEmpty = isNullOrEmpty || len(field) == 0
		}
		isNullOrEmpty = isNullOrEmpty || (getNullFlag(param, param.Attrs[colIdx], field))
		switch id {
		case types.T_bool:
			cols := vector.MustFixedCol[bool](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				if field == "true" || field == "1" {
					cols[rowIdx] = true
				} else if field == "false" || field == "0" {
					cols[rowIdx] = false
				} else {
					return moerr.NewInternalError(param.Ctx, "the input value '%s' is not bool type for column %d", field, colIdx)
				}
			}
		case types.T_int8:
			cols := vector.MustFixedCol[int8](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseInt(field, 10, 8)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int8 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int8(d)
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < math.MinInt8 || d > math.MaxInt8 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int8 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int8(d)
				}
			}
		case types.T_int16:
			cols := vector.MustFixedCol[int16](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseInt(field, 10, 16)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int16 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int16(d)
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < math.MinInt16 || d > math.MaxInt16 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int16 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int16(d)
				}
			}
		case types.T_int32:
			cols := vector.MustFixedCol[int32](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseInt(field, 10, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int32 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int32(d)
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < math.MinInt32 || d > math.MaxInt32 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int32 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int32(d)
				}
			}
		case types.T_int64:
			cols := vector.MustFixedCol[int64](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseInt(field, 10, 64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int64 type for column %d", field, colIdx)
					}
					cols[rowIdx] = d
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < math.MinInt64 || d > math.MaxInt64 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int64 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int64(d)
				}
			}
		case types.T_uint8:
			cols := vector.MustFixedCol[uint8](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseUint(field, 10, 8)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint8 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint8(d)
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < 0 || d > math.MaxUint8 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint8 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint8(d)
				}
			}
		case types.T_uint16:
			cols := vector.MustFixedCol[uint16](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseUint(field, 10, 16)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint16 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint16(d)
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < 0 || d > math.MaxUint16 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint16 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint16(d)
				}
			}
		case types.T_uint32:
			cols := vector.MustFixedCol[uint32](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseUint(field, 10, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint32 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint32(d)
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < 0 || d > math.MaxUint32 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint32 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint32(d)
				}
			}
		case types.T_uint64:
			cols := vector.MustFixedCol[uint64](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseUint(field, 10, 64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint64 type for column %d", field, colIdx)
					}
					cols[rowIdx] = d
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < 0 || d > math.MaxUint64 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint64 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint64(d)
				}
			}
		case types.T_float32:
			cols := vector.MustFixedCol[float32](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				// origin float32 data type
				if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
					d, err := strconv.ParseFloat(field, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not float32 type for column %d", field, colIdx)
					}
					cols[rowIdx] = float32(d)
					continue
				}
				d, err := types.ParseDecimal128(field, vec.GetType().Width, vec.GetType().Scale)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not float32 type for column %d", field, colIdx)
				}
				cols[rowIdx] = float32(types.Decimal128ToFloat64(d, vec.GetType().Scale))
			}
		case types.T_float64:
			cols := vector.MustFixedCol[float64](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				// origin float64 data type
				if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not float64 type for column %d", field, colIdx)
					}
					cols[rowIdx] = d
					continue
				}
				d, err := types.ParseDecimal128(field, vec.GetType().Width, vec.GetType().Scale)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not float64 type for column %d", field, colIdx)
				}
				cols[rowIdx] = types.Decimal128ToFloat64(d, vec.GetType().Scale)
			}
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				// XXX Memory accounting?
				err := vector.SetStringAt(vec, rowIdx, field, mp)
				if err != nil {
					return err
				}
			}
		case types.T_json:
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				var (
					byteJson  bytejson.ByteJson
					err       error
					jsonBytes []byte
				)
				if param.Extern.Format == tree.CSV {
					byteJson, err = types.ParseStringToByteJson(field)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not json type for column %d", field, colIdx)
					}
					jsonBytes, err = types.EncodeJson(byteJson)
					if err != nil {
						logutil.Errorf("encode json[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is not json type for column %d", field, colIdx)
					}
				} else { //jsonline
					jsonBytes = []byte(field)
				}
				err = vector.SetBytesAt(vec, rowIdx, jsonBytes, mp)
				if err != nil {
					return err
				}
			}
		case types.T_date:
			cols := vector.MustFixedCol[types.Date](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				d, err := types.ParseDateCast(field)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not Date type for column %d", field, colIdx)
				}
				cols[rowIdx] = d
			}
		case types.T_time:
			cols := vector.MustFixedCol[types.Time](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				d, err := types.ParseTime(field, vec.GetType().Scale)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not Time type for column %d", field, colIdx)
				}
				cols[rowIdx] = d
			}
		case types.T_datetime:
			cols := vector.MustFixedCol[types.Datetime](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				d, err := types.ParseDatetime(field, vec.GetType().Scale)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not Datetime type for column %d", field, colIdx)
				}
				cols[rowIdx] = d
			}
		case types.T_decimal64:
			cols := vector.MustFixedCol[types.Decimal64](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				d, err := types.ParseDecimal64(field, vec.GetType().Width, vec.GetType().Scale)
				if err != nil {
					// we tolerate loss of digits.
					if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is invalid Decimal64 type for column %d", field, colIdx)
					}
				}
				cols[rowIdx] = d
			}
		case types.T_decimal128:
			cols := vector.MustFixedCol[types.Decimal128](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				d, err := types.ParseDecimal128(field, vec.GetType().Width, vec.GetType().Scale)
				if err != nil {
					// we tolerate loss of digits.
					if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError(param.Ctx, "the input value '%v' is invalid Decimal128 type for column %d", field, colIdx)
					}
				}
				cols[rowIdx] = d
			}
		case types.T_timestamp:
			cols := vector.MustFixedCol[types.Timestamp](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				t := time.Local
				d, err := types.ParseTimestamp(t, field, vec.GetType().Scale)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not Timestamp type for column %d", field, colIdx)
				}
				cols[rowIdx] = d
			}
		case types.T_uuid:
			cols := vector.MustFixedCol[types.Uuid](vec)
			if isNullOrEmpty {
				nulls.Add(vec.GetNulls(), uint64(rowIdx))
			} else {
				d, err := types.ParseUuid(field)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uuid type for column %d", field, colIdx)
				}
				cols[rowIdx] = d
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
func (r *MOCsvReader) ReadLimitSize(cnt int, ctx context.Context, size uint64, records [][]string) (int, bool, error) {
	if !r.first {
		r.first = true
		r.rCsv = csv.NewReader(r.r)
		r.rCsv.LazyQuotes = r.LazyQuotes
		r.rCsv.TrimLeadingSpace = r.TrimLeadingSpace
		r.rCsv.Comment = r.Comment
		r.rCsv.Comma = r.Comma
		r.rCsv.FieldsPerRecord = r.FieldsPerRecord
		r.rCsv.ReuseRecord = r.ReuseRecord
	}
	cnt2, finish, err := ReadCountStringLimitSize(r.rCsv, ctx, cnt, size, records)
	if err != nil {
		return cnt2, finish, err
	}
	return cnt2, finish, nil
}

func ReadCountStringLimitSize(r *csv.Reader, ctx context.Context, cnt int, size uint64, records [][]string) (int, bool, error) {
	var curBatchSize uint64 = 0
	quit := false
	for i := 0; i < cnt; i++ {
		select {
		case <-ctx.Done():
			quit = true
		default:
		}
		if quit {
			return i, true, nil
		}
		record, err := r.Read()
		if err == io.EOF {
			return i, true, nil
		}
		if err != nil {
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
	return cnt, false, nil
}

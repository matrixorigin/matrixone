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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/crt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	OneBatchMaxRow   = int(options.DefaultBlockMaxRows)
	S3ParallelMaxnum = 10
)

var (
	STATEMENT_ACCOUNT = "account"
)

const opName = "external"

func (external *External) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": external output")
}

func (external *External) OpType() vm.OpType {
	return vm.External
}

func (external *External) Prepare(proc *process.Process) error {
	_, span := trace.Start(proc.Ctx, "ExternalPrepare")
	defer span.End()

	if external.OpAnalyzer == nil {
		external.OpAnalyzer = process.NewAnalyzer(external.GetIdx(), external.IsFirst, external.IsLast, "external")
	} else {
		external.OpAnalyzer.Reset()
	}

	param := external.Es
	if proc.GetLim().MaxMsgSize == 0 {
		param.maxBatchSize = uint64(morpc.GetMessageSize())
	} else {
		param.maxBatchSize = proc.GetLim().MaxMsgSize
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
		param.Extern.FileService = proc.Base.FileService
	}
	if !loadFormatIsValid(param.Extern) {
		return moerr.NewNYIf(proc.Ctx, "load format '%s'", param.Extern.Format)
	}

	// File list check
	if len(param.FileList) == 0 && param.Extern.ScanType != tree.INLINE {
		logutil.Warnf("no such file '%s'", param.Extern.Filepath)
		param.Fileparam.End = true
	}
	param.Fileparam.FileCnt = len(param.FileList)
	// INLINE mode has no FileList, set FileCnt=1 as virtual single file
	if param.Extern.ScanType == tree.INLINE {
		param.Fileparam.FileCnt = 1
	}
	param.Ctx = proc.Ctx

	// Filter public preprocessing
	if param.Filter == nil {
		param.Filter = &FilterParam{}
	}
	name2ColIndex := make(map[string]int32, len(param.Cols))
	for i, col := range param.Cols {
		name2ColIndex[col.Name] = int32(i)
	}
	tableDef := &plan.TableDef{Name2ColIndex: name2ColIndex}
	param.Filter.columnMap, _, _, _ = plan2.GetColumnsByExpr(
		param.Filter.FilterExpr, tableDef)

	// Deep copy FilterExpr + one-time AssignAuxIdForExpr
	if param.Filter.FilterExpr != nil {
		param.Filter.FilterExpr = plan2.DeepCopyExpr(param.Filter.FilterExpr)
		param.Filter.AuxIdCnt = plan2.AssignAuxIdForExpr(param.Filter.FilterExpr, 0)
	} else {
		param.Filter.AuxIdCnt = 0
	}

	// Defensive close old reader
	if external.reader != nil {
		logutil.Debugf("external Prepare: closing leftover reader from previous execution")
		external.reader.Close()
		external.reader = nil
		external.fileOpened = false
	}

	// Create reader (single dispatch point)
	switch {
	case param.Extern.ExternType == int32(plan.ExternType_RESULT_SCAN):
		external.reader = NewZonemapReader(param, proc)
	case param.Extern.Format == tree.PARQUET:
		external.reader = NewParquetReader(param, proc)
	default:
		r, err := NewCsvReader(param, proc)
		if err != nil {
			return err
		}
		external.reader = r
	}

	// Projection init
	if external.ProjectList != nil {
		err := external.PrepareProjection(proc)
		if err != nil {
			return err
		}
	}
	if external.ctr.buf == nil {
		attrs := make([]string, len(param.Attrs))
		for i, attr := range param.Attrs {
			attrs[i] = attr.ColName
		}
		external.ctr.buf = batch.New(attrs)
		flag := param.ParallelLoad
		if param.Extern.Format == tree.PARQUET {
			flag = false
		}
		//alloc space for vector
		for i := range param.Attrs {
			typ := makeType(&param.Cols[i].Typ, flag)
			external.ctr.buf.Vecs[i] = vector.NewVec(typ)
		}
	}
	return nil
}

func (external *External) Call(proc *process.Process) (vm.CallResult, error) {
	t := time.Now()
	ctx, span := trace.Start(proc.Ctx, "ExternalCall")
	t1 := time.Now()

	analyzer := external.OpAnalyzer
	defer func() {
		analyzer.AddScanTime(t1)
		span.End()
		v2.TxnStatementExternalScanDurationHistogram.Observe(time.Since(t).Seconds())
	}()

	result := vm.NewCallResult()
	param := external.Es
	if param.Fileparam.End {
		result.Status = vm.ExecStop
		return result, nil
	}

	// "Open file loop": skip empty files, find next non-empty file
	for !external.fileOpened {
		// INLINE mode has no FileList, skip file pointer advancement
		if param.Extern.ScanType != tree.INLINE {
			if param.Fileparam.FileIndex >= len(param.FileList) {
				param.Fileparam.End = true
				break
			}
			param.Fileparam.Filepath = param.FileList[param.Fileparam.FileIndex]
			param.Fileparam.FileIndex++
		}

		fileEmpty, err := external.reader.Open(param, proc)
		if err != nil {
			external.reader.Close()
			external.fileOpened = false
			param.Fileparam.End = true
			return result, err
		}
		if fileEmpty {
			external.reader.Close()
			external.finishCurrentFile(param)
			if param.Fileparam.End {
				break
			}
			continue
		}
		external.fileOpened = true
	}

	// All files processed (including all-empty case)
	if param.Fileparam.End && !external.fileOpened {
		result.Status = vm.ExecStop
		return result, nil
	}

	if external.ctr.buf != nil {
		external.ctr.buf.CleanOnlyData()
	}

	fileFinished, err := external.reader.ReadBatch(ctx, external.ctr.buf, proc, analyzer)
	if err != nil {
		external.reader.Close()
		external.fileOpened = false
		param.Fileparam.End = true
		return result, err
	}

	if fileFinished {
		external.reader.Close()
		external.finishCurrentFile(param)
	}

	analyzer.Input(external.ctr.buf)

	result.Batch = external.ctr.buf
	if external.ctr.buf != nil {
		external.ctr.maxAllocSize = max(external.ctr.maxAllocSize, external.ctr.buf.Size())
		result.Batch.ShuffleIDX = int32(param.Idx)
	}

	return result, nil
}

// finishCurrentFile handles unified file completion.
func (external *External) finishCurrentFile(param *ExternalParam) {
	external.fileOpened = false
	param.Fileparam.FileFin++
	if param.Fileparam.FileFin >= param.Fileparam.FileCnt {
		param.Fileparam.End = true
	}
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

func makeFilepathBatch(node *plan.Node, proc *process.Process, fileList []string) (bat *batch.Batch, err error) {
	num := len(node.TableDef.Cols)
	bat = &batch.Batch{
		Attrs: make([]string, num),
		Vecs:  make([]*vector.Vector, num),
	}

	mp := proc.GetMPool()
	for i := 0; i < num; i++ {
		bat.Attrs[i] = node.TableDef.Cols[i].Name
		if bat.Attrs[i] == STATEMENT_ACCOUNT {
			typ := types.New(types.T(node.TableDef.Cols[i].Typ.Id), node.TableDef.Cols[i].Typ.Width, node.TableDef.Cols[i].Typ.Scale)
			bat.Vecs[i], err = proc.AllocVectorOfRows(typ, len(fileList), nil)
			if err != nil {
				bat.Clean(mp)
				return nil, err
			}

			for j := 0; j < len(fileList); j++ {
				if err = vector.SetStringAt(bat.Vecs[i], j, getAccountCol(fileList[j]), mp); err != nil {
					bat.Clean(mp)
					return nil, err
				}
			}
		} else if bat.Attrs[i] == catalog.ExternalFilePath {
			typ := types.T_varchar.ToType()
			bat.Vecs[i], err = proc.AllocVectorOfRows(typ, len(fileList), nil)
			if err != nil {
				bat.Clean(mp)
				return nil, err
			}

			for j := 0; j < len(fileList); j++ {
				if err = vector.SetStringAt(bat.Vecs[i], j, fileList[j], mp); err != nil {
					bat.Clean(mp)
					return nil, err
				}
			}
		}
	}
	bat.SetRowCount(len(fileList))
	return bat, nil
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
	bat, err := makeFilepathBatch(node, proc, fileList)
	if err != nil {
		return nil, nil, err
	}
	filter := colexec.RewriteFilterExprList(filterList)

	executor, err := colexec.NewExpressionExecutor(proc, filter)
	if err != nil {
		return nil, nil, err
	}
	vec, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		executor.Free()
		return nil, nil, err
	}

	fileListTmp := make([]string, 0)
	fileSizeTmp := make([]int64, 0)
	bs := vector.MustFixedColWithTypeCheck[bool](vec)
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
	var fileOffsets []int64
	var fileSizeMax int64 = math.MaxInt64
	if param.Extern.ScanType == tree.INLINE || param.Extern.Local {
		return crt.GetIOReadCloser(proc, param.Extern, param.Extern.Data, fileOffsets, fileSizeMax)
	}

	// adjust read offset for parallel load.
	if 2*param.Idx >= len(param.FileOffsetTotal[param.Fileparam.FileIndex-1].Offset) {
		return nil, nil
	}
	param.FileOffset = param.FileOffsetTotal[param.Fileparam.FileIndex-1].Offset[2*param.Idx : 2*param.Idx+2]
	fileOffsets = param.FileOffset
	fileSizeMax = param.FileSize[param.Fileparam.FileIndex-1]

	return crt.GetIOReadCloser(proc, param.Extern, param.Fileparam.Filepath, fileOffsets, fileSizeMax)
}

func ReadFileOffset(param *tree.ExternParam, mcpu int, fileSize int64, visibleCols []*plan.ColDef) ([]int64, error) {
	if crt.GetCompressType(param.CompressType, param.Filepath) != tree.NOCOMPRESS {
		ctx := param.Ctx
		if ctx == nil {
			ctx = context.Background()
		}
		return nil, moerr.NewInvalidInputf(ctx, "parallel read is not supported for compressed file %s", param.Filepath)
	}
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

	var offsets []int64

	offsets = append(offsets, param.FileStartOff)
	batchSize := (fileSize - param.FileStartOff) / int64(mcpu)

	for i := 1; i < mcpu; i++ {
		vec.Entries[0].Offset = offsets[i-1] + batchSize
		if vec.Entries[0].Offset >= fileSize {
			break
		}
		if err = fs.Read(param.Ctx, &vec); err != nil {
			return nil, err
		}
		tailSize, err := getTailSize(param, visibleCols, r)
		if err != nil {
			break
		}
		offsets = append(offsets, vec.Entries[0].Offset+tailSize)
	}

	for i := 0; i < len(offsets); i++ {
		if i+1 < len(offsets) {
			arr = append(arr, offsets[i])
			arr = append(arr, offsets[i+1])
		} else {
			arr = append(arr, offsets[i])
			arr = append(arr, -1)
		}
	}
	return arr, nil
}

func getTailSize(param *tree.ExternParam, cols []*plan.ColDef, r io.ReadCloser) (int64, error) {
	if param.Strict {
		return getTailSizeStrict(param, cols, r)
	} else {
		return getTailSizeNoStrict(r), nil
	}
}

func getTailSizeNoStrict(r io.ReadCloser) int64 {
	r2 := bufio.NewReader(r)
	line, _ := r2.ReadString('\n')
	return int64(len(line))
}

func getTailSizeStrict(param *tree.ExternParam, cols []*plan.ColDef, r io.ReadCloser) (int64, error) {
	bufR := bufio.NewReader(r)
	// ensure the first character is not field quote symbol
	quoteByte := byte('"')
	if param.Tail.Fields != nil {
		if enclosed := param.Tail.Fields.EnclosedBy; enclosed != nil && enclosed.Value != 0 {
			quoteByte = enclosed.Value
		}
	}
	skipCount := int64(0)
	for {
		ch, err := bufR.ReadByte()
		if err != nil {
			return 0, err
		}
		if ch != quoteByte {
			err = bufR.UnreadByte()
			if err != nil {
				return 0, err
			}
			break
		}
		skipCount++
	}
	csvReader, err := newCSVParserFromReader(param, bufR)
	if err != nil {
		return 0, err
	}
	var fields []csvparser.Field
	for {
		fields, err = csvReader.Read(fields)
		if err != nil {
			return 0, err
		}
		if len(fields) < len(cols) {
			continue
		}
		if isLegalLine(param, cols, fields) {
			return csvReader.Pos() + skipCount, nil
		}
	}
}

func isLegalLine(param *tree.ExternParam, cols []*plan.ColDef, fields []csvparser.Field) bool {
	for idx, col := range cols {
		field := fields[idx]
		id := types.T(col.Typ.Id)
		if id != types.T_char && id != types.T_varchar && id != types.T_json &&
			id != types.T_binary && id != types.T_varbinary && id != types.T_blob && id != types.T_text && id != types.T_datalink {
			field.Val = strings.TrimSpace(field.Val)
		}
		isNullOrEmpty := field.IsNull || (getNullFlag(param.NullMap, col.Name, field.Val))
		if id != types.T_char && id != types.T_varchar &&
			id != types.T_binary && id != types.T_varbinary && id != types.T_json && id != types.T_blob && id != types.T_text && id != types.T_datalink {
			isNullOrEmpty = isNullOrEmpty || len(field.Val) == 0
		}
		if isNullOrEmpty {
			continue
		}
		switch id {
		case types.T_bool:
			_, err := types.ParseBool(field.Val)
			if err != nil {
				return false
			}

		case types.T_bit:
			if len(field.Val) > 8 {
				return false
			}
			width := col.Typ.Width
			var val uint64
			for i := 0; i < len(field.Val); i++ {
				val = (val << 8) | uint64(field.Val[i])
			}
			if val > uint64(1<<width-1) {
				return false
			}
		case types.T_int8:
			_, err := strconv.ParseInt(field.Val, 10, 8)
			if err != nil {
				if errors.Is(err, strconv.ErrRange) {
					return false
				}
				f, err := strconv.ParseFloat(field.Val, 64)
				if err != nil || f < math.MinInt8 || f > math.MaxInt8 {
					return false
				}
			}
		case types.T_int16:
			_, err := strconv.ParseInt(field.Val, 10, 16)
			if err != nil {
				if errors.Is(err, strconv.ErrRange) {
					return false
				}
				f, err := strconv.ParseFloat(field.Val, 64)
				if err != nil || f < math.MinInt16 || f > math.MaxInt16 {
					return false
				}
			}
		case types.T_int32:
			_, err := strconv.ParseInt(field.Val, 10, 32)
			if err != nil {
				if errors.Is(err, strconv.ErrRange) {
					return false
				}
				f, err := strconv.ParseFloat(field.Val, 64)
				if err != nil || f < math.MinInt32 || f > math.MaxInt32 {
					return false
				}
			}
		case types.T_int64:
			_, err := strconv.ParseInt(field.Val, 10, 64)
			if err != nil {
				if errors.Is(err, strconv.ErrRange) {
					return false
				}
				f, err := strconv.ParseFloat(field.Val, 64)
				if err != nil || f < math.MinInt64 || f > math.MaxInt64 {
					return false
				}
			}
		case types.T_uint8:
			_, err := strconv.ParseUint(field.Val, 10, 8)
			if err != nil {
				if errors.Is(err, strconv.ErrRange) {
					return false
				}
				f, err := strconv.ParseFloat(field.Val, 64)
				if err != nil || f < 0 || f > math.MaxUint8 {
					return false
				}
			}
		case types.T_uint16:
			_, err := strconv.ParseUint(field.Val, 10, 16)
			if err != nil {
				if errors.Is(err, strconv.ErrRange) {
					return false
				}
				f, err := strconv.ParseFloat(field.Val, 64)
				if err != nil || f < 0 || f > math.MaxUint16 {
					return false
				}
			}
		case types.T_uint32:
			_, err := strconv.ParseUint(field.Val, 10, 32)
			if err != nil {
				if errors.Is(err, strconv.ErrRange) {
					return false
				}
				f, err := strconv.ParseFloat(field.Val, 64)
				if err != nil || f < 0 || f > math.MaxUint32 {
					return false
				}
			}
		case types.T_uint64:
			_, err := strconv.ParseUint(field.Val, 10, 64)
			if err != nil {
				if errors.Is(err, strconv.ErrRange) {
					return false
				}
				f, err := strconv.ParseFloat(field.Val, 64)
				if err != nil || f < 0 || f > math.MaxUint64 {
					return false
				}
			}
		case types.T_float32:
			// origin float32 data type
			if col.Typ.Scale < 0 || col.Typ.Width == 0 {
				_, err := strconv.ParseFloat(field.Val, 32)
				if err != nil {
					return false
				}
			} else {
				_, err := types.ParseDecimal128(field.Val, col.Typ.Width, col.Typ.Scale)
				if err != nil {
					return false
				}
			}
		case types.T_float64:
			// origin float64 data type
			if col.Typ.Scale < 0 || col.Typ.Width == 0 {
				_, err := strconv.ParseFloat(field.Val, 64)
				if err != nil {
					return false
				}
			} else {
				_, err := types.ParseDecimal128(field.Val, col.Typ.Width, col.Typ.Scale)
				if err != nil {
					return false
				}

			}
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
			continue
		case types.T_array_float32:
			_, err := types.StringToArrayToBytes[float32](field.Val)
			if err != nil {
				return false
			}
		case types.T_array_float64:
			_, err := types.StringToArrayToBytes[float64](field.Val)
			if err != nil {
				return false
			}
		case types.T_json:
			if param.Format == tree.CSV {
				field.Val = fmt.Sprintf("%v", strings.Trim(field.Val, "\""))
				byteJson, err := types.ParseStringToByteJson(field.Val)
				if err != nil {
					return false
				}
				_, err = types.EncodeJson(byteJson)
				if err != nil {
					return false
				}
			}
		case types.T_date:
			_, err := types.ParseDateCast(field.Val)
			if err != nil {
				return false
			}
		case types.T_time:
			_, err := types.ParseTime(field.Val, col.Typ.Scale)
			if err != nil {
				return false
			}
		case types.T_datetime:
			_, err := types.ParseDatetime(field.Val, col.Typ.Scale)
			if err != nil {
				return false
			}
		case types.T_enum:
			_, err := strconv.ParseUint(field.Val, 10, 16)
			if err == nil {
				continue
			} else if errors.Is(err, strconv.ErrSyntax) {
				_, err := types.ParseEnum(col.Typ.Enumvalues, field.Val)
				if err != nil {
					return false
				}
			} else {
				if errors.Is(err, strconv.ErrRange) {
					return false
				}
				f, err := strconv.ParseFloat(field.Val, 64)
				if err != nil || f < 0 || f > math.MaxUint16 {
					return false
				}
			}
		case types.T_decimal64:
			_, err := types.ParseDecimal64(field.Val, col.Typ.Width, col.Typ.Scale)
			if err != nil {
				// we tolerate loss of digits.
				if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
					return false
				}
			}
		case types.T_decimal128:
			_, err := types.ParseDecimal128(field.Val, col.Typ.Width, col.Typ.Scale)
			if err != nil {
				// we tolerate loss of digits.
				if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
					return false
				}
			}
		case types.T_timestamp:
			// Note: isLegalLine is only used for file offset calculation in parallel LOAD DATA,
			// not for actual data loading. It uses time.Local as fallback since proc is not available.
			// The actual data loading uses getColData which correctly uses session timezone.
			t := time.Local
			_, err := types.ParseTimestamp(t, field.Val, col.Typ.Scale)
			if err != nil {
				return false
			}
		case types.T_uuid:
			_, err := types.ParseUuid(field.Val)
			if err != nil {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func makeType(typ *plan.Type, flag bool) types.Type {
	if flag {
		return types.New(types.T_varchar, 0, 0)
	}
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
}

func getRealAttrCnt(attrs []plan.ExternAttr) int {
	cnt := 0
	for i := 0; i < len(attrs); i++ {
		if catalog.ContainExternalHidenCol(attrs[i].ColName) {
			cnt++
		}
	}
	return len(attrs) - cnt
}

func checkLineValidRestrictive(param *ExternalParam, proc *process.Process, line []csvparser.Field, rowIdx int) error {
	if param.ClusterTable != nil && param.ClusterTable.GetIsClusterTable() {
		//the column account_id of the cluster table do need to be filled here
		if len(line)+1 != getRealAttrCnt(param.Attrs) {
			return moerr.NewInvalidInputf(proc.Ctx, "the data of row %d contained is not equal to input columns", rowIdx+1)
		}
	} else {
		if param.Extern.ExternType == int32(plan.ExternType_EXTERNAL_TB) {
			if len(line) < getRealAttrCnt(param.Attrs) {
				return moerr.NewInvalidInputf(proc.Ctx, "the data of row %d contained is less than input columns", rowIdx+1)
			}
			return nil
		}
		if len(line) != int(param.ColumnListLen) {
			if len(line) != int(param.ColumnListLen)+1 {
				return moerr.NewInvalidInputf(proc.Ctx, "the data of row %d contained is not equal to input columns", rowIdx+1)
			}
			field := line[len(line)-1]
			if field.Val != "" {
				return moerr.NewInvalidInputf(proc.Ctx, "the data of row %d contained is not equal to input columns", rowIdx+1)
			}
		}
	}
	return nil
}

func checkLineStrict(param *ExternalParam) bool {
	if param.Extern.Local || !param.StrictSqlMode {
		return false
	}
	return true
}

const JsonNull = "\\N"

func getNullFlag(nullMap map[string][]string, attr, field string) bool {
	if nullMap == nil || len(nullMap[attr]) == 0 {
		return false
	}
	for _, v := range nullMap[attr] {
		if v == field {
			return true
		}
	}
	return false
}

func getFieldFromLine(line []csvparser.Field, colName string, param *ExternalParam, fieldIdx int32) csvparser.Field {
	if catalog.ContainExternalHidenCol(colName) {
		return csvparser.Field{Val: param.Fileparam.Filepath}
	}
	return line[fieldIdx]
}

func getOneRowData(proc *process.Process, bat *batch.Batch, line []csvparser.Field, rowIdx int, param *ExternalParam) error {
	mp := proc.GetMPool()
	if checkLineStrict(param) {
		if err := checkLineValidRestrictive(param, proc, line, rowIdx); err != nil {
			return err
		}
		for _, attr := range param.Attrs {
			if err := getColData(bat, line, rowIdx, param, mp, attr, proc); err != nil {
				return err
			}
		}
		return nil
	}

	if int32(len(line)) >= param.ColumnListLen {
		for _, attr := range param.Attrs {
			if err := getColData(bat, line, rowIdx, param, mp, attr, proc); err != nil {
				return err
			}
		}
		return nil
	}

	for _, attr := range param.Attrs {
		if attr.ColFieldIndex < int32(len(line)) {
			if err := getColData(bat, line, rowIdx, param, mp, attr, proc); err != nil {
				return err
			}
			continue
		}
		vec := bat.Vecs[attr.ColIndex]
		vector.AppendBytes(vec, nil, true, mp)
	}
	return nil
}

func getColData(bat *batch.Batch, line []csvparser.Field, rowIdx int, param *ExternalParam, mp *mpool.MPool, attr plan.ExternAttr, proc *process.Process) error {
	colIdx := attr.ColIndex
	colName := attr.ColName
	vec := bat.Vecs[colIdx]
	col := param.Cols[colIdx]

	fieldIdx := attr.ColFieldIndex

	field := getFieldFromLine(line, colName, param, fieldIdx)
	id := types.T(col.Typ.Id)
	trimSpace := false
	if id != types.T_char && id != types.T_varchar && id != types.T_json &&
		id != types.T_binary && id != types.T_varbinary && id != types.T_blob && id != types.T_text && id != types.T_datalink {
		field.Val = strings.TrimSpace(field.Val)
		trimSpace = true
	}
	isNullOrEmpty := field.IsNull || (getNullFlag(param.Extern.NullMap, colName, field.Val))
	if trimSpace {
		isNullOrEmpty = isNullOrEmpty || len(field.Val) == 0
	}
	if isNullOrEmpty {
		vector.AppendBytes(vec, nil, true, mp)
		return nil
	}

	if param.ParallelLoad {
		err := vector.AppendBytes(vec, []byte(field.Val), false, mp)
		if err != nil {
			return err
		}
		return nil
	}

	switch id {
	case types.T_bool:
		b, err := types.ParseBool(field.Val)
		if err != nil {
			return err
		}
		if err = vector.AppendFixed(vec, b, false, mp); err != nil {
			return err
		}
	case types.T_bit:
		if len(field.Val) > 8 {
			return moerr.NewInternalErrorf(param.Ctx, "data too long, len(val) = %v", len(field.Val))
		}

		width := col.Typ.Width
		var val uint64
		for i := 0; i < len(field.Val); i++ {
			val = (val << 8) | uint64(field.Val[i])
		}
		if val > uint64(1<<width-1) {
			return moerr.NewInternalErrorf(param.Ctx, "data too long, type width = %d, val = %b", width, val)
		}

		if err := vector.AppendFixed(vec, val, false, mp); err != nil {
			return err
		}
	case types.T_int8:
		d, err := strconv.ParseInt(field.Val, 10, 8)
		if err == nil {
			if err := vector.AppendFixed(vec, int8(d), false, mp); err != nil {
				return err
			}
		} else {
			// if field.HasStringQuote is true, like load "1.9" into int type, return error, if load 1.9, can load successful
			if errors.Is(err, strconv.ErrRange) || field.HasStringQuote {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not int8 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < math.MinInt8 || f > math.MaxInt8 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not int8 type for column %d", field.Val, colIdx)
			}
			if err := vector.AppendFixed(vec, int8(f), false, mp); err != nil {
				return err
			}
		}
	case types.T_int16:
		d, err := strconv.ParseInt(field.Val, 10, 16)
		if err == nil {
			if err := vector.AppendFixed(vec, int16(d), false, mp); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) || field.HasStringQuote {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not int16 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < math.MinInt16 || f > math.MaxInt16 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not int16 type for column %d", field.Val, colIdx)
			}
			if err := vector.AppendFixed(vec, int16(f), false, mp); err != nil {
				return err
			}
		}
	case types.T_int32:
		d, err := strconv.ParseInt(field.Val, 10, 32)
		if err == nil {
			if err := vector.AppendFixed(vec, int32(d), false, mp); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) || field.HasStringQuote {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not int32 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < math.MinInt32 || f > math.MaxInt32 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not int32 type for column %d", field.Val, colIdx)
			}
			if err := vector.AppendFixed(vec, int32(f), false, mp); err != nil {
				return err
			}
		}
	case types.T_int64:
		d, err := strconv.ParseInt(field.Val, 10, 64)
		if err == nil {
			if err := vector.AppendFixed(vec, d, false, mp); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) || field.HasStringQuote {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not int64 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < math.MinInt64 || f > math.MaxInt64 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not int64 type for column %d", field.Val, colIdx)
			}
			if err := vector.AppendFixed(vec, int64(f), false, mp); err != nil {
				return err
			}
		}
	case types.T_uint8:
		d, err := strconv.ParseUint(field.Val, 10, 8)
		if err == nil {
			if err := vector.AppendFixed(vec, uint8(d), false, mp); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) || field.HasStringQuote {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not uint8 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < 0 || f > math.MaxUint8 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not uint8 type for column %d", field.Val, colIdx)
			}
			if err := vector.AppendFixed(vec, uint8(f), false, mp); err != nil {
				return err
			}
		}
	case types.T_uint16:
		d, err := strconv.ParseUint(field.Val, 10, 16)
		if err == nil {
			if err := vector.AppendFixed(vec, uint16(d), false, mp); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) || field.HasStringQuote {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not uint16 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < 0 || f > math.MaxUint16 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not uint16 type for column %d", field.Val, colIdx)
			}
			if err := vector.AppendFixed(vec, uint16(f), false, mp); err != nil {
				return err
			}
		}
	case types.T_uint32:
		d, err := strconv.ParseUint(field.Val, 10, 32)
		if err == nil {
			if err := vector.AppendFixed(vec, uint32(d), false, mp); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) || field.HasStringQuote {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not uint32 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < 0 || f > math.MaxUint32 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not uint32 type for column %d", field.Val, colIdx)
			}
			if err := vector.AppendFixed(vec, uint32(f), false, mp); err != nil {
				return err
			}
		}
	case types.T_uint64:
		d, err := strconv.ParseUint(field.Val, 10, 64)
		if err == nil {
			if err := vector.AppendFixed(vec, d, false, mp); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) || field.HasStringQuote {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not uint64 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < 0 || f > math.MaxUint64 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not uint64 type for column %d", field.Val, colIdx)
			}
			if err := vector.AppendFixed(vec, uint64(f), false, mp); err != nil {
				return err
			}
		}
	case types.T_float32:
		// origin float32 data type
		if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
			d, err := strconv.ParseFloat(field.Val, 32)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not float32 type for column %d", field.Val, colIdx)
			}
			if err := vector.AppendFixed(vec, float32(d), false, mp); err != nil {
				return err
			}
		} else {
			d, err := types.ParseDecimal128(field.Val, vec.GetType().Width, vec.GetType().Scale)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not float32 type for column %d", field.Val, colIdx)
			}
			if err := vector.AppendFixed(vec, float32(types.Decimal128ToFloat64(d, vec.GetType().Scale)), false, mp); err != nil {
				return err
			}
		}
	case types.T_float64:
		// origin float64 data type
		if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
			d, err := strconv.ParseFloat(field.Val, 64)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not float64 type for column %d", field.Val, colIdx)
			}
			if err := vector.AppendFixed(vec, d, false, mp); err != nil {
				return err
			}
		} else {
			d, err := types.ParseDecimal128(field.Val, vec.GetType().Width, vec.GetType().Scale)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not float64 type for column %d", field.Val, colIdx)
			}
			if err := vector.AppendFixed(vec, types.Decimal128ToFloat64(d, vec.GetType().Scale), false, mp); err != nil {
				return err
			}
		}
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		err := vector.AppendBytes(vec, []byte(field.Val), false, mp)
		if err != nil {
			return err
		}
	case types.T_array_float32:
		arrBytes, err := types.StringToArrayToBytes[float32](field.Val)
		if err != nil {
			return err
		}
		if err = vector.AppendBytes(vec, arrBytes, false, mp); err != nil {
			return err
		}
	case types.T_array_float64:
		arrBytes, err := types.StringToArrayToBytes[float64](field.Val)
		if err != nil {
			return err
		}
		if err = vector.AppendBytes(vec, arrBytes, false, mp); err != nil {
			return err
		}
	case types.T_json:
		var jsonBytes []byte
		if param.Extern.Format != tree.CSV {
			jsonBytes = []byte(field.Val)
		} else {
			field.Val = fmt.Sprintf("%v", strings.Trim(field.Val, "\""))
			byteJson, err := types.ParseStringToByteJson(field.Val)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not json type for column %d", field.Val, colIdx)
			}
			jsonBytes, err = types.EncodeJson(byteJson)
			if err != nil {
				logutil.Errorf("encode json[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not json type for column %d", field.Val, colIdx)
			}
		}

		if err := vector.AppendBytes(vec, jsonBytes, false, mp); err != nil {
			return err
		}
	case types.T_date:
		d, err := types.ParseDateCast(field.Val)
		if err != nil {
			logutil.Errorf("parse field[%v] err:%v", field.Val, err)
			return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not Date type for column %d", field.Val, colIdx)
		}

		if err := vector.AppendFixed(vec, d, false, mp); err != nil {
			return err
		}
	case types.T_time:
		d, err := types.ParseTime(field.Val, vec.GetType().Scale)
		if err != nil {
			logutil.Errorf("parse field[%v] err:%v", field.Val, err)
			return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not Time type for column %d", field.Val, colIdx)
		}

		if err := vector.AppendFixed(vec, d, false, mp); err != nil {
			return err
		}
	case types.T_datetime:
		d, err := types.ParseDatetime(field.Val, vec.GetType().Scale)
		if err != nil {
			logutil.Errorf("parse field[%v] err:%v", field.Val, err)
			return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not Datetime type for column %d", field.Val, colIdx)
		}
		if err := vector.AppendFixed(vec, d, false, mp); err != nil {
			return err
		}
	case types.T_enum:
		d, err := strconv.ParseUint(field.Val, 10, 16)
		if err == nil {
			if err := vector.AppendFixed(vec, types.Enum(d), false, mp); err != nil {
				return err
			}
		} else if errors.Is(err, strconv.ErrSyntax) {
			v, err := types.ParseEnum(col.Typ.Enumvalues, field.Val)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return err
			}
			if err := vector.AppendFixed(vec, types.Enum(v), false, mp); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not uint16 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < 0 || f > math.MaxUint16 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not uint16 type for column %d", field.Val, colIdx)
			}
			if err := vector.AppendFixed(vec, types.Enum(f), false, mp); err != nil {
				return err
			}
		}
	case types.T_decimal64:
		d, err := types.ParseDecimal64(field.Val, vec.GetType().Width, vec.GetType().Scale)
		if err != nil {
			// we tolerate loss of digits.
			if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is invalid Decimal64 type for column %d", field.Val, colIdx)
			}
		}
		if err := vector.AppendFixed(vec, d, false, mp); err != nil {
			return err
		}
	case types.T_decimal128:
		d, err := types.ParseDecimal128(field.Val, vec.GetType().Width, vec.GetType().Scale)
		if err != nil {
			// we tolerate loss of digits.
			if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is invalid Decimal128 type for column %d", field.Val, colIdx)
			}
		}
		if err := vector.AppendFixed(vec, d, false, mp); err != nil {
			return err
		}
	case types.T_timestamp:
		t := time.Local
		if proc != nil {
			t = proc.GetSessionInfo().TimeZone
			if t == nil {
				t = time.Local
			}
		}
		d, err := types.ParseTimestamp(t, field.Val, vec.GetType().Scale)
		if err != nil {
			logutil.Errorf("parse field[%v] err:%v", field.Val, err)
			return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not Timestamp type for column %d", field.Val, colIdx)
		}
		if err := vector.AppendFixed(vec, d, false, mp); err != nil {
			return err
		}
	case types.T_uuid:
		d, err := types.ParseUuid(field.Val)
		if err != nil {
			logutil.Errorf("parse field[%v] err:%v", field.Val, err)
			return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not uuid type for column %d", field.Val, colIdx)
		}
		if err := vector.AppendFixed(vec, d, false, mp); err != nil {
			return err
		}
	default:
		return moerr.NewInternalErrorf(param.Ctx, "the value type %d is not support now", param.Cols[rowIdx].Typ.Id)
	}
	return nil
}

func loadFormatIsValid(param *tree.ExternParam) bool {
	switch param.Format {
	case tree.JSONLINE, tree.CSV, tree.PARQUET:
		return true
	}
	return false
}

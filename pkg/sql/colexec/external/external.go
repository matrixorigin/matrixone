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
	"archive/tar"
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
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
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm"
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
	external.ctr = new(container)
	defer span.End()
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
		return moerr.NewNYI(proc.Ctx, "load format '%s'", param.Extern.Format)
	}

	if param.Extern.Format != tree.PARQUET {
		if param.Extern.Format == tree.JSONLINE {
			if param.Extern.JsonData != tree.OBJECT && param.Extern.JsonData != tree.ARRAY {
				param.Fileparam.End = true
				return moerr.NewNotSupported(proc.Ctx, "the jsonline format '%s' is not supported now", param.Extern.JsonData)
			}
		}
		param.IgnoreLineTag = int(param.Extern.Tail.IgnoredLines)
		param.IgnoreLine = param.IgnoreLineTag
		param.MoCsvLineArray = make([][]csvparser.Field, OneBatchMaxRow)
	}

	if len(param.FileList) == 0 && param.Extern.ScanType != tree.INLINE {
		logutil.Warnf("no such file '%s'", param.Extern.Filepath)
		param.Fileparam.End = true
	}
	param.Fileparam.FileCnt = len(param.FileList)
	param.Ctx = proc.Ctx
	param.Zoneparam = &ZonemapFileparam{}
	name2ColIndex := make(map[string]int32, len(param.Cols))
	for i, col := range param.Cols {
		name2ColIndex[col.Name] = int32(i)
	}
	param.tableDef = &plan.TableDef{
		Name2ColIndex: name2ColIndex,
	}
	param.Filter.columnMap, _, _, _ = plan2.GetColumnsByExpr(param.Filter.FilterExpr, param.tableDef)
	param.Filter.zonemappable = plan2.ExprIsZonemappable(proc.Ctx, param.Filter.FilterExpr)
	return nil
}

func (external *External) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	t := time.Now()
	ctx, span := trace.Start(proc.Ctx, "ExternalCall")
	t1 := time.Now()
	anal := proc.GetAnalyze(external.GetIdx(), external.GetParallelIdx(), external.GetParallelMajor())
	anal.Start()
	defer func() {
		anal.Stop()
		anal.AddScanTime(t1)
		span.End()
		v2.TxnStatementExternalScanDurationHistogram.Observe(time.Since(t).Seconds())
	}()
	anal.Input(nil, external.GetIsFirst())

	var err error
	result := vm.NewCallResult()
	param := external.Es
	if param.Fileparam.End {
		result.Status = vm.ExecStop
		return result, nil
	}
	if (param.plh == nil && param.parqh == nil) && param.Extern.ScanType != tree.INLINE {
		if param.Fileparam.FileIndex >= len(param.FileList) {
			result.Status = vm.ExecStop
			return result, nil
		}
		param.Fileparam.Filepath = param.FileList[param.Fileparam.FileIndex]
		param.Fileparam.FileIndex++
	}
	if external.ctr.buf != nil {
		proc.PutBatch(external.ctr.buf)
		external.ctr.buf = nil
	}
	external.ctr.buf, err = scanFileData(ctx, param, proc)
	if err != nil {
		param.Fileparam.End = true
		return result, err
	}

	if external.ctr.buf != nil {
		anal.Output(external.ctr.buf, external.GetIsLast())
		external.ctr.maxAllocSize = max(external.ctr.maxAllocSize, external.ctr.buf.Size())
	}
	result.Batch = external.ctr.buf
	if result.Batch != nil {
		result.Batch.ShuffleIDX = int32(param.Idx)
	}
	return result, nil
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
		Cnt:   1,
	}

	var buf bytes.Buffer
	mp := proc.GetMPool()
	for i := 0; i < num; i++ {
		bat.Attrs[i] = node.TableDef.Cols[i].GetOriginCaseName()
		if bat.Attrs[i] == STATEMENT_ACCOUNT {
			typ := types.New(types.T(node.TableDef.Cols[i].Typ.Id), node.TableDef.Cols[i].Typ.Width, node.TableDef.Cols[i].Typ.Scale)
			bat.Vecs[i], err = proc.AllocVectorOfRows(typ, len(fileList), nil)
			if err != nil {
				bat.Clean(mp)
				return nil, err
			}

			for j := 0; j < len(fileList); j++ {
				buf.WriteString(getAccountCol(fileList[j]))
				bs := buf.Bytes()
				if err = vector.SetBytesAt(bat.Vecs[i], j, bs, mp); err != nil {
					bat.Clean(mp)
					return nil, err
				}
				buf.Reset()
			}
		} else if bat.Attrs[i] == catalog.ExternalFilePath {
			typ := types.T_varchar.ToType()
			bat.Vecs[i], err = proc.AllocVectorOfRows(typ, len(fileList), nil)
			if err != nil {
				bat.Clean(mp)
				return nil, err
			}

			for j := 0; j < len(fileList); j++ {
				buf.WriteString(fileList[j])
				bs := buf.Bytes()
				if err = vector.SetBytesAt(bat.Vecs[i], j, bs, mp); err != nil {
					bat.Clean(mp)
					return nil, err
				}
				buf.Reset()
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
	if param.Extern.ScanType == tree.INLINE {
		return io.NopCloser(bytes.NewReader(util.UnsafeStringToBytes(param.Extern.Data))), nil
	}
	if param.Extern.Local {
		return io.NopCloser(proc.GetLoadLocalReader()), nil
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

// TODO : merge below two functions
func ReadFileOffsetNoStrict(param *tree.ExternParam, mcpu int, fileSize int64) ([]int64, error) {
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

func ReadFileOffsetStrict(param *tree.ExternParam, mcpu int, fileSize int64, visibleCols []*plan.ColDef) ([]int64, error) {
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

	var offset []int64
	batchSize := fileSize / int64(mcpu)

	offset = append(offset, 0)

	for i := 1; i < mcpu; i++ {
		vec.Entries[0].Offset = offset[i-1] + batchSize
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
		offset = append(offset, vec.Entries[0].Offset+tailSize)
	}

	for i := 0; i < len(offset); i++ {
		if i+1 < len(offset) {
			arr = append(arr, offset[i])
			arr = append(arr, offset[i+1])
		} else {
			arr = append(arr, offset[i])
			arr = append(arr, -1)
		}
	}
	return arr, nil
}

func getTailSize(param *tree.ExternParam, cols []*plan.ColDef, r io.ReadCloser) (int64, error) {
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
	csvReader, err := newReaderWithParam(&ExternalParam{
		ExParamConst: ExParamConst{Extern: param},
		ExParam:      ExParam{reader: io.NopCloser(bufR)},
	}, true)
	if err != nil {
		return 0, err
	}
	var fields []csvparser.Field
	for {
		fields, err = csvReader.Read()
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

func GetCompressType(param *tree.ExternParam, filepath string) string {
	if param.CompressType != "" && param.CompressType != tree.AUTO {
		return param.CompressType
	}

	filepath = strings.ToLower(filepath)

	switch {
	case strings.HasSuffix(filepath, ".tar.gz") || strings.HasSuffix(filepath, ".tar.gzip"):
		return tree.TAR_GZ
	case strings.HasSuffix(filepath, ".tar.bz2") || strings.HasSuffix(filepath, ".tar.bzip2"):
		return tree.TAR_BZ2
	case strings.HasSuffix(filepath, ".gz") || strings.HasSuffix(filepath, ".gzip"):
		return tree.GZIP
	case strings.HasSuffix(filepath, ".bz2") || strings.HasSuffix(filepath, ".bzip2"):
		return tree.BZIP2
	case strings.HasSuffix(filepath, ".lz4"):
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
	case tree.TAR_GZ:
		gzipReader, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		return getTarReader(param.Ctx, gzipReader)
	case tree.TAR_BZ2:
		return getTarReader(param.Ctx, bzip2.NewReader(r))
	default:
		return nil, moerr.NewInternalError(param.Ctx, "the compress type '%s' is not support now", param.CompressType)
	}
}

func getTarReader(ctx context.Context, r io.Reader) (io.ReadCloser, error) {
	tarReader := tar.NewReader(r)
	// move to first file
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return nil, moerr.NewInternalError(ctx, "failed to decompress the file, no available files found")
		}
		if err != nil {
			return nil, err
		}
		if !header.FileInfo().IsDir() && !strings.HasPrefix(header.FileInfo().Name(), ".") {
			break
		}
	}
	return io.NopCloser(tarReader), nil
}

func makeType(typ *plan.Type, flag bool) types.Type {
	if flag {
		return types.New(types.T_varchar, 0, 0)
	}
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
}

func makeBatch(param *ExternalParam, batchSize int, proc *process.Process) (bat *batch.Batch, err error) {
	bat = batch.New(false, param.Attrs)
	//alloc space for vector
	for i := range param.Attrs {
		typ := makeType(&param.Cols[i].Typ, param.ParallelLoad)
		bat.Vecs[i] = proc.GetVector(typ)
	}
	if err = bat.PreExtend(proc.GetMPool(), batchSize); err != nil {
		bat.Clean(proc.GetMPool())
		return nil, err
	}
	for i := range bat.Vecs {
		bat.Vecs[i].SetLength(batchSize)
	}
	return bat, nil
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

func checkLineValidRestrictive(param *ExternalParam, proc *process.Process, line []csvparser.Field, rowIdx int) error {
	if param.ClusterTable != nil && param.ClusterTable.GetIsClusterTable() {
		//the column account_id of the cluster table do need to be filled here
		if len(line)+1 != getRealAttrCnt(param.Attrs, param.Cols) {
			return moerr.NewInvalidInput(proc.Ctx, "the data of row %d contained is not equal to input columns", rowIdx+1)
		}
	} else {
		if param.Extern.ExtTab {
			if len(line) < getRealAttrCnt(param.Attrs, param.Cols) {
				return moerr.NewInvalidInput(proc.Ctx, "the data of row %d contained is less than input columns", rowIdx+1)
			}
			return nil
		}
		if len(line) != len(param.TbColToDataCol) {
			if len(line) != len(param.TbColToDataCol)+1 {
				return moerr.NewInvalidInput(proc.Ctx, "the data of row %d contained is not equal to input columns", rowIdx+1)
			}
			field := line[len(line)-1]
			if field.Val != "" {
				return moerr.NewInvalidInput(proc.Ctx, "the data of row %d contained is not equal to input columns", rowIdx+1)
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

// Data interpretation is nonrestrictive if the SQL mode is nonrestrictive or the IGNORE or LOCAL modifier is specified
// todo IGNORE
func checkLineValid(param *ExternalParam, proc *process.Process, line []csvparser.Field, rowIdx int) error {
	if checkLineStrict(param) {
		return checkLineValidRestrictive(param, proc, line, rowIdx)
	}

	return nil
}

func getBatchData(param *ExternalParam, plh *ParseLineHandler, proc *process.Process) (*batch.Batch, error) {
	bat, err := makeBatch(param, plh.batchSize, proc)
	if err != nil {
		return nil, err
	}

	unexpectEOF := false
	for rowIdx := 0; rowIdx < plh.batchSize; rowIdx++ {
		line := plh.moCsvLineArray[rowIdx]
		if param.Extern.Format == tree.JSONLINE {
			line, err = transJson2Lines(proc.Ctx, line[0].Val, param.Attrs, param.Cols, param.Extern.JsonData, param)
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

		if err := checkLineValid(param, proc, line, rowIdx); err != nil {
			return nil, err
		}

		err = getOneRowData(bat, line, rowIdx, param, proc.GetMPool())
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
	bat.SetRowCount(n)
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

	csvReader, err := newReaderWithParam(param, false)
	if err != nil {
		return nil, err
	}
	plh := &ParseLineHandler{
		csvReader:      csvReader,
		moCsvLineArray: param.MoCsvLineArray,
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
	cnt, finish, err = readCountStringLimitSize(plh.csvReader, proc.Ctx, param.maxBatchSize, plh.moCsvLineArray)
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
				plh.moCsvLineArray = append(plh.moCsvLineArray, make([]csvparser.Field, param.IgnoreLine))
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

func getBatchFromZonemapFile(ctx context.Context, param *ExternalParam, proc *process.Process, objectReader *blockio.BlockReader) (bat *batch.Batch, err error) {
	var tmpBat *batch.Batch
	var vecTmp *vector.Vector
	var release func()
	mp := proc.Mp()

	ctx, span := trace.Start(ctx, "getBatchFromZonemapFile")
	defer func() {
		span.End()
		if tmpBat != nil {
			for i, v := range tmpBat.Vecs {
				if v == vecTmp {
					tmpBat.Vecs[i] = nil
				}
			}
			tmpBat.Clean(mp)
		}
		if vecTmp != nil {
			vecTmp.Free(mp)
		}
		if release != nil {
			release()
		}
		if err != nil && bat != nil {
			bat.Clean(mp)
		}
	}()

	bat, err = makeBatch(param, 0, proc)
	if err != nil {
		return nil, err
	}
	if param.Zoneparam.offset >= len(param.Zoneparam.bs) {
		return bat, nil
	}

	rows := 0

	idxs := make([]uint16, len(param.Attrs))
	meta := param.Zoneparam.bs[param.Zoneparam.offset].GetMeta()
	colCnt := meta.BlockHeader().ColumnCount()
	for i := 0; i < len(param.Attrs); i++ {
		idxs[i] = uint16(param.Name2ColIndex[strings.ToLower(param.Attrs[i])])
		if idxs[i] >= colCnt {
			idxs[i] = 0
		}
	}

	tmpBat, release, err = objectReader.LoadColumns(ctx, idxs, nil, param.Zoneparam.bs[param.Zoneparam.offset].BlockHeader().BlockID().Sequence(), mp)
	if err != nil {
		return nil, err
	}
	filepathBytes := []byte(param.Fileparam.Filepath)

	var sels []int32
	for i := 0; i < len(param.Attrs); i++ {
		if uint16(param.Name2ColIndex[strings.ToLower(param.Attrs[i])]) >= colCnt {
			vecTmp, err = proc.AllocVectorOfRows(makeType(&param.Cols[i].Typ, false), rows, nil)
			if err != nil {
				return nil, err
			}
			for j := 0; j < rows; j++ {
				nulls.Add(vecTmp.GetNulls(), uint64(j))
			}
		} else if catalog.ContainExternalHidenCol(param.Attrs[i]) {
			if rows == 0 {
				rows = tmpBat.Vecs[i].Length()
			}
			vecTmp, err = proc.AllocVectorOfRows(makeType(&param.Cols[i].Typ, false), rows, nil)
			if err != nil {
				return nil, err
			}
			for j := 0; j < rows; j++ {
				if err = vector.SetBytesAt(vecTmp, j, filepathBytes, mp); err != nil {
					return nil, err
				}
			}
		} else {
			vecTmp = tmpBat.Vecs[i]
			rows = vecTmp.Length()
		}
		if cap(sels) >= vecTmp.Length() {
			sels = sels[:vecTmp.Length()]
		} else {
			sels = make([]int32, vecTmp.Length())

			for j, k := int32(0), int32(len(sels)); j < k; j++ {
				sels[j] = j
			}
		}

		if err = bat.Vecs[i].Union(vecTmp, sels, proc.GetMPool()); err != nil {
			return nil, err
		}
	}

	n := bat.Vecs[0].Length()
	bat.SetRowCount(n)
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

	if isMonoExpr := plan2.ExprIsZonemappable(proc.Ctx, expr); isMonoExpr {
		cnt := plan2.AssignAuxIdForExpr(expr, 0)
		zms = make([]objectio.ZoneMap, cnt)
		vecs = make([]*vector.Vector, cnt)
	}

	return colexec.EvaluateFilterByZoneMap(
		notReportErrCtx, proc, expr, meta, columnMap, zms, vecs)
}

func getZonemapBatch(ctx context.Context, param *ExternalParam, proc *process.Process, objectReader *blockio.BlockReader) (*batch.Batch, error) {
	var err error
	param.Zoneparam.bs, err = objectReader.LoadAllBlocks(param.Ctx, proc.GetMPool())
	if err != nil {
		return nil, err
	}
	if param.Zoneparam.offset >= len(param.Zoneparam.bs) {
		return makeBatch(param, 0, proc)
	}

	if param.Filter.zonemappable {
		for !needRead(ctx, param, proc) {
			param.Zoneparam.offset++
		}
	}
	return getBatchFromZonemapFile(ctx, param, proc, objectReader)
}

func scanZonemapFile(ctx context.Context, param *ExternalParam, proc *process.Process) (*batch.Batch, error) {
	var err error
	param.Filter.blockReader, err = blockio.NewFileReader(proc.GetService(), param.Extern.FileService, param.Fileparam.Filepath)
	if err != nil {
		return nil, err
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

// scanFileData read batch data from external file
func scanFileData(ctx context.Context, param *ExternalParam, proc *process.Process) (*batch.Batch, error) {
	if param.Extern.QueryResult {
		return scanZonemapFile(ctx, param, proc)
	}
	if param.Extern.Format == tree.PARQUET {
		return scanParquetFile(ctx, param, proc)
	}
	return scanCsvFile(ctx, param, proc)
}

func transJson2Lines(ctx context.Context, str string, attrs []string, cols []*plan.ColDef, jsonData string, param *ExternalParam) ([]csvparser.Field, error) {
	switch jsonData {
	case tree.OBJECT:
		return transJsonObject2Lines(ctx, str, attrs, cols, param)
	case tree.ARRAY:
		return transJsonArray2Lines(ctx, str, attrs, cols, param)
	default:
		return nil, moerr.NewNotSupported(ctx, "the jsonline format '%s' is not support now", jsonData)
	}
}

const JsonNull = "\\N"

func transJsonObject2Lines(ctx context.Context, str string, attrs []string, cols []*plan.ColDef, param *ExternalParam) ([]csvparser.Field, error) {
	var (
		err error
		res = make([]csvparser.Field, 0, len(attrs))
	)
	if param.prevStr != "" {
		str = param.prevStr + str
		param.prevStr = ""
	}
	jsonNode, err := bytejson.ParseNodeString(str)
	if err != nil {
		logutil.Errorf("json unmarshal err:%v", err)
		param.prevStr = str
		return nil, err
	}
	defer jsonNode.Free()
	g, ok := jsonNode.V.(*bytejson.Group)
	if !ok || !g.Obj {
		return nil, moerr.NewInvalidInput(ctx, "not a object")
	}
	if len(g.Keys) < getRealAttrCnt(attrs, cols) {
		return nil, moerr.NewInternalError(ctx, ColumnCntLargerErrorInfo)
	}
	for idx, attr := range attrs {
		if cols[idx].Hidden {
			continue
		}
		ki := slices.Index(g.Keys, attr)
		if ki < 0 {
			return nil, moerr.NewInvalidInput(ctx, "the attr %s is not in json", attr)
		}

		valN := g.Values[ki]
		if valN.V == nil {
			res = append(res, csvparser.Field{IsNull: true})
			continue
		}

		tp := cols[idx].Typ.Id
		if tp == int32(types.T_json) {
			data, err := valN.ByteJsonRaw()
			if err != nil {
				return nil, err
			}
			res = append(res, csvparser.Field{Val: string(data)})
			continue
		}

		val := fmt.Sprint(valN)
		res = append(res, csvparser.Field{Val: val, IsNull: val == JsonNull})
	}
	return res, nil
}

func transJsonArray2Lines(ctx context.Context, str string, attrs []string, cols []*plan.ColDef, param *ExternalParam) ([]csvparser.Field, error) {
	var (
		err error
		res = make([]csvparser.Field, 0, len(attrs))
	)
	if param.prevStr != "" {
		str = param.prevStr + str
		param.prevStr = ""
	}
	jsonNode, err := bytejson.ParseNodeString(str)
	if err != nil {
		param.prevStr = str
		return nil, err
	}
	defer jsonNode.Free()
	g, ok := jsonNode.V.(*bytejson.Group)
	if !ok || g.Obj {
		return nil, moerr.NewInvalidInput(ctx, "not a json array")
	}
	if len(g.Values) < getRealAttrCnt(attrs, cols) {
		return nil, moerr.NewInternalError(ctx, ColumnCntLargerErrorInfo)
	}
	for idx, valN := range g.Values {
		if idx >= len(cols) {
			return nil, moerr.NewInvalidInput(ctx, str+" , wrong number of colunms")
		}

		if valN.V == nil {
			res = append(res, csvparser.Field{IsNull: true})
			continue
		}

		tp := cols[idx].Typ.Id
		if tp == int32(types.T_json) {
			data, err := valN.ByteJsonRaw()
			if err != nil {
				return nil, err
			}
			res = append(res, csvparser.Field{Val: string(data)})
			continue
		}

		val := fmt.Sprint(valN)
		res = append(res, csvparser.Field{Val: val, IsNull: val == JsonNull})
	}
	return res, nil
}

func getNullFlag(nullMap map[string][]string, attr, field string) bool {
	if nullMap == nil || len(nullMap[attr]) == 0 {
		return false
	}
	field = strings.ToLower(field)
	for _, v := range nullMap[attr] {
		if v == field {
			return true
		}
	}
	return false
}

func getFieldFromLine(line []csvparser.Field, colName string, param *ExternalParam) csvparser.Field {
	if catalog.ContainExternalHidenCol(colName) {
		return csvparser.Field{Val: param.Fileparam.Filepath}
	}
	if param.Extern.ExtTab {
		return line[param.Name2ColIndex[strings.ToLower(colName)]]
	}
	return line[param.TbColToDataCol[strings.ToLower(colName)]]
}

// when len(line) >= len(param.TbColToDataCol), call this function to get one row data
func getOneRowDataLineGECol(bat *batch.Batch, line []csvparser.Field, rowIdx int, param *ExternalParam, mp *mpool.MPool) error {
	for colIdx := range param.Attrs {
		if err := getColData(bat, line, rowIdx, param, mp, colIdx); err != nil {
			return err
		}
	}
	return nil
}

func getOneRowDataRestrictive(bat *batch.Batch, line []csvparser.Field, rowIdx int, param *ExternalParam, mp *mpool.MPool) error {
	return getOneRowDataLineGECol(bat, line, rowIdx, param, mp)
}

func getOneRowDataNonRestrictive(bat *batch.Batch, line []csvparser.Field, rowIdx int, param *ExternalParam, mp *mpool.MPool) error {
	if len(line) >= len(param.TbColToDataCol) {
		return getOneRowDataLineGECol(bat, line, rowIdx, param, mp)
	}

	for colIdx, colName := range param.Attrs {
		if param.TbColToDataCol[colName] < int32(len(line)) {
			if err := getColData(bat, line, rowIdx, param, mp, colIdx); err != nil {
				return err
			}
			continue
		}
		vec := bat.Vecs[colIdx]
		if param.Cols[colIdx].Hidden {
			nulls.Add(vec.GetNulls(), uint64(rowIdx))
			continue
		}
		nulls.Add(vec.GetNulls(), uint64(rowIdx))
	}
	return nil
}

func getOneRowData(bat *batch.Batch, line []csvparser.Field, rowIdx int, param *ExternalParam, mp *mpool.MPool) error {
	if checkLineStrict(param) {
		return getOneRowDataRestrictive(bat, line, rowIdx, param, mp)
	}
	return getOneRowDataNonRestrictive(bat, line, rowIdx, param, mp)
}

func getColData(bat *batch.Batch, line []csvparser.Field, rowIdx int, param *ExternalParam, mp *mpool.MPool, colIdx int) error {
	var buf bytes.Buffer

	colName := param.Attrs[colIdx]
	vec := bat.Vecs[colIdx]

	if param.Cols[colIdx].Hidden {
		nulls.Add(vec.GetNulls(), uint64(rowIdx))
		return nil
	}

	field := getFieldFromLine(line, colName, param)
	id := types.T(param.Cols[colIdx].Typ.Id)
	if id != types.T_char && id != types.T_varchar && id != types.T_json &&
		id != types.T_binary && id != types.T_varbinary && id != types.T_blob && id != types.T_text && id != types.T_datalink {
		field.Val = strings.TrimSpace(field.Val)
	}
	isNullOrEmpty := field.IsNull || (getNullFlag(param.Extern.NullMap, param.Attrs[colIdx], field.Val))
	if id != types.T_char && id != types.T_varchar &&
		id != types.T_binary && id != types.T_varbinary && id != types.T_json && id != types.T_blob && id != types.T_text && id != types.T_datalink {
		isNullOrEmpty = isNullOrEmpty || len(field.Val) == 0
	}
	if isNullOrEmpty {
		nulls.Add(vec.GetNulls(), uint64(rowIdx))
		return nil
	}
	if param.ParallelLoad {
		buf.WriteString(field.Val)
		bs := buf.Bytes()
		err := vector.SetBytesAt(vec, rowIdx, bs, mp)
		if err != nil {
			return err
		}
		buf.Reset()
		return nil
	}

	switch id {
	case types.T_bool:
		b, err := types.ParseBool(field.Val)
		if err != nil {
			return moerr.NewInternalError(param.Ctx, "the input value '%s' is not bool type for column %d", field.Val, colIdx)
		}
		if err := vector.SetFixedAt(vec, rowIdx, b); err != nil {
			return err
		}
	case types.T_bit:
		if len(field.Val) > 8 {
			return moerr.NewInternalError(param.Ctx, "data too long, len(val) = %v", len(field.Val))
		}

		width := param.Cols[colIdx].Typ.Width
		var val uint64
		for i := 0; i < len(field.Val); i++ {
			val = (val << 8) | uint64(field.Val[i])
		}
		if val > uint64(1<<width-1) {
			return moerr.NewInternalError(param.Ctx, "data too long, type width = %d, val = %b", width, val)
		}
		if err := vector.SetFixedAt(vec, rowIdx, val); err != nil {
			return err
		}
		buf.Reset()
	case types.T_int8:
		d, err := strconv.ParseInt(field.Val, 10, 8)
		if err == nil {
			if err := vector.SetFixedAt(vec, rowIdx, int8(d)); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int8 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < math.MinInt8 || f > math.MaxInt8 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int8 type for column %d", field.Val, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, int8(f)); err != nil {
				return err
			}
		}
	case types.T_int16:
		d, err := strconv.ParseInt(field.Val, 10, 16)
		if err == nil {
			if err := vector.SetFixedAt(vec, rowIdx, int16(d)); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int16 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < math.MinInt16 || f > math.MaxInt16 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int16 type for column %d", field.Val, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, int16(f)); err != nil {
				return err
			}
		}
	case types.T_int32:
		d, err := strconv.ParseInt(field.Val, 10, 32)
		if err == nil {
			if err := vector.SetFixedAt(vec, rowIdx, int32(d)); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int32 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < math.MinInt32 || f > math.MaxInt32 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int32 type for column %d", field.Val, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, int32(f)); err != nil {
				return err
			}
		}
	case types.T_int64:
		d, err := strconv.ParseInt(field.Val, 10, 64)
		if err == nil {
			if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int64 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < math.MinInt64 || f > math.MaxInt64 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not int64 type for column %d", field.Val, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, int64(f)); err != nil {
				return err
			}
		}
	case types.T_uint8:
		d, err := strconv.ParseUint(field.Val, 10, 8)
		if err == nil {
			if err := vector.SetFixedAt(vec, rowIdx, uint8(d)); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint8 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < 0 || f > math.MaxUint8 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint8 type for column %d", field.Val, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, uint8(f)); err != nil {
				return err
			}
		}
	case types.T_uint16:
		d, err := strconv.ParseUint(field.Val, 10, 16)
		if err == nil {
			if err := vector.SetFixedAt(vec, rowIdx, uint16(d)); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint16 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < 0 || f > math.MaxUint16 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint16 type for column %d", field.Val, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, uint16(f)); err != nil {
				return err
			}
		}
	case types.T_uint32:
		d, err := strconv.ParseUint(field.Val, 10, 32)
		if err == nil {
			if err := vector.SetFixedAt(vec, rowIdx, uint32(d)); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint32 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < 0 || f > math.MaxUint32 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint32 type for column %d", field.Val, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, uint32(f)); err != nil {
				return err
			}
		}
	case types.T_uint64:
		d, err := strconv.ParseUint(field.Val, 10, 64)
		if err == nil {
			if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint64 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < 0 || f > math.MaxUint64 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint64 type for column %d", field.Val, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, uint64(f)); err != nil {
				return err
			}
		}
	case types.T_float32:
		// origin float32 data type
		if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
			d, err := strconv.ParseFloat(field.Val, 32)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not float32 type for column %d", field.Val, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, float32(d)); err != nil {
				return err
			}
		} else {
			d, err := types.ParseDecimal128(field.Val, vec.GetType().Width, vec.GetType().Scale)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not float32 type for column %d", field.Val, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, float32(types.Decimal128ToFloat64(d, vec.GetType().Scale))); err != nil {
				return err
			}
		}
	case types.T_float64:
		// origin float64 data type
		if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
			d, err := strconv.ParseFloat(field.Val, 64)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not float64 type for column %d", field.Val, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
				return err
			}
		} else {
			d, err := types.ParseDecimal128(field.Val, vec.GetType().Width, vec.GetType().Scale)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not float64 type for column %d", field.Val, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, types.Decimal128ToFloat64(d, vec.GetType().Scale)); err != nil {
				return err
			}
		}
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		// XXX Memory accounting?
		buf.WriteString(field.Val)
		bs := buf.Bytes()
		err := vector.SetBytesAt(vec, rowIdx, bs, mp)
		if err != nil {
			return err
		}
		buf.Reset()
	case types.T_array_float32:
		arrBytes, err := types.StringToArrayToBytes[float32](field.Val)
		if err != nil {
			return err
		}
		err = vector.SetBytesAt(vec, rowIdx, arrBytes, mp)
		if err != nil {
			return err
		}
		buf.Reset()
	case types.T_array_float64:
		arrBytes, err := types.StringToArrayToBytes[float64](field.Val)
		if err != nil {
			return err
		}
		err = vector.SetBytesAt(vec, rowIdx, arrBytes, mp)
		if err != nil {
			return err
		}
		buf.Reset()
	case types.T_json:
		var jsonBytes []byte
		if param.Extern.Format != tree.CSV {
			jsonBytes = []byte(field.Val)
		} else {
			field.Val = fmt.Sprintf("%v", strings.Trim(field.Val, "\""))
			byteJson, err := types.ParseStringToByteJson(field.Val)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not json type for column %d", field.Val, colIdx)
			}
			jsonBytes, err = types.EncodeJson(byteJson)
			if err != nil {
				logutil.Errorf("encode json[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not json type for column %d", field.Val, colIdx)
			}
		}

		err := vector.SetBytesAt(vec, rowIdx, jsonBytes, mp)
		if err != nil {
			return err
		}
	case types.T_date:
		d, err := types.ParseDateCast(field.Val)
		if err != nil {
			logutil.Errorf("parse field[%v] err:%v", field.Val, err)
			return moerr.NewInternalError(param.Ctx, "the input value '%v' is not Date type for column %d", field.Val, colIdx)
		}
		if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
			return err
		}
	case types.T_time:
		d, err := types.ParseTime(field.Val, vec.GetType().Scale)
		if err != nil {
			logutil.Errorf("parse field[%v] err:%v", field.Val, err)
			return moerr.NewInternalError(param.Ctx, "the input value '%v' is not Time type for column %d", field.Val, colIdx)
		}
		if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
			return err
		}
	case types.T_datetime:
		d, err := types.ParseDatetime(field.Val, vec.GetType().Scale)
		if err != nil {
			logutil.Errorf("parse field[%v] err:%v", field.Val, err)
			return moerr.NewInternalError(param.Ctx, "the input value '%v' is not Datetime type for column %d", field.Val, colIdx)
		}
		if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
			return err
		}
	case types.T_enum:
		d, err := strconv.ParseUint(field.Val, 10, 16)
		if err == nil {
			if err := vector.SetFixedAt(vec, rowIdx, types.Enum(d)); err != nil {
				return err
			}
		} else if errors.Is(err, strconv.ErrSyntax) {
			v, err := types.ParseEnum(param.Cols[colIdx].Typ.Enumvalues, field.Val)
			if err != nil {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return err
			}
			if err := vector.SetFixedAt(vec, rowIdx, types.Enum(v)); err != nil {
				return err
			}
		} else {
			if errors.Is(err, strconv.ErrRange) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint16 type for column %d", field.Val, colIdx)
			}
			f, err := strconv.ParseFloat(field.Val, 64)
			if err != nil || f < 0 || f > math.MaxUint16 {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uint16 type for column %d", field.Val, colIdx)
			}
			if err := vector.SetFixedAt(vec, rowIdx, types.Enum(f)); err != nil {
				return err
			}
		}
	case types.T_decimal64:
		d, err := types.ParseDecimal64(field.Val, vec.GetType().Width, vec.GetType().Scale)
		if err != nil {
			// we tolerate loss of digits.
			if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is invalid Decimal64 type for column %d", field.Val, colIdx)
			}
		}
		if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
			return err
		}
	case types.T_decimal128:
		d, err := types.ParseDecimal128(field.Val, vec.GetType().Width, vec.GetType().Scale)
		if err != nil {
			// we tolerate loss of digits.
			if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
				logutil.Errorf("parse field[%v] err:%v", field.Val, err)
				return moerr.NewInternalError(param.Ctx, "the input value '%v' is invalid Decimal128 type for column %d", field.Val, colIdx)
			}
		}
		if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
			return err
		}
	case types.T_timestamp:
		t := time.Local
		d, err := types.ParseTimestamp(t, field.Val, vec.GetType().Scale)
		if err != nil {
			logutil.Errorf("parse field[%v] err:%v", field.Val, err)
			return moerr.NewInternalError(param.Ctx, "the input value '%v' is not Timestamp type for column %d", field.Val, colIdx)
		}
		if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
			return err
		}
	case types.T_uuid:
		d, err := types.ParseUuid(field.Val)
		if err != nil {
			logutil.Errorf("parse field[%v] err:%v", field.Val, err)
			return moerr.NewInternalError(param.Ctx, "the input value '%v' is not uuid type for column %d", field.Val, colIdx)
		}
		if err := vector.SetFixedAt(vec, rowIdx, d); err != nil {
			return err
		}
	default:
		return moerr.NewInternalError(param.Ctx, "the value type %d is not support now", param.Cols[rowIdx].Typ.Id)
	}
	return nil
}

// Read reads len count records from r.
// Each record is a slice of fields.
// A successful call returns err == nil, not err == io.EOF. Because ReadAll is
// defined to read until EOF, it does not treat an end of file as an error to be
// reported.
func readCountStringLimitSize(r *csvparser.CSVParser, ctx context.Context, size uint64, records [][]csvparser.Field) (int, bool, error) {
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
			curBatchSize += uint64(len(record[j].Val))
		}
		if curBatchSize >= size {
			return i + 1, false, nil
		}
	}
	return OneBatchMaxRow, false, nil
}

func loadFormatIsValid(param *tree.ExternParam) bool {
	switch param.Format {
	case tree.JSONLINE, tree.CSV, tree.PARQUET:
		return true
	}
	return false
}

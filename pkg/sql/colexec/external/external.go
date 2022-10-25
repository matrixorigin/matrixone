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
	"bytes"
	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"

	"math"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/matrixorigin/simdcsv"
	"github.com/pierrec/lz4"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("external output")
}

func Prepare(proc *process.Process, arg any) error {
	param := arg.(*Argument).Es
	param.batchSize = 40000
	param.records = make([][]string, param.batchSize)
	param.extern = &tree.ExternParam{}
	err := json.Unmarshal([]byte(param.CreateSql), param.extern)
	if err != nil {
		param.Fileparam.End = true
		return err
	}
	/*if param.extern.Format != tree.CSV && param.extern.Format != tree.JSONLINE {
		param.End = true
		return moerr.NewNotSupported("the format '%s' is not supported now", param.extern.Format)
	}*/
	if param.extern.Format == tree.JSONLINE {
		if param.extern.JsonData != tree.OBJECT && param.extern.JsonData != tree.ARRAY {
			param.Fileparam.End = true
			return moerr.NewNotSupported("the jsonline format '%s' is not supported now", param.extern.JsonData)
		}
	}
	param.extern.FileService = proc.FileService
	param.IgnoreLineTag = int(param.extern.Tail.IgnoredLines)
	param.IgnoreLine = param.IgnoreLineTag
	fileList, err := ReadDir(param.extern)
	if err != nil {
		param.Fileparam.End = true
		return err
	}

	if len(fileList) == 0 {
		logutil.Warnf("no such file '%s'", param.extern.Filepath)
		param.Fileparam.End = true
	}
	param.FileList = fileList
	param.extern.Filepath = ""
	param.Fileparam.FileCnt = len(fileList)
	return nil
}

func Call(_ int, proc *process.Process, arg any) (bool, error) {
	param := arg.(*Argument).Es
	param.Fileparam.mu.Lock()
	if param.Fileparam.End {
		param.Fileparam.mu.Unlock()
		proc.SetInputBatch(nil)
		return true, nil
	}
	if param.extern.Filepath == "" {
		if param.Fileparam.FileIndex >= len(param.FileList) {
			param.Fileparam.mu.Unlock()
			proc.SetInputBatch(nil)
			return true, nil
		}
		param.extern.Filepath = param.FileList[param.Fileparam.FileIndex]
		param.Fileparam.FileIndex++
	}
	param.Fileparam.mu.Unlock()
	bat, err := ScanFileData(param, proc)
	if err != nil {
		param.Fileparam.End = true
		return false, err
	}
	proc.SetInputBatch(bat)
	return false, nil
}

func ReadDir(param *tree.ExternParam) (fileList []string, err error) {
	ctx := context.TODO()

	filePath := strings.TrimSpace(param.Filepath)
	pathDir := strings.Split(filePath, "/")
	l := list.New()
	if pathDir[0] == "" {
		l.PushBack("/")
	} else {
		l.PushBack(pathDir[0])
	}

	for i := 1; i < len(pathDir); i++ {
		length := l.Len()
		for j := 0; j < length; j++ {
			prefix := l.Front().Value.(string)
			fs, readPath, err := fileservice.GetForETL(param.FileService, prefix)
			if err != nil {
				return nil, err
			}
			entries, err := fs.List(ctx, readPath)
			if err != nil {
				return nil, err
			}
			for _, entry := range entries {
				if !entry.IsDir && i+1 != len(pathDir) {
					continue
				}
				if entry.IsDir && i+1 == len(pathDir) {
					continue
				}
				matched, _ := filepath.Match(pathDir[i], entry.Name)
				if !matched {
					continue
				}
				l.PushBack(path.Join(l.Front().Value.(string), entry.Name))
			}
			l.Remove(l.Front())
		}
	}
	len := l.Len()
	for j := 0; j < len; j++ {
		fileList = append(fileList, l.Front().Value.(string))
		l.Remove(l.Front())
	}
	return fileList, err
}

func ReadFile(param *tree.ExternParam) (io.ReadCloser, error) {
	fs, readPath, err := fileservice.GetForETL(param.FileService, param.Filepath)
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
	ctx := context.TODO()
	err = fs.Read(ctx, &vec)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func getCompressType(param *tree.ExternParam) string {
	if param.CompressType != "" && param.CompressType != tree.AUTO {
		return param.CompressType
	}
	index := strings.LastIndex(param.Filepath, ".")
	if index == -1 {
		return tree.NOCOMPRESS
	}
	tail := string([]byte(param.Filepath)[index+1:])
	switch tail {
	case "gz":
		return tree.GZIP
	case "bz2":
		return tree.BZIP2
	case "lz4":
		return tree.LZ4
	default:
		return tree.NOCOMPRESS
	}
}

func getUnCompressReader(param *tree.ExternParam, r io.ReadCloser) (io.ReadCloser, error) {
	switch strings.ToLower(getCompressType(param)) {
	case tree.NOCOMPRESS:
		return r, nil
	case tree.GZIP:
		r, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		return r, nil
	case tree.BZIP2:
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
		return nil, moerr.NewInternalError("the compress type '%s' is not support now", param.CompressType)
	default:
		return nil, moerr.NewInternalError("the compress type '%s' is not support now", param.CompressType)
	}
}

func makeBatch(param *ExternalParam, plh *ParseLineHandler, mp *mpool.MPool) *batch.Batch {
	batchData := batch.New(true, param.Attrs)
	batchSize := plh.batchSize
	//alloc space for vector
	for i := 0; i < len(param.Attrs); i++ {
		typ := types.New(types.T(param.Cols[i].Typ.Id), param.Cols[i].Typ.Width, param.Cols[i].Typ.Scale, param.Cols[i].Typ.Precision)
		vec := vector.NewOriginal(typ)
		vector.PreAlloc(vec, batchSize, batchSize, mp)
		vec.SetOriginal(false)
		batchData.Vecs[i] = vec
	}
	return batchData
}

func deleteEnclosed(param *ExternalParam, plh *ParseLineHandler) {
	close := param.extern.Tail.Fields.EnclosedBy
	if close == '"' || close == 0 {
		return
	}
	for rowIdx := 0; rowIdx < plh.batchSize; rowIdx++ {
		Line := plh.simdCsvLineArray[rowIdx]
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

func GetBatchData(param *ExternalParam, plh *ParseLineHandler, proc *process.Process) (*batch.Batch, error) {
	bat := makeBatch(param, plh, proc.Mp())
	var (
		Line []string
		err  error
	)
	deleteEnclosed(param, plh)
	for rowIdx := 0; rowIdx < plh.batchSize; rowIdx++ {
		Line = plh.simdCsvLineArray[rowIdx]
		if param.extern.Format == tree.JSONLINE {
			Line, err = transJson2Lines(Line[0], param.Attrs, param.extern.JsonData)
			if err != nil {
				return nil, err
			}
			plh.simdCsvLineArray[rowIdx] = Line
		}
		if len(Line) < len(param.Attrs) {
			return nil, errColumnCntLarger
		}
		err = getData(bat, Line, rowIdx, param, proc.Mp())
		if err != nil {
			return nil, err
		}
	}

	n := vector.Length(bat.Vecs[0])
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

// GetSimdcsvReader get file reader from external file
func GetSimdcsvReader(param *ExternalParam) (*ParseLineHandler, error) {
	var err error
	param.reader, err = ReadFile(param.extern)
	if err != nil {
		return nil, err
	}
	param.reader, err = getUnCompressReader(param.extern, param.reader)
	if err != nil {
		return nil, err
	}

	channelSize := 100
	plh := &ParseLineHandler{}
	plh.batchSize = param.batchSize
	plh.simdCsvGetParsedLinesChan = atomic.Value{}
	plh.simdCsvGetParsedLinesChan.Store(make(chan simdcsv.LineOut, channelSize))
	if param.extern.Tail.Fields == nil {
		param.extern.Tail.Fields = &tree.Fields{Terminated: ","}
	}
	if param.extern.Format == tree.JSONLINE {
		param.extern.Tail.Fields.Terminated = "\t"
	}
	plh.simdCsvReader = simdcsv.NewReaderWithOptions(param.reader,
		rune(param.extern.Tail.Fields.Terminated[0]),
		'#',
		true,
		true)

	return plh, nil
}

// ScanFileData read batch data from external file
func ScanFileData(param *ExternalParam, proc *process.Process) (*batch.Batch, error) {
	var bat *batch.Batch
	var err error
	var cnt int
	if param.plh == nil {
		param.IgnoreLine = param.IgnoreLineTag
		param.plh, err = GetSimdcsvReader(param)
		if err != nil {
			return nil, err
		}
	}
	plh := param.plh
	plh.simdCsvLineArray, cnt, err = plh.simdCsvReader.Read(param.batchSize, proc.Ctx, param.records)
	if err != nil {
		return nil, err
	}
	if cnt < param.batchSize {
		plh.simdCsvLineArray = plh.simdCsvLineArray[:cnt]
		err := param.reader.Close()
		if err != nil {
			logutil.Errorf("close file failed. err:%v", err)
		}
		plh.simdCsvReader.Close()
		param.plh = nil
		param.Fileparam.mu.Lock()
		param.Fileparam.FileFin++
		param.extern.Filepath = ""
		if param.Fileparam.FileFin >= param.Fileparam.FileCnt {
			param.Fileparam.End = true
		}
		param.Fileparam.mu.Unlock()
	}
	if param.IgnoreLine != 0 {
		if len(plh.simdCsvLineArray) >= param.IgnoreLine {
			plh.simdCsvLineArray = plh.simdCsvLineArray[param.IgnoreLine:]
		} else {
			plh.simdCsvLineArray = nil
		}
		param.IgnoreLine = 0
	}
	plh.batchSize = len(plh.simdCsvLineArray)
	bat, err = GetBatchData(param, plh, proc)
	if err != nil {
		return nil, err
	}
	bat.Cnt = 1
	return bat, nil
}

func transJson2Lines(str string, attrs []string, jsonData string) ([]string, error) {
	switch jsonData {
	case tree.OBJECT:
		return transJsonObject2Lines(str, attrs)
	case tree.ARRAY:
		return transJsonArray2Lines(str, attrs)
	default:
		return nil, moerr.NewNotSupported("the jsonline format '%s' is not support now", jsonData)
	}
}

func transJsonObject2Lines(str string, attrs []string) ([]string, error) {
	var (
		err error
		res = make([]string, 0, len(attrs))
	)
	var jsonMap map[string]interface{}
	err = json.Unmarshal([]byte(str), &jsonMap)
	if err != nil {
		logutil.Errorf("json unmarshal err:%v", err)
		return nil, err
	}
	if len(jsonMap) < len(attrs) {
		return nil, errColumnCntLarger
	}
	for _, attr := range attrs {
		if val, ok := jsonMap[attr]; ok {
			res = append(res, fmt.Sprintf("%v", val))
		} else {
			return nil, moerr.NewInvalidInput("the attr %s is not in json", attr)
		}
	}
	return res, nil
}

func transJsonArray2Lines(str string, attrs []string) ([]string, error) {
	var (
		err error
		res = make([]string, 0, len(attrs))
	)
	var jsonArray []interface{}
	err = json.Unmarshal([]byte(str), &jsonArray)
	if err != nil {
		logutil.Errorf("json unmarshal err:%v", err)
		return nil, err
	}
	if len(jsonArray) < len(attrs) {
		return nil, errColumnCntLarger
	}
	for idx := range attrs {
		res = append(res, fmt.Sprintf("%v", jsonArray[idx]))
	}
	return res, nil
}

func getNullFlag(param *ExternalParam, attr, field string) bool {
	list := param.extern.NullMap[attr]
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

func getData(bat *batch.Batch, Line []string, rowIdx int, param *ExternalParam, mp *mpool.MPool) error {
	for colIdx := range param.Attrs {
		field := Line[param.Name2ColIndex[param.Attrs[colIdx]]]
		id := types.T(param.Cols[colIdx].Typ.Id)
		if id != types.T_char && id != types.T_varchar {
			field = strings.TrimSpace(field)
		}
		vec := bat.Vecs[colIdx]
		isNullOrEmpty := field == NULL_FLAG
		if id != types.T_char && id != types.T_varchar && id != types.T_json && id != types.T_blob && id != types.T_text {
			isNullOrEmpty = isNullOrEmpty || len(field) == 0
		}
		isNullOrEmpty = isNullOrEmpty || (getNullFlag(param, param.Attrs[colIdx], field))
		switch id {
		case types.T_bool:
			cols := vector.MustTCols[bool](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				if field == "true" || field == "1" {
					cols[rowIdx] = true
				} else if field == "false" || field == "0" {
					cols[rowIdx] = false
				} else {
					return moerr.NewInternalError("the input value '%s' is not bool type for column %d", field, colIdx)
				}
			}
		case types.T_int8:
			//cols := vec.Col.([]int8)
			cols := vector.MustTCols[int8](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseInt(field, 10, 8)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not int8 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int8(d)
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < math.MinInt8 || d > math.MaxInt8 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not int8 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int8(d)
				}
			}
		case types.T_int16:
			//cols := vec.Col.([]int16)
			cols := vector.MustTCols[int16](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseInt(field, 10, 16)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not int16 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int16(d)
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < math.MinInt16 || d > math.MaxInt16 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not int16 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int16(d)
				}
			}
		case types.T_int32:
			//cols := vec.Col.([]int32)
			cols := vector.MustTCols[int32](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseInt(field, 10, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not int32 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int32(d)
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < math.MinInt32 || d > math.MaxInt32 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not int32 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int32(d)
				}
			}
		case types.T_int64:
			//cols := vec.Col.([]int64)
			cols := vector.MustTCols[int64](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseInt(field, 10, 64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not int64 type for column %d", field, colIdx)
					}
					cols[rowIdx] = d
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < math.MinInt64 || d > math.MaxInt64 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not int64 type for column %d", field, colIdx)
					}
					cols[rowIdx] = int64(d)
				}
			}
		case types.T_uint8:
			//cols := vec.Col.([]uint8)
			cols := vector.MustTCols[uint8](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseUint(field, 10, 8)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not uint8 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint8(d)
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < 0 || d > math.MaxUint8 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not uint8 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint8(d)
				}
			}
		case types.T_uint16:
			//cols := vec.Col.([]uint16)
			cols := vector.MustTCols[uint16](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseUint(field, 10, 16)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not uint16 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint16(d)
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < 0 || d > math.MaxUint16 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not uint16 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint16(d)
				}
			}
		case types.T_uint32:
			//cols := vec.Col.([]uint32)
			cols := vector.MustTCols[uint32](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseUint(field, 10, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not uint32 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint32(d)
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < 0 || d > math.MaxUint32 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not uint32 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint32(d)
				}
			}
		case types.T_uint64:
			//cols := vec.Col.([]uint64)
			cols := vector.MustTCols[uint64](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				if judgeInteger(field) {
					d, err := strconv.ParseUint(field, 10, 64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not uint64 type for column %d", field, colIdx)
					}
					cols[rowIdx] = d
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < 0 || d > math.MaxUint64 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is not uint64 type for column %d", field, colIdx)
					}
					cols[rowIdx] = uint64(d)
				}
			}
		case types.T_float32:
			//cols := vec.Col.([]float32)
			cols := vector.MustTCols[float32](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				d, err := strconv.ParseFloat(field, 32)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError("the input value '%v' is not float32 type for column %d", field, colIdx)
				}
				cols[rowIdx] = float32(d)
			}
		case types.T_float64:
			//cols := vec.Col.([]float64)
			cols := vector.MustTCols[float64](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError("the input value '%v' is not float64 type for column %d", field, colIdx)
				}
				cols[rowIdx] = d
			}
		case types.T_char, types.T_varchar, types.T_blob, types.T_text:
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				// XXX Memory accounting?
				err := vector.SetStringAt(vec, rowIdx, field, mp)
				if err != nil {
					return err
				}
			}
		case types.T_json:
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				byteJson, err := types.ParseStringToByteJson(field)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError("the input value '%v' is not json type for column %d", field, colIdx)
				}
				jsonBytes, err := types.EncodeJson(byteJson)
				if err != nil {
					logutil.Errorf("encode json[%v] err:%v", field, err)
					return moerr.NewInternalError("the input value '%v' is not json type for column %d", field, colIdx)
				}
				err = vector.SetBytesAt(vec, rowIdx, jsonBytes, mp)
				if err != nil {
					return err
				}
			}
		case types.T_date:
			//cols := vec.Col.([]types.Date)
			cols := vector.MustTCols[types.Date](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				d, err := types.ParseDate(field)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError("the input value '%v' is not Date type for column %d", field, colIdx)
				}
				cols[rowIdx] = d
			}
		case types.T_time:
			//cols := vec.Col.([]types.Time)
			cols := vector.MustTCols[types.Time](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				d, err := types.ParseTime(field, vec.Typ.Precision)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError("the input value '%v' is not Datetime type for column %d", field, colIdx)
				}
				cols[rowIdx] = d
			}
		case types.T_datetime:
			//cols := vec.Col.([]types.Datetime)
			cols := vector.MustTCols[types.Datetime](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				d, err := types.ParseDatetime(field, vec.Typ.Precision)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError("the input value '%v' is not Datetime type for column %d", field, colIdx)
				}
				cols[rowIdx] = d
			}
		case types.T_decimal64:
			//cols := vec.Col.([]types.Decimal64)
			cols := vector.MustTCols[types.Decimal64](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				d, err := types.Decimal64_FromStringWithScale(field, vec.Typ.Width, vec.Typ.Scale)
				if err != nil {
					// we tolerate loss of digits.
					if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is invalid Decimal64 type for column %d", field, colIdx)
					}
				}
				cols[rowIdx] = d
			}
		case types.T_decimal128:
			//cols := vec.Col.([]types.Decimal128)
			cols := vector.MustTCols[types.Decimal128](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				d, err := types.Decimal128_FromStringWithScale(field, vec.Typ.Width, vec.Typ.Scale)
				if err != nil {
					// we tolerate loss of digits.
					if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return moerr.NewInternalError("the input value '%v' is invalid Decimal128 type for column %d", field, colIdx)
					}
				}
				cols[rowIdx] = d
			}
		case types.T_timestamp:
			//cols := vec.Col.([]types.Timestamp)
			cols := vector.MustTCols[types.Timestamp](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				d, err := types.ParseTimestamp(time.UTC, field, vec.Typ.Precision)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError("the input value '%v' is not Timestamp type for column %d", field, colIdx)
				}
				cols[rowIdx] = d
			}
		case types.T_uuid:
			cols := vector.MustTCols[types.Uuid](vec)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
			} else {
				d, err := types.ParseUuid(field)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return moerr.NewInternalError("the input value '%v' is not uuid type for column %d", field, colIdx)
				}
				cols[rowIdx] = d
			}
		default:
			return moerr.NewInternalError("the value type %d is not support now", param.Cols[rowIdx].Typ.Id)
		}
	}
	return nil
}

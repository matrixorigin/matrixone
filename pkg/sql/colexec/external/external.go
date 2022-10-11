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
	"context"
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/multi"
	"github.com/matrixorigin/matrixone/pkg/vectorize/external"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/matrixorigin/simdcsv"
	"github.com/pierrec/lz4"
	"io"
	"path"
	"strings"
	"sync/atomic"
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
		param.End = true
		return err
	}
	if param.extern.Format != tree.CSV && param.extern.Format != tree.JSONLINE {
		param.End = true
		return moerr.NewNotSupported("the format '%s' is not supported now", param.extern.Format)
	}
	if param.extern.Format == tree.JSONLINE {
		if param.extern.JsonData != tree.OBJECT && param.extern.JsonData != tree.ARRAY {
			param.End = true
			return moerr.NewNotSupported("the jsonline format '%s' is not supported now", param.extern.JsonData)
		}
	}
	param.extern.FileService = proc.FileService
	param.IgnoreLineTag = int(param.extern.Tail.IgnoredLines)
	param.IgnoreLine = param.IgnoreLineTag
	fileList, err := ReadDir(param.extern)
	if err != nil {
		param.End = true
		return err
	}

	if len(fileList) == 0 {
		logutil.Warnf("no such file '%s'", param.extern.Filepath)
		param.End = true
	}
	param.FileList = fileList
	param.FileCnt = len(fileList)
	return nil
}

func Call(_ int, proc *process.Process, arg any) (bool, error) {
	param := arg.(*Argument).Es
	if param.End {
		proc.SetInputBatch(nil)
		return true, nil
	}
	param.extern.Filepath = param.FileList[param.FileIndex]
	bat, err := ScanFileData(param, proc)
	if err != nil {
		param.End = true
		return false, err
	}
	proc.SetInputBatch(bat)
	return false, nil
}

func ReadDir(param *tree.ExternParam) (fileList []string, err error) {
	dir, pattern := path.Split(param.Filepath)
	fs, readPath, err := fileservice.GetForETL(param.FileService, dir+"/")
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	entries, err := fs.List(ctx, readPath)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		matched, _ := path.Match(pattern, entry.Name)
		if !matched {
			continue
		}
		fileList = append(fileList, path.Join(dir, entry.Name))
	}
	return
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
	originBat := makeOriginBatch(param, plh, proc.Mp())
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

		for j := 0; j < len(param.Attrs); j++ {
			vec := originBat.GetVector(int32(j))
			field := Line[param.Name2ColIndex[param.Attrs[j]]]
			err = vector.SetStringAt(vec, rowIdx, field, proc.Mp())
			if err != nil {
				return nil, err
			}
		}
	}

	for i := 0; i < len(param.Attrs); i++ {
		id := types.T(param.Cols[i].Typ.Id)
		nullList := param.extern.NullMap[param.Attrs[i]]
		nullVec := vector.New(types.Type{Oid: types.T_varchar, Width: 1024})
		vector.PreAlloc(nullVec, len(nullList), len(nullList), proc.Mp())
		for j := 0; j < len(nullList); j++ {
			err = vector.SetStringAt(nullVec, j, nullList[j], proc.Mp())
			if err != nil {
				return nil, err
			}
		}
		vectors := []*vector.Vector{
			originBat.GetVector(int32(i)),
			bat.GetVector(int32(i)),
			nullVec,
		}
		switch id {
		case types.T_bool:
			bat.Vecs[i], err = multi.ParseNumber[bool](vectors, proc, external.ParseBool)
		case types.T_int8:
			bat.Vecs[i], err = multi.ParseNumber[int8](vectors, proc, external.ParseInt8)
		case types.T_int16:
			bat.Vecs[i], err = multi.ParseNumber[int16](vectors, proc, external.ParseInt16)
		case types.T_int32:
			bat.Vecs[i], err = multi.ParseNumber[int32](vectors, proc, external.ParseInt32)
		case types.T_int64:
			bat.Vecs[i], err = multi.ParseNumber[int64](vectors, proc, external.ParseInt64)
		case types.T_uint8:
			bat.Vecs[i], err = multi.ParseNumber[uint8](vectors, proc, external.ParseUint8)
		case types.T_uint16:
			bat.Vecs[i], err = multi.ParseNumber[uint16](vectors, proc, external.ParseUint16)
		case types.T_uint32:
			bat.Vecs[i], err = multi.ParseNumber[uint32](vectors, proc, external.ParseUint32)
		case types.T_uint64:
			bat.Vecs[i], err = multi.ParseNumber[uint64](vectors, proc, external.ParseUint64)
		case types.T_float32:
			bat.Vecs[i], err = multi.ParseNumber[float32](vectors, proc, external.ParseFloat32)
		case types.T_float64:
			bat.Vecs[i], err = multi.ParseNumber[float64](vectors, proc, external.ParseFloat64)
		case types.T_date:
			bat.Vecs[i], err = multi.ParseNumber[types.Date](vectors, proc, external.ParseDate)
		case types.T_datetime:
			bat.Vecs[i], err = multi.ParseTime[types.Datetime](vectors, proc, external.ParseDateTime)
		case types.T_timestamp:
			bat.Vecs[i], err = multi.ParseTime[types.Timestamp](vectors, proc, external.ParseTimeStamp)
		case types.T_decimal64:
			bat.Vecs[i], err = multi.ParseDecimal[types.Decimal64](vectors, proc, external.ParseDecimal64)
		case types.T_decimal128:
			bat.Vecs[i], err = multi.ParseDecimal[types.Decimal128](vectors, proc, external.ParseDecimal128)
		case types.T_char, types.T_varchar, types.T_blob:
			bat.Vecs[i], err = multi.ParseString(vectors, proc)
		case types.T_json:
			bat.Vecs[i], err = multi.ParseJson(vectors, proc)
		default:
			err = moerr.NewNotSupported("the value type %d is not support now", param.Cols[i].Typ.Id)
		}
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
	plh.simdCsvLineArray, cnt, err = plh.simdCsvReader.Read(param.batchSize, param.Ctx, param.records)
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
		param.FileIndex++
		if param.FileIndex >= param.FileCnt {
			param.End = true
		}
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

func makeOriginBatch(param *ExternalParam, plh *ParseLineHandler, mp *mpool.MPool) *batch.Batch {
	batchData := batch.New(true, param.Attrs)
	batchSize := plh.batchSize
	//alloc space for vector
	for i := 0; i < len(param.Attrs); i++ {
		typ := types.New(types.T_varchar, 0, 0, 0)
		vec := vector.NewOriginal(typ)
		vector.PreAlloc(vec, batchSize, batchSize, mp)
		batchData.Vecs[i] = vec
	}
	return batchData
}

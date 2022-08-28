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
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	buf.WriteString("sql output")
}

func Prepare(_ *process.Process, arg any) error {
	param := arg.(*Argument).Es
	param.batchSize = 40000
	param.extern = &tree.ExternParam{}
	err := json.Unmarshal([]byte(param.CreateSql), param.extern)
	if err != nil {
		param.End = true
		return err
	}
	param.IgnoreLineTag = int(param.extern.Tail.IgnoredLines)
	param.IgnoreLine = param.IgnoreLineTag
	fileList, err := getFileDataList(param.extern)
	if err != nil {
		param.End = true
		return err
	}

	if len(fileList) == 0 {
		param.End = true
		return fmt.Errorf("no such file '%s'", param.extern.Filepath)
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

func ReadFromS3(param *tree.ExternParam) ([]string, error) {
	var config fileservice.S3Config
	config.Bucket = param.S3Param.Config.Bucket
	config.Endpoint = param.S3Param.Config.Endpoint

	if param.S3Param.APIKey != "" {
		os.Setenv("AWS_REGION", param.S3Param.Region)
		os.Setenv("AWS_ACCESS_KEY_ID", param.S3Param.APIKey)
		os.Setenv("AWS_SECRET_ACCESS_KEY", param.S3Param.APISecret)
	}

	fs, err := fileservice.NewS3FS(
		"",
		"s3",
		config.Endpoint,
		config.Bucket,
		config.KeyPrefix,
		128*1024,
	)
	if err != nil {
		return nil, err
	}

	index := strings.LastIndex(param.Filepath, "/")
	dir, file := "", param.Filepath
	if index != -1 {
		dir = string([]byte(param.Filepath)[0:index])
		file = string([]byte(param.Filepath)[index+1:])
	}

	ctx := context.Background()
	DirEntry, err := fs.List(ctx, dir)
	if err != nil {
		return nil, err
	}

	var fileList []string
	for _, entry := range DirEntry {
		matched, _ := path.Match(file, entry.Name)
		if matched {
			if dir != "" {
				fileList = append(fileList, dir+"/"+entry.Name)
			} else {
				fileList = append(fileList, entry.Name)
			}
		}
	}
	return fileList, nil
}

func ReadFromS3File(param *tree.ExternParam) (io.ReadCloser, error) {
	var config fileservice.S3Config
	config.Bucket = param.S3Param.Config.Bucket
	config.Endpoint = param.S3Param.Config.Endpoint

	if param.S3Param.APIKey != "" {
		os.Setenv("AWS_REGION", param.S3Param.Region)
		os.Setenv("AWS_ACCESS_KEY_ID", param.S3Param.APIKey)
		os.Setenv("AWS_SECRET_ACCESS_KEY", param.S3Param.APISecret)
	}

	fs, err := fileservice.NewS3FS(
		"",
		"s3",
		config.Endpoint,
		config.Bucket,
		config.KeyPrefix,
		128*1024,
	)
	if err != nil {
		return nil, err
	}

	var r io.ReadCloser
	vec := fileservice.IOVector{
		FilePath: param.Filepath,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            0,
				Size:              -1,
				ReadCloserForRead: &r,
			},
		},
	}
	ctx := context.Background()
	err = fs.Read(ctx, &vec)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func ReadFromLocal(param *tree.ExternParam) ([]string, error) {
	index := strings.LastIndex(param.Filepath, "/")
	dir, file := "", param.Filepath
	if index != -1 {
		dir = string([]byte(param.Filepath)[0:index])
		file = string([]byte(param.Filepath)[index+1:])
	}

	fs, err := fileservice.NewLocalETLFS("etl", dir)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	DirEntry, err := fs.List(ctx, "")
	if err != nil {
		return nil, err
	}

	var fileList []string
	for _, entry := range DirEntry {
		matched, _ := path.Match(file, entry.Name)
		if matched {
			fileList = append(fileList, dir+"/"+entry.Name)
		}
	}
	return fileList, nil
}

func ReadFromLocalFile(param *tree.ExternParam) (io.ReadCloser, error) {
	var r io.ReadCloser
	index := strings.LastIndex(param.Filepath, "/")
	dir, file := "", param.Filepath
	if index != -1 {
		dir = string([]byte(param.Filepath)[0:index])
		file = string([]byte(param.Filepath)[index+1:])
	}

	fs, err := fileservice.NewLocalETLFS("etl", dir)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	vec := fileservice.IOVector{
		FilePath: file,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            0,
				Size:              -1,
				ReadCloserForRead: &r,
			},
		},
	}
	err = fs.Read(ctx, &vec)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func InitS3Param(param *tree.ExternParam) error {
	param.S3Param = &tree.S3Parameter{}
	for i := 0; i < len(param.S3option); i += 2 {
		switch strings.ToLower(param.S3option[i]) {
		case "endpoint":
			param.S3Param.Config.Endpoint = param.S3option[i+1]
		case "region":
			param.S3Param.Region = param.S3option[i+1]
		case "access_key_id":
			param.S3Param.APIKey = param.S3option[i+1]
		case "secret_access_key":
			param.S3Param.APISecret = param.S3option[i+1]
		case "bucket":
			param.S3Param.Config.Bucket = param.S3option[i+1]
		case "filepath":
			param.Filepath = param.S3option[i+1]
		case "compression":
			param.CompressType = param.S3option[i+1]
		default:
			return fmt.Errorf("the keyword '%s' is not support", strings.ToLower(param.S3option[i]))
		}
	}
	return nil
}

func getFileDataList(param *tree.ExternParam) ([]string, error) {
	switch param.ScanType {
	case tree.LOCAL:
		fileList, err := ReadFromLocal(param)
		if err != nil {
			return nil, err
		}
		return fileList, nil
	case tree.S3, tree.MinIO:
		err := InitS3Param(param)
		if err != nil {
			return nil, err
		}
		fileList, err := ReadFromS3(param)
		if err != nil {
			return nil, err
		}
		return fileList, nil
	default:
		return nil, errors.New("the extern file type is not support now")
	}
}

func getFileDataReader(param *tree.ExternParam) (io.ReadCloser, error) {
	switch param.ScanType {
	case tree.LOCAL:
		reader, err := ReadFromLocalFile(param)
		if err != nil {
			return nil, err
		}
		return reader, nil
	case tree.S3, tree.MinIO:
		reader, err := ReadFromS3File(param)
		if err != nil {
			return nil, err
		}
		return reader, nil
	default:
		return nil, errors.New("the extern file type is not support now")
	}
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
		return nil, fmt.Errorf("the compress type '%s' is not support now", param.CompressType)
	default:
		return nil, fmt.Errorf("the compress type '%s' is not support now", param.CompressType)
	}
}

const NULL_FLAG = "\\N"

func judgeInterge(field string) bool {
	for i := 0; i < len(field); i++ {
		if field[i] > '9' || field[i] < '0' {
			return false
		}
	}
	return true
}

func makeBatch(param *ExternalParam, plh *ParseLineHandler) *batch.Batch {
	batchData := batch.New(true, param.Attrs)
	batchSize := plh.batchSize
	//alloc space for vector
	for i := 0; i < len(param.Attrs); i++ {
		typ := types.New(types.T(param.Cols[i].Typ.Id), param.Cols[i].Typ.Width, param.Cols[i].Typ.Scale, param.Cols[i].Typ.Precision)
		vec := vector.New(typ)
		vec.Or = true
		switch vec.Typ.Oid {
		case types.T_bool:
			vec.Data = make([]byte, batchSize)
			vec.Col = types.DecodeBoolSlice(vec.Data)
		case types.T_int8:
			vec.Data = make([]byte, batchSize)
			vec.Col = types.DecodeInt8Slice(vec.Data)
		case types.T_int16:
			vec.Data = make([]byte, 2*batchSize)
			vec.Col = types.DecodeInt16Slice(vec.Data)
		case types.T_int32:
			vec.Data = make([]byte, 4*batchSize)
			vec.Col = types.DecodeInt32Slice(vec.Data)
		case types.T_int64:
			vec.Data = make([]byte, 8*batchSize)
			vec.Col = types.DecodeInt64Slice(vec.Data)
		case types.T_uint8:
			vec.Data = make([]byte, batchSize)
			vec.Col = types.DecodeUint8Slice(vec.Data)
		case types.T_uint16:
			vec.Data = make([]byte, 2*batchSize)
			vec.Col = types.DecodeUint16Slice(vec.Data)
		case types.T_uint32:
			vec.Data = make([]byte, 4*batchSize)
			vec.Col = types.DecodeUint32Slice(vec.Data)
		case types.T_uint64:
			vec.Data = make([]byte, 8*batchSize)
			vec.Col = types.DecodeUint64Slice(vec.Data)
		case types.T_float32:
			vec.Data = make([]byte, 4*batchSize)
			vec.Col = types.DecodeFloat32Slice(vec.Data)
		case types.T_float64:
			vec.Data = make([]byte, 8*batchSize)
			vec.Col = types.DecodeFloat64Slice(vec.Data)
		case types.T_char, types.T_varchar, types.T_json:
			vBytes := &types.Bytes{
				Offsets: make([]uint32, batchSize),
				Lengths: make([]uint32, batchSize),
				Data:    nil,
			}
			vec.Col = vBytes
			vec.Data = make([]byte, batchSize)
		case types.T_date:
			vec.Data = make([]byte, 4*batchSize)
			vec.Col = types.DecodeDateSlice(vec.Data)
		case types.T_datetime:
			vec.Data = make([]byte, 8*batchSize)
			vec.Col = types.DecodeDatetimeSlice(vec.Data)
		case types.T_decimal64:
			vec.Data = make([]byte, 8*batchSize)
			vec.Col = types.DecodeDecimal64Slice(vec.Data)
		case types.T_decimal128:
			vec.Data = make([]byte, 16*batchSize)
			vec.Col = types.DecodeDecimal128Slice(vec.Data)
		case types.T_timestamp:
			vec.Data = make([]byte, 8*batchSize)
			vec.Col = types.DecodeTimestampSlice(vec.Data)
		default:
			panic("unsupported vector type")
		}
		batchData.Vecs[i] = vec
	}
	return batchData
}

func GetBatchData(param *ExternalParam, plh *ParseLineHandler, proc *process.Process) (*batch.Batch, error) {
	bat := makeBatch(param, plh)
	var Line []string
	for rowIdx := 0; rowIdx < plh.batchSize; rowIdx++ {
		Line = plh.simdCsvLineArray[rowIdx]
		if len(Line) < len(param.Attrs) {
			return nil, errors.New("the table column is larger than input data column")
		}
		for colIdx := range param.Attrs {
			field := Line[param.Name2ColIndex[param.Attrs[colIdx]]]
			if types.T(param.Cols[colIdx].Typ.Id) != types.T_char && types.T(param.Cols[colIdx].Typ.Id) != types.T_varchar {
				field = strings.TrimSpace(field)
			}
			vec := bat.Vecs[colIdx]
			isNullOrEmpty := field == NULL_FLAG
			if types.T(param.Cols[colIdx].Typ.Id) != types.T_char && types.T(param.Cols[colIdx].Typ.Id) != types.T_varchar {
				isNullOrEmpty = isNullOrEmpty || len(field) == 0
			}
			switch types.T(param.Cols[colIdx].Typ.Id) {
			case types.T_bool:
				cols := vec.Col.([]bool)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					if field == "true" || field == "1" {
						cols[rowIdx] = true
					} else if field == "false" || field == "0" {
						cols[rowIdx] = false
					} else {
						return nil, fmt.Errorf("the input value '%s' is not bool type for column %d", field, colIdx)
					}
				}
			case types.T_int8:
				cols := vec.Col.([]int8)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					if judgeInterge(field) {
						d, err := strconv.ParseInt(field, 10, 8)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not int8 type for column %d", field, colIdx)
						}
						cols[rowIdx] = int8(d)
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < math.MinInt8 || d > math.MaxInt8 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not int8 type for column %d", field, colIdx)
						}
						cols[rowIdx] = int8(d)
					}
				}
			case types.T_int16:
				cols := vec.Col.([]int16)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					if judgeInterge(field) {
						d, err := strconv.ParseInt(field, 10, 16)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not int16 type for column %d", field, colIdx)
						}
						cols[rowIdx] = int16(d)
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < math.MinInt16 || d > math.MaxInt16 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not int16 type for column %d", field, colIdx)
						}
						cols[rowIdx] = int16(d)
					}
				}
			case types.T_int32:
				cols := vec.Col.([]int32)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					if judgeInterge(field) {
						d, err := strconv.ParseInt(field, 10, 32)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not int32 type for column %d", field, colIdx)
						}
						cols[rowIdx] = int32(d)
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < math.MinInt32 || d > math.MaxInt32 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not int32 type for column %d", field, colIdx)
						}
						cols[rowIdx] = int32(d)
					}
				}
			case types.T_int64:
				cols := vec.Col.([]int64)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					if judgeInterge(field) {
						d, err := strconv.ParseInt(field, 10, 64)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not int64 type for column %d", field, colIdx)
						}
						cols[rowIdx] = d
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < math.MinInt64 || d > math.MaxInt64 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not int64 type for column %d", field, colIdx)
						}
						cols[rowIdx] = int64(d)
					}
				}
			case types.T_uint8:
				cols := vec.Col.([]uint8)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					if judgeInterge(field) {
						d, err := strconv.ParseUint(field, 10, 8)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not uint8 type for column %d", field, colIdx)
						}
						cols[rowIdx] = uint8(d)
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < 0 || d > math.MaxUint8 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not uint8 type for column %d", field, colIdx)
						}
						cols[rowIdx] = uint8(d)
					}
				}
			case types.T_uint16:
				cols := vec.Col.([]uint16)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					if judgeInterge(field) {
						d, err := strconv.ParseUint(field, 10, 16)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not uint16 type for column %d", field, colIdx)
						}
						cols[rowIdx] = uint16(d)
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < 0 || d > math.MaxUint16 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not uint16 type for column %d", field, colIdx)
						}
						cols[rowIdx] = uint16(d)
					}
				}
			case types.T_uint32:
				cols := vec.Col.([]uint32)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					if judgeInterge(field) {
						d, err := strconv.ParseUint(field, 10, 32)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not uint32 type for column %d", field, colIdx)
						}
						cols[rowIdx] = uint32(d)
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < 0 || d > math.MaxUint32 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not uint32 type for column %d", field, colIdx)
						}
						cols[rowIdx] = uint32(d)
					}
				}
			case types.T_uint64:
				cols := vec.Col.([]uint64)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					if judgeInterge(field) {
						d, err := strconv.ParseUint(field, 10, 64)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not uint64 type for column %d", field, colIdx)
						}
						cols[rowIdx] = d
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < 0 || d > math.MaxUint64 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not uint64 type for column %d", field, colIdx)
						}
						cols[rowIdx] = uint64(d)
					}
				}
			case types.T_float32:
				cols := vec.Col.([]float32)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseFloat(field, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not float32 type for column %d", field, colIdx)
					}
					cols[rowIdx] = float32(d)
				}
			case types.T_float64:
				cols := vec.Col.([]float64)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseFloat(field, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not float64 type for column %d", field, colIdx)
					}
					cols[rowIdx] = d
				}
			case types.T_char, types.T_varchar:
				vBytes := vec.Col.(*types.Bytes)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
					vBytes.Data = append(vBytes.Data, field...)
					vBytes.Lengths[rowIdx] = uint32(len(field))
				}
			case types.T_json:
				vBytes := vec.Col.(*types.Bytes)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
					vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
					vBytes.Lengths[rowIdx] = uint32(len(field))
				} else {
					vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
					byteJson, err := types.ParseStringToByteJson(field)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not json type for column %d", field, colIdx)
					}
					jsonBytes, err := types.EncodeJson(byteJson)
					if err != nil {
						logutil.Errorf("encode json[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not json type for column %d", field, colIdx)
					}
					vBytes.Data = append(vBytes.Data, jsonBytes...)
					vBytes.Lengths[rowIdx] = uint32(len(jsonBytes))
				}
			case types.T_date:
				cols := vec.Col.([]types.Date)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := types.ParseDate(field)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not Date type for column %d", field, colIdx)
					}
					cols[rowIdx] = d
				}
			case types.T_datetime:
				cols := vec.Col.([]types.Datetime)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := types.ParseDatetime(field, vec.Typ.Precision)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not Datetime type for column %d", field, colIdx)
					}
					cols[rowIdx] = d
				}
			case types.T_decimal64:
				cols := vec.Col.([]types.Decimal64)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := types.Decimal64_FromString(field)
					if err != nil {
						// we tolerate loss of digits.
						if !moerr.IsMoErrCode(err, moerr.DATA_TRUNCATED) {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not Decimal64 type for column %d", field, colIdx)
						}
					}
					cols[rowIdx] = d
				}
			case types.T_decimal128:
				cols := vec.Col.([]types.Decimal128)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := types.Decimal128_FromString(field)
					if err != nil {
						// we tolerate loss of digits.
						if !moerr.IsMoErrCode(err, moerr.DATA_TRUNCATED) {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not Decimal128 type for column %d", field, colIdx)
						}
					}
					cols[rowIdx] = d
				}
			case types.T_timestamp:
				cols := vec.Col.([]types.Timestamp)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := types.ParseTimestamp(time.UTC, field, vec.Typ.Precision)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not Timestamp type for column %d", field, colIdx)
					}
					cols[rowIdx] = d
				}
			default:
				return nil, fmt.Errorf("the value type %d is not support now", param.Cols[rowIdx].Typ.Id)
			}
		}
	}
	n := vector.Length(bat.Vecs[0])
	sels := proc.Mp.GetSels()
	if n > cap(sels) {
		proc.Mp.PutSels(sels)
		sels = make([]int64, n)
	}
	bat.Zs = sels[:n]
	for k := 0; k < n; k++ {
		bat.Zs[k] = 1
	}

	return bat, nil
}

// get file reader from external file
func GetSimdcsvReader(param *ExternalParam) (*ParseLineHandler, error) {
	var err error
	param.reader, err = getFileDataReader(param.extern)
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
	plh.simdCsvReader = simdcsv.NewReaderWithOptions(param.reader,
		rune(param.extern.Tail.Fields.Terminated[0]),
		'#',
		true,
		true)

	return plh, nil
}

// read batch data from external file
func ScanFileData(param *ExternalParam, proc *process.Process) (*batch.Batch, error) {
	var bat *batch.Batch
	var err error
	if param.plh == nil {
		param.plh, err = GetSimdcsvReader(param)
		if err != nil {
			return nil, err
		}
	}
	plh := param.plh
	plh.simdCsvLineArray, err = plh.simdCsvReader.Read(param.batchSize, param.Ctx)
	if err != nil {
		return nil, err
	}
	if len(plh.simdCsvLineArray) < param.batchSize {
		err := param.reader.Close()
		if err != nil {
			logutil.Errorf("close file failed. err:%v", err)
		}
		param.plh = nil
		param.FileIndex++
		param.IgnoreLine = param.IgnoreLineTag
		if param.FileIndex >= param.FileCnt {
			param.End = true
		}
	}
	if param.IgnoreLine != 0 {
		plh.simdCsvLineArray = plh.simdCsvLineArray[param.IgnoreLine:]
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

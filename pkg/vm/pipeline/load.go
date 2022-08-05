// Copyright 2021 Matrix Origin
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

package pipeline

import (
	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/matrixorigin/simdcsv"
	"github.com/pierrec/lz4"
)

func ReadFromS3(loadParam *tree.LoadParam) ([]string, error) {
	var config fileservice.S3Config
	config.Bucket = loadParam.Config.Bucket
	config.Endpoint = loadParam.Config.Endpoint

	os.Setenv("AWS_REGION", loadParam.Region)
	os.Setenv("AWS_ACCESS_KEY_ID", loadParam.APIKey)
	os.Setenv("AWS_SECRET_ACCESS_KEY", loadParam.APISecret)

	fs, err := fileservice.NewS3FS(
		config.Endpoint,
		config.Bucket,
		config.KeyPrefix,
		1,
	)
	if err != nil {
		return nil, err
	}

	index := strings.LastIndex(loadParam.Filepath, "/")
	dir, file  := "", loadParam.Filepath
	if index != -1 {
		dir = string([]byte(loadParam.Filepath)[0:index])
		file = string([]byte(loadParam.Filepath)[index+1:])
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

func ReadFromS3File(loadParam *tree.LoadParam) (io.ReadCloser, error) {
	var config fileservice.S3Config
	config.Bucket = loadParam.Config.Bucket
	config.Endpoint = loadParam.Config.Endpoint

	os.Setenv("AWS_REGION", loadParam.Region)
	os.Setenv("AWS_ACCESS_KEY_ID", loadParam.APIKey)
	os.Setenv("AWS_SECRET_ACCESS_KEY", loadParam.APISecret)

	fs, err := fileservice.NewS3FS(
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
		FilePath: loadParam.Filepath,
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

func ReadFromLocal(loadParam *tree.LoadParam) ([]string, error) {
	index := strings.LastIndex(loadParam.Filepath, "/")
	dir, file  := "", loadParam.Filepath
	if index != -1 {
		dir = string([]byte(loadParam.Filepath)[0:index])
		file = string([]byte(loadParam.Filepath)[index+1:])
	}

	fs, err := fileservice.NewLocalETLFS(dir)
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

func ReadFromLocalFile(loadParam *tree.LoadParam) (io.ReadCloser, error) {
	var r io.ReadCloser
	index := strings.LastIndex(loadParam.Filepath, "/")
	dir, file  := "", loadParam.Filepath
	if index != -1 {
		dir = string([]byte(loadParam.Filepath)[0:index])
		file = string([]byte(loadParam.Filepath)[index+1:])
	}

	fs, err := fileservice.NewLocalETLFS(dir)
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

func InitS3Param(loadParam *tree.LoadParam) error {
	for i := 0; i < len(loadParam.S3options); i += 2 {
		switch strings.ToLower(loadParam.S3options[i]) {
		case "endpoint":
			loadParam.Config.Endpoint = loadParam.S3options[i+1]
		case "region":
			loadParam.Region = loadParam.S3options[i+1]
		case "access_key_id":
			loadParam.APIKey = loadParam.S3options[i+1]
		case "secret_access_key":
			loadParam.APISecret = loadParam.S3options[i+1]
		case "bucket":
			loadParam.Config.Bucket = loadParam.S3options[i+1]
		case "filepath":
			loadParam.Filepath = loadParam.S3options[i+1]
		case "compression":
			loadParam.CompressType = loadParam.S3options[i+1]
		default:
			return fmt.Errorf("the keyword '%s' is not support", strings.ToLower(loadParam.S3options[i]))
		}
	}
	return nil
}

func getLoadDataList(loadParam *tree.LoadParam) ([]string, error) {
	switch loadParam.LoadType {
	case tree.LOCAL:
		dataFile, err := ReadFromLocal(loadParam)
		if err != nil {
			return nil, err
		}
		return dataFile, nil
	case tree.S3, tree.MinIO:
		err := InitS3Param(loadParam)
		if err != nil {
			return nil, err
		}
		dataFile, err := ReadFromS3(loadParam)
		if err != nil {
			return nil, err
		}
		return dataFile, nil
	default:
		return nil, errors.New("the load file type is not support now")
	}
}

func getLoadDataReader(loadParam *tree.LoadParam) (io.ReadCloser, error) {
	switch loadParam.LoadType {
	case tree.LOCAL:
		dataFile, err := ReadFromLocalFile(loadParam)
		if err != nil {
			return nil, err
		}
		return dataFile, nil
	case tree.S3, tree.MinIO:
		dataFile, err := ReadFromS3File(loadParam)
		if err != nil {
			return nil, err
		}
		return dataFile, nil
	default:
		return nil, errors.New("the load file type is not support now")
	}
}

func getCompressType(loadParam *tree.LoadParam) string {
	if loadParam.CompressType != "" && loadParam.CompressType != tree.AUTO {
		return loadParam.CompressType
	}
	index := strings.LastIndex(loadParam.Filepath, ".")
	if index == -1 {
		return tree.NOCOMPRESS
	}
	tail := string([]byte(loadParam.Filepath)[index+1:])
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

func getUnCompressReader(loadParam *tree.LoadParam, r io.ReadCloser) (io.ReadCloser, error) {
	switch strings.ToLower(getCompressType(loadParam)) {
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
		return nil, fmt.Errorf("the compress type '%s' is not support now", loadParam.CompressType)
	default:
		return nil, fmt.Errorf("the compress type '%s' is not support now", loadParam.CompressType)
	}
}

const NULL_FLAG = "\\N"

func getLineOutChan(v atomic.Value) chan simdcsv.LineOut {
	return v.Load().(chan simdcsv.LineOut)
}

func judgeInterge(field string) bool {
	for i := 0; i < len(field); i++ {
		if field[i] > '9' || field[i] < '0' {
			return false
		}
	}
	return true
}

func makeBatch(p *Pipeline, plh *ParseLineHandler) *batch.Batch {
	batchData := batch.New(true, p.attrs)
	batchSize := plh.batchSize
	//alloc space for vector
	for i := 0; i < len(p.attrs); i++ {
		typ := types.New(types.T(p.cols[i].Typ.Id), p.cols[i].Typ.Width, p.cols[i].Typ.Scale, p.cols[i].Typ.Precision)
		vec := vector.New(typ)
		switch vec.Typ.Oid {
		case types.T_bool:
			vec.Col = make([]bool, batchSize)
			vec.Data = make([]byte, batchSize)
		case types.T_int8:
			vec.Col = make([]int8, batchSize)
			vec.Data = make([]byte, batchSize)
		case types.T_int16:
			vec.Col = make([]int16, batchSize)
			vec.Data = make([]byte, 2*batchSize)
		case types.T_int32:
			vec.Col = make([]int32, batchSize)
			vec.Data = make([]byte, 4*batchSize)
		case types.T_int64:
			vec.Col = make([]int64, batchSize)
			vec.Data = make([]byte, 8*batchSize)
		case types.T_uint8:
			vec.Col = make([]uint8, batchSize)
			vec.Data = make([]byte, batchSize)
		case types.T_uint16:
			vec.Col = make([]uint16, batchSize)
			vec.Data = make([]byte, 2*batchSize)
		case types.T_uint32:
			vec.Col = make([]uint32, batchSize)
			vec.Data = make([]byte, 4*batchSize)
		case types.T_uint64:
			vec.Col = make([]uint64, batchSize)
			vec.Data = make([]byte, 8*batchSize)
		case types.T_float32:
			vec.Col = make([]float32, batchSize)
			vec.Data = make([]byte, 4*batchSize)
		case types.T_float64:
			vec.Col = make([]float64, batchSize)
			vec.Data = make([]byte, 8*batchSize)
		case types.T_char, types.T_varchar, types.T_json:
			vBytes := &types.Bytes{
				Offsets: make([]uint32, batchSize),
				Lengths: make([]uint32, batchSize),
				Data:    nil,
			}
			vec.Col = vBytes
			vec.Data = make([]byte, batchSize)
		case types.T_date:
			vec.Col = make([]types.Date, batchSize)
			vec.Data = make([]byte, 4*batchSize)
		case types.T_datetime:
			vec.Col = make([]types.Datetime, batchSize)
			vec.Data = make([]byte, 8*batchSize)
		case types.T_decimal64:
			vec.Col = make([]types.Decimal64, batchSize)
			vec.Data = make([]byte, 8*batchSize)
		case types.T_decimal128:
			vec.Col = make([]types.Decimal128, batchSize)
			vec.Data = make([]byte, 16*batchSize)
		case types.T_timestamp:
			vec.Col = make([]types.Timestamp, batchSize)
			vec.Data = make([]byte, 8*batchSize)
		default:
			panic("unsupported vector type")
		}
		batchData.Vecs[i] = vec
	}
	return batchData
}

func GetBatchData(p *Pipeline, plh *ParseLineHandler, proc *process.Process) (*batch.Batch, error) {
	bat := makeBatch(p, plh)

	var Line []string
	for rowIdx := 0; rowIdx < plh.batchSize; rowIdx++ {
		Line = plh.simdCsvLineArray[rowIdx]
		if len(Line) < len(p.attrs) {
			return nil, errors.New("the table colnum is larger than input data colnum")
		}
		for colIdx := range p.attrs {
			field := strings.TrimSpace(Line[p.Name2ColIndex[p.attrs[colIdx]]])
			vec := bat.Vecs[colIdx]
			isNullOrEmpty := len(field) == 0 || field == NULL_FLAG
			switch types.T(p.cols[colIdx].Typ.Id) {
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
						return nil, fmt.Errorf("the input value '%s' is not bool type for colnum %d", field, colIdx)
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
							return nil, fmt.Errorf("the input value '%v' is not int8 type for colnum %d", field, colIdx)
						}
						cols[rowIdx] = int8(d)
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < math.MinInt8 || d > math.MaxInt8 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not int8 type for colnum %d", field, colIdx)
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
							return nil, fmt.Errorf("the input value '%v' is not int16 type for colnum %d", field, colIdx)
						}
						cols[rowIdx] = int16(d)
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < math.MinInt16 || d > math.MaxInt16 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not int16 type for colnum %d", field, colIdx)
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
							return nil, fmt.Errorf("the input value '%v' is not int32 type for colnum %d", field, colIdx)
						}
						cols[rowIdx] = int32(d)
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < math.MinInt32 || d > math.MaxInt32 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not int32 type for colnum %d", field, colIdx)
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
							return nil, fmt.Errorf("the input value '%v' is not int64 type for colnum %d", field, colIdx)
						}
						cols[rowIdx] = d
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < math.MinInt64 || d > math.MaxInt64 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not int64 type for colnum %d", field, colIdx)
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
							return nil, fmt.Errorf("the input value '%v' is not uint8 type for colnum %d", field, colIdx)
						}
						cols[rowIdx] = uint8(d)
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < 0 || d > math.MaxUint8 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not uint8 type for colnum %d", field, colIdx)
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
							return nil, fmt.Errorf("the input value '%v' is not uint16 type for colnum %d", field, colIdx)
						}
						cols[rowIdx] = uint16(d)
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < 0 || d > math.MaxUint16 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not uint16 type for colnum %d", field, colIdx)
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
							return nil, fmt.Errorf("the input value '%v' is not uint32 type for colnum %d", field, colIdx)
						}
						cols[rowIdx] = uint32(d)
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < 0 || d > math.MaxUint32 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not uint32 type for colnum %d", field, colIdx)
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
							return nil, fmt.Errorf("the input value '%v' is not uint64 type for colnum %d", field, colIdx)
						}
						cols[rowIdx] = d
					} else {
						d, err := strconv.ParseFloat(field, 64)
						if err != nil || d < 0 || d > math.MaxUint64 {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							return nil, fmt.Errorf("the input value '%v' is not uint64 type for colnum %d", field, colIdx)
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
						return nil, fmt.Errorf("the input value '%v' is not float32 type for colnum %d", field, colIdx)
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
						return nil, fmt.Errorf("the input value '%v' is not float64 type for colnum %d", field, colIdx)
					}
					cols[rowIdx] = d
				}
			case types.T_char, types.T_varchar, types.T_json:
				vBytes := vec.Col.(*types.Bytes)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
					vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
					vBytes.Lengths[rowIdx] = uint32(len(field))
				} else {
					vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
					vBytes.Data = append(vBytes.Data, field...)
					vBytes.Lengths[rowIdx] = uint32(len(field))
				}
			case types.T_date:
				cols := vec.Col.([]types.Date)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := types.ParseDate(field)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not Date type for colnum %d", field, colIdx)
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
						return nil, fmt.Errorf("the input value '%v' is not Datetime type for colnum %d", field, colIdx)
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
							return nil, fmt.Errorf("the input value '%v' is not Decimal64 type for colnum %d", field, colIdx)
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
							return nil, fmt.Errorf("the input value '%v' is not Decimal128 type for colnum %d", field, colIdx)
						}
					}
					cols[rowIdx] = d
				}
			case types.T_timestamp:
				cols := vec.Col.([]types.Timestamp)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := types.ParseTimestamp(field, vec.Typ.Precision)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not Timestamp type for colnum %d", field, colIdx)
					}
					cols[rowIdx] = d
				}
			default:
				return nil, fmt.Errorf("the value type %d is not support now", p.cols[rowIdx].Typ.Id)
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

func Run3(load *tree.LoadParam, p *Pipeline, proc *process.Process) (bool, error) {
	var bat *batch.Batch
	var end bool // exist flag
	var dataFile io.ReadCloser
	dataFile, err := getLoadDataReader(load)
	if err != nil {
		logutil.Errorf("open file failed. err:%v", err)
		return false, err
	}
	dataFile, err = getUnCompressReader(load, dataFile)
	if err != nil {
		return false, err
	}

	defer func() {
		err := dataFile.Close()
		if err != nil {
			logutil.Errorf("close file failed. err:%v", err)
		}
	}()

	channelSize := 100
	plh := &ParseLineHandler{}
	plh.batchSize = 40000
	plh.simdCsvLineArray = make([][]string, plh.batchSize)
	plh.lineIdx = 0
	plh.simdCsvGetParsedLinesChan = atomic.Value{}
	plh.simdCsvGetParsedLinesChan.Store(make(chan simdcsv.LineOut, channelSize))
	if load.Fields == nil {
		load.Fields = &tree.Fields{Terminated: ","}
	}
	plh.simdCsvReader = simdcsv.NewReaderWithOptions(dataFile,
		rune(load.Fields.Terminated[0]),
		'#',
		false,
		false)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := plh.simdCsvReader.ReadLoop(getLineOutChan(plh.simdCsvGetParsedLinesChan))
		if err != nil {
			close(getLineOutChan(plh.simdCsvGetParsedLinesChan))
		}
	}()

	ignore := int(load.IgnoredLines)
	var lineOut simdcsv.LineOut
	for {
		lineOut = <-getLineOutChan(plh.simdCsvGetParsedLinesChan)
		if lineOut.Line == nil && lineOut.Lines == nil {
			bat = nil
		} else {
			if plh.lineIdx < ignore {
				ignore--
				continue
			}
			plh.simdCsvLineArray[plh.lineIdx] = lineOut.Line
			plh.lineIdx++
			if plh.lineIdx == plh.batchSize {
				bat, err = GetBatchData(p, plh, proc)
				if err != nil {
					return false, err
				}
				bat.Cnt = 1
				plh.lineIdx = 0
			} else {
				continue
			}
		}
		if bat == nil && plh.lineIdx != 0 {
			plh.batchSize = plh.lineIdx
			bat, err = GetBatchData(p, plh, proc)
			if err != nil {
				return false, err
			}
			bat.Cnt = 1
			plh.lineIdx = 0
		}

		// processing the batch according to the instructions
		proc.Reg.InputBatch = bat
		if end, err = vm.Run(p.instructions, proc); err != nil || end { // end is true means pipeline successfully completed
			break
		}
		if lineOut.Line == nil && lineOut.Lines == nil {
			break
		}
	}
	wg.Wait()
	return true, nil
}

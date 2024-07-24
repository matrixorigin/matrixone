// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	util2 "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type ExportConfig struct {
	// configs from user input
	userConfig *tree.ExportParam
	// file handler
	File *os.File
	// bufio.writer
	Writer *bufio.Writer
	// curFileSize
	CurFileSize uint64
	Rows        uint64
	FileCnt     uint
	ColumnFlag  []bool
	Symbol      [][]byte
	// default flush size
	DefaultBufSize int64
	OutputStr      []byte
	LineSize       uint64

	//file service & buffer for the line
	UseFileService bool
	writeParam
	FileService fileservice.FileService
	LineBuffer  *bytes.Buffer
	Ctx         context.Context
	AsyncReader *io.PipeReader
	AsyncWriter *io.PipeWriter
	AsyncGroup  *errgroup.Group
	mrs         *MysqlResultSet
	lineStr     []byte
	ctx         context.Context
}

type writeParam struct {
	First      bool
	OutTofile  bool
	Index      atomic.Int32
	WriteIndex atomic.Int32
	ByteChan   chan *BatchByte
	BatchMap   map[int32][]byte
}

type BatchByte struct {
	index     int32
	writeByte []byte
	err       error
}

var OpenFile = os.OpenFile
var escape byte = '"'

type CloseExportData struct {
	stopExportData chan interface{}
	onceClose      sync.Once
}

func NewCloseExportData() *CloseExportData {
	return &CloseExportData{
		stopExportData: make(chan interface{}),
	}
}

func (cld *CloseExportData) Open() {
}

func (cld *CloseExportData) Close() {
	cld.onceClose.Do(func() {
		close(cld.stopExportData)
	})
}

// needExportToFile checks needing to export into file or not
func (ec *ExportConfig) needExportToFile() bool {
	return ec != nil && ec.userConfig != nil && ec.userConfig.Outfile
}

func initExportFileParam(ep *ExportConfig, mrs *MysqlResultSet) {
	ep.DefaultBufSize *= 1024 * 1024
	n := (int)(mrs.GetColumnCount())
	if n <= 0 {
		return
	}
	ep.Symbol = make([][]byte, n)
	for i := 0; i < n-1; i++ {
		ep.Symbol[i] = []byte(ep.userConfig.Fields.Terminated.Value)
	}
	ep.Symbol[n-1] = []byte(ep.userConfig.Lines.TerminatedBy.Value)
	ep.ColumnFlag = make([]bool, len(mrs.Name2Index))
	for i := 0; i < len(ep.userConfig.ForceQuote); i++ {
		col, ok := mrs.Name2Index[ep.userConfig.ForceQuote[i]]
		if ok {
			ep.ColumnFlag[col] = true
		}
	}
}

var openNewFile = func(ctx context.Context, ep *ExportConfig, mrs *MysqlResultSet) error {
	lineSize := ep.LineSize
	var err error
	ep.CurFileSize = 0
	if !ep.UseFileService {
		var filePath string
		if len(ep.userConfig.StageFilePath) != 0 {
			filePath = getExportFilePath(ep.userConfig.StageFilePath, ep.FileCnt)
		} else {
			filePath = getExportFilePath(ep.userConfig.FilePath, ep.FileCnt)
		}
		ep.File, err = OpenFile(filePath, os.O_RDWR|os.O_EXCL|os.O_CREATE, 0o666)
		if err != nil {
			return err
		}
		ep.Writer = bufio.NewWriterSize(ep.File, int(ep.DefaultBufSize))
	} else {
		//default 1MB
		if ep.LineBuffer == nil {
			ep.LineBuffer = &bytes.Buffer{}
		} else {
			ep.LineBuffer.Reset()
		}
		ep.AsyncReader, ep.AsyncWriter = io.Pipe()
		filePath := getExportFilePath(ep.userConfig.FilePath, ep.FileCnt)

		asyncWriteFunc := func() error {
			vec := fileservice.IOVector{
				FilePath: filePath,
				Entries: []fileservice.IOEntry{
					{
						ReaderForWrite: ep.AsyncReader,
						Size:           -1,
					},
				},
			}
			err := ep.FileService.Write(ctx, vec)
			if err != nil {
				err2 := ep.AsyncReader.CloseWithError(err)
				if err2 != nil {
					return err2
				}
			}
			return err
		}

		ep.AsyncGroup, _ = errgroup.WithContext(ctx)
		ep.AsyncGroup.Go(asyncWriteFunc)
	}
	if ep.userConfig.Header {
		var header string
		n := len(mrs.Columns)
		if n == 0 {
			return nil
		}
		for i := 0; i < n-1; i++ {
			header += mrs.Columns[i].Name() + ep.userConfig.Fields.Terminated.Value
		}
		header += mrs.Columns[n-1].Name() + ep.userConfig.Lines.TerminatedBy.Value
		if ep.userConfig.MaxFileSize != 0 && uint64(len(header)) >= ep.userConfig.MaxFileSize {
			return moerr.NewInternalError(ctx, "the header line size is over the maxFileSize")
		}
		if err := writeDataToCSVFile(ep, []byte(header)); err != nil {
			return err
		}
		if _, err := EndOfLine(ep); err != nil {
			return err
		}
	}
	if lineSize != 0 {
		ep.LineSize = 0
		ep.Rows = 0
		if err := writeDataToCSVFile(ep, ep.OutputStr); err != nil {
			return err
		}
	}
	return nil
}

func getExportFilePath(filename string, fileCnt uint) string {
	if fileCnt == 0 {
		return filename
	} else {
		return fmt.Sprintf("%s.%d", filename, fileCnt)
	}
}

var formatOutputString = func(oq *ExportConfig, tmp, symbol []byte, enclosed byte, flag bool) error {
	var err error
	if flag && enclosed != 0 {
		if err = writeToCSVFile(oq, []byte{enclosed}); err != nil {
			return err
		}
	}
	if err = writeToCSVFile(oq, tmp); err != nil {
		return err
	}
	if flag && enclosed != 0 {
		if err = writeToCSVFile(oq, []byte{enclosed}); err != nil {
			return err
		}
	}
	if err = writeToCSVFile(oq, symbol); err != nil {
		return err
	}
	return nil
}

var Flush = func(ep *ExportConfig) error {
	if !ep.UseFileService {
		return ep.Writer.Flush()
	}
	return nil
}

var Seek = func(ep *ExportConfig) (int64, error) {
	if !ep.UseFileService {
		return ep.File.Seek(int64(ep.CurFileSize-ep.LineSize), io.SeekStart)
	}
	return 0, nil
}

var Read = func(ep *ExportConfig) (int, error) {
	if !ep.UseFileService {
		ep.OutputStr = make([]byte, ep.LineSize)
		return ep.File.Read(ep.OutputStr)
	} else {
		ep.OutputStr = make([]byte, ep.LineSize)
		copy(ep.OutputStr, ep.LineBuffer.Bytes())
		ep.LineBuffer.Reset()
		return int(ep.LineSize), nil
	}
}

var Truncate = func(ep *ExportConfig) error {
	if !ep.UseFileService {
		return ep.File.Truncate(int64(ep.CurFileSize - ep.LineSize))
	} else {
		return nil
	}
}

var Close = func(ep *ExportConfig) error {
	if !ep.UseFileService {
		ep.FileCnt++
		return ep.File.Close()
	} else {
		ep.FileCnt++
		err := ep.AsyncWriter.Close()
		if err != nil {
			return err
		}
		err = ep.AsyncGroup.Wait()
		if err != nil {
			return err
		}
		err = ep.AsyncReader.Close()
		if err != nil {
			return err
		}
		ep.AsyncReader = nil
		ep.AsyncWriter = nil
		ep.AsyncGroup = nil
		return err
	}
}

var Write = func(ep *ExportConfig, output []byte) (int, error) {
	if !ep.UseFileService {
		return ep.Writer.Write(output)
	} else {
		return ep.LineBuffer.Write(output)
	}
}

var EndOfLine = func(ep *ExportConfig) (int, error) {
	if ep.UseFileService {
		n, err := ep.AsyncWriter.Write(ep.LineBuffer.Bytes())
		if err != nil {
			err2 := ep.AsyncWriter.CloseWithError(err)
			if err2 != nil {
				return 0, err2
			}
		}
		ep.LineBuffer.Reset()
		return n, err
	}
	return 0, nil
}

func writeToCSVFile(ep *ExportConfig, output []byte) error {
	if ep.userConfig.MaxFileSize != 0 && ep.CurFileSize+uint64(len(output)) > ep.userConfig.MaxFileSize {
		if err := Flush(ep); err != nil {
			return err
		}
		if ep.LineSize != 0 && ep.OutTofile {
			if _, err := Seek(ep); err != nil {
				return err
			}
			for {
				if n, err := Read(ep); err != nil {
					return err
				} else if uint64(n) == ep.LineSize {
					break
				}
			}
			if err := Truncate(ep); err != nil {
				return err
			}
		}
		if err := Close(ep); err != nil {
			return err
		}
		if err := openNewFile(ep.ctx, ep, ep.mrs); err != nil {
			return err
		}
	}

	if err := writeDataToCSVFile(ep, output); err != nil {
		return err
	}
	return nil
}

var writeDataToCSVFile = func(ep *ExportConfig, output []byte) error {
	for {
		if n, err := Write(ep, output); err != nil {
			return err
		} else if n == len(output) {
			break
		}
	}
	ep.LineSize += uint64(len(output))
	ep.CurFileSize += uint64(len(output))
	return nil
}

func appendBytes(writeByte, tmp, symbol []byte, enclosed byte, flag bool) []byte {
	if flag && enclosed != 0 {
		writeByte = append(writeByte, enclosed)
	}
	writeByte = append(writeByte, tmp...)
	if flag && enclosed != 0 {
		writeByte = append(writeByte, enclosed)
	}
	writeByte = append(writeByte, symbol...)
	return writeByte
}

func formatJsonString(str string, flag bool, terminatedBy string) string {
	if len(str) < 2 {
		return "\"" + str + "\""
	}
	var tmp string
	if !flag {
		tmp = strings.ReplaceAll(str, terminatedBy, "\\"+terminatedBy)
	} else {
		tmp = strings.ReplaceAll(str, "\",", "\"\",")
	}
	return tmp
}

func constructByte(ctx context.Context, obj FeSession, bat *batch.Batch, index int32, ByteChan chan *BatchByte, ep *ExportConfig) {
	ses := obj.(*Session)
	symbol := ep.Symbol
	closeby := ep.userConfig.Fields.EnclosedBy.Value
	terminated := ep.userConfig.Fields.Terminated.Value
	flag := ep.ColumnFlag
	writeByte := make([]byte, 0)
	for i := 0; i < bat.RowCount(); i++ {
		for j, vec := range bat.Vecs {
			if vec.GetNulls().Contains(uint64(i)) {
				writeByte = appendBytes(writeByte, []byte("\\N"), symbol[j], closeby, flag[j])
				continue
			}
			switch vec.GetType().Oid { //get col
			case types.T_json:
				val := types.DecodeJson(vec.GetBytesAt(i))
				writeByte = appendBytes(writeByte, []byte(formatJsonString(val.String(), flag[j], terminated)), symbol[j], closeby, flag[j])
			case types.T_bool:
				val := vector.GetFixedAt[bool](vec, i)
				if val {
					writeByte = appendBytes(writeByte, []byte("true"), symbol[j], closeby, flag[j])
				} else {
					writeByte = appendBytes(writeByte, []byte("false"), symbol[j], closeby, flag[j])
				}
			case types.T_bit:
				val := vector.GetFixedAt[uint64](vec, i)
				bitLength := vec.GetType().Width
				byteLength := (bitLength + 7) / 8
				b := types.EncodeUint64(&val)[:byteLength]
				slices.Reverse(b)
				writeByte = appendBytes(writeByte, b, symbol[j], closeby, flag[j])
			case types.T_int8:
				val := vector.GetFixedAt[int8](vec, i)
				writeByte = appendBytes(writeByte, []byte(strconv.FormatInt(int64(val), 10)), symbol[j], closeby, flag[j])
			case types.T_int16:
				val := vector.GetFixedAt[int16](vec, i)
				writeByte = appendBytes(writeByte, []byte(strconv.FormatInt(int64(val), 10)), symbol[j], closeby, flag[j])
			case types.T_int32:
				val := vector.GetFixedAt[int32](vec, i)
				writeByte = appendBytes(writeByte, []byte(strconv.FormatInt(int64(val), 10)), symbol[j], closeby, flag[j])
			case types.T_int64:
				val := vector.GetFixedAt[int64](vec, i)
				writeByte = appendBytes(writeByte, []byte(strconv.FormatInt(int64(val), 10)), symbol[j], closeby, flag[j])
			case types.T_uint8:
				val := vector.GetFixedAt[uint8](vec, i)
				writeByte = appendBytes(writeByte, []byte(strconv.FormatUint(uint64(val), 10)), symbol[j], closeby, flag[j])
			case types.T_uint16:
				val := vector.GetFixedAt[uint16](vec, i)
				writeByte = appendBytes(writeByte, []byte(strconv.FormatUint(uint64(val), 10)), symbol[j], closeby, flag[j])
			case types.T_uint32:
				val := vector.GetFixedAt[uint32](vec, i)
				writeByte = appendBytes(writeByte, []byte(strconv.FormatUint(uint64(val), 10)), symbol[j], closeby, flag[j])
			case types.T_uint64:
				val := vector.GetFixedAt[uint64](vec, i)
				writeByte = appendBytes(writeByte, []byte(strconv.FormatUint(uint64(val), 10)), symbol[j], closeby, flag[j])
			case types.T_float32:
				val := vector.GetFixedAt[float32](vec, i)
				if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
					writeByte = appendBytes(writeByte, []byte(strconv.FormatFloat(float64(val), 'f', -1, 32)), symbol[j], closeby, flag[j])
				} else {
					writeByte = appendBytes(writeByte, []byte(strconv.FormatFloat(float64(val), 'f', int(vec.GetType().Scale), 64)), symbol[j], closeby, flag[j])
				}
			case types.T_float64:
				val := vector.GetFixedAt[float64](vec, i)
				if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
					writeByte = appendBytes(writeByte, []byte(strconv.FormatFloat(float64(val), 'f', -1, 32)), symbol[j], closeby, flag[j])
				} else {
					writeByte = appendBytes(writeByte, []byte(strconv.FormatFloat(float64(val), 'f', int(vec.GetType().Scale), 64)), symbol[j], closeby, flag[j])
				}
			case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
				value := addEscapeToString(vec.GetBytesAt(i))
				writeByte = appendBytes(writeByte, value, symbol[j], closeby, true)
			case types.T_array_float32:
				arrStr := types.BytesToArrayToString[float32](vec.GetBytesAt(i))
				value := addEscapeToString(util2.UnsafeStringToBytes(arrStr))
				writeByte = appendBytes(writeByte, value, symbol[j], closeby, true)
			case types.T_array_float64:
				arrStr := types.BytesToArrayToString[float64](vec.GetBytesAt(i))
				value := addEscapeToString(util2.UnsafeStringToBytes(arrStr))
				writeByte = appendBytes(writeByte, value, symbol[j], closeby, true)
			case types.T_date:
				val := vector.GetFixedAt[types.Date](vec, i)
				writeByte = appendBytes(writeByte, []byte(val.String()), symbol[j], closeby, flag[j])
			case types.T_datetime:
				scale := vec.GetType().Scale
				val := vector.GetFixedAt[types.Datetime](vec, i).String2(scale)
				writeByte = appendBytes(writeByte, []byte(val), symbol[j], closeby, flag[j])
			case types.T_time:
				scale := vec.GetType().Scale
				val := vector.GetFixedAt[types.Time](vec, i).String2(scale)
				writeByte = appendBytes(writeByte, []byte(val), symbol[j], closeby, flag[j])
			case types.T_timestamp:
				scale := vec.GetType().Scale
				timeZone := ses.GetTimeZone()
				val := vector.GetFixedAt[types.Timestamp](vec, i).String2(timeZone, scale)
				writeByte = appendBytes(writeByte, []byte(val), symbol[j], closeby, flag[j])
			case types.T_decimal64:
				scale := vec.GetType().Scale
				val := vector.GetFixedAt[types.Decimal64](vec, i).Format(scale)
				writeByte = appendBytes(writeByte, []byte(val), symbol[j], closeby, flag[j])
			case types.T_decimal128:
				scale := vec.GetType().Scale
				val := vector.GetFixedAt[types.Decimal128](vec, i).Format(scale)
				writeByte = appendBytes(writeByte, []byte(val), symbol[j], closeby, flag[j])
			case types.T_uuid:
				val := vector.GetFixedAt[types.Uuid](vec, i).String()
				writeByte = appendBytes(writeByte, []byte(val), symbol[j], closeby, flag[j])
			case types.T_Rowid:
				val := vector.GetFixedAt[types.Rowid](vec, i)
				writeByte = appendBytes(writeByte, []byte(val.String()), symbol[j], closeby, flag[j])
			case types.T_Blockid:
				val := vector.GetFixedAt[types.Blockid](vec, i)
				writeByte = appendBytes(writeByte, []byte(val.String()), symbol[j], closeby, flag[j])
			case types.T_enum:
				val := vector.GetFixedAt[types.Enum](vec, i).String()
				writeByte = appendBytes(writeByte, []byte(val), symbol[j], closeby, flag[j])
			default:
				ses.Error(ctx,
					"Failed to construct byte due to unsupported type",
					zap.Int("typeOid", int(vec.GetType().Oid)))
				ByteChan <- &BatchByte{
					err: moerr.NewInternalError(ctx, "constructByte : unsupported type %d", vec.GetType().Oid),
				}
				bat.Clean(ses.GetMemPool())
				return
			}
		}
	}

	ByteChan <- &BatchByte{
		index:     index,
		writeByte: writeByte,
		err:       nil,
	}
	ses.writeCsvBytes.Add(int64(len(writeByte))) // statistic out traffic, CASE 2: select into
	bat.Clean(ses.GetMemPool())
}

func addEscapeToString(s []byte) []byte {
	pos := make([]int, 0)
	for i := 0; i < len(s); i++ {
		if s[i] == escape {
			pos = append(pos, i)
		}
	}
	if len(pos) == 0 {
		return s
	}
	ret := make([]byte, 0)
	cur := 0
	for i := 0; i < len(pos); i++ {
		ret = append(ret, s[cur:pos[i]]...)
		ret = append(ret, escape)
		cur = pos[i]
	}
	ret = append(ret, s[cur:]...)
	return ret
}

func exportDataToCSVFile(oq *ExportConfig) error {
	if !oq.OutTofile {
		return exportDataToCSVFile2(oq)
	}
	oq.LineSize = 0

	symbol := oq.Symbol
	closeby := oq.userConfig.Fields.EnclosedBy.Value
	flag := oq.ColumnFlag
	for i := uint64(0); i < oq.mrs.GetColumnCount(); i++ {
		column, err := oq.mrs.GetColumn(oq.ctx, i)
		if err != nil {
			return err
		}
		mysqlColumn, ok := column.(*MysqlColumn)
		if !ok {
			return moerr.NewInternalError(oq.ctx, "sendColumn need MysqlColumn")
		}
		if isNil, err := oq.mrs.ColumnIsNull(oq.ctx, 0, i); err != nil {
			return err
		} else if isNil {
			//NULL is output as \N
			if err = formatOutputString(oq, []byte{'\\', 'N'}, symbol[i], closeby, false); err != nil {
				return err
			}
			continue
		}

		switch mysqlColumn.ColumnType() {
		case defines.MYSQL_TYPE_DECIMAL:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_BOOL:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_BIT:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_YEAR:
			value, err := oq.mrs.GetInt64(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if mysqlColumn.ColumnType() == defines.MYSQL_TYPE_YEAR {
				if value == 0 {
					if err = formatOutputString(oq, []byte("0000"), symbol[i], closeby, flag[i]); err != nil {
						return err
					}
				} else {
					oq.resetLineStr()
					oq.lineStr = strconv.AppendInt(oq.lineStr, value, 10)
					if err = formatOutputString(oq, oq.lineStr, symbol[i], closeby, flag[i]); err != nil {
						return err
					}
				}
			} else {
				oq.resetLineStr()
				oq.lineStr = strconv.AppendInt(oq.lineStr, value, 10)
				if err = formatOutputString(oq, oq.lineStr, symbol[i], closeby, flag[i]); err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_FLOAT, defines.MYSQL_TYPE_DOUBLE:
			value, err := oq.mrs.GetFloat64(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			oq.lineStr = []byte(fmt.Sprintf("%v", value))
			if err = formatOutputString(oq, oq.lineStr, symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_LONGLONG:
			if uint32(mysqlColumn.Flag())&defines.UNSIGNED_FLAG != 0 {
				if value, err := oq.mrs.GetUint64(oq.ctx, 0, i); err != nil {
					return err
				} else {
					oq.resetLineStr()
					oq.lineStr = strconv.AppendUint(oq.lineStr, value, 10)
					if err = formatOutputString(oq, oq.lineStr, symbol[i], closeby, flag[i]); err != nil {
						return err
					}
				}
			} else {
				if value, err := oq.mrs.GetInt64(oq.ctx, 0, i); err != nil {
					return err
				} else {
					oq.resetLineStr()
					oq.lineStr = strconv.AppendInt(oq.lineStr, value, 10)
					if err = formatOutputString(oq, oq.lineStr, symbol[i], closeby, flag[i]); err != nil {
						return err
					}
				}
			}
		// Binary/varbinary has mysql_type_varchar.
		case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING,
			defines.MYSQL_TYPE_BLOB, defines.MYSQL_TYPE_TEXT:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			value = addEscapeToString(value.([]byte))
			if err = formatOutputString(oq, value.([]byte), symbol[i], closeby, true); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_DATE:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value.(types.Date).String()), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_TIME:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value.(types.Time).String()), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_DATETIME:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value.(string)), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_TIMESTAMP:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_JSON:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			jsonStr := value.(bytejson.ByteJson).String()
			if err = formatOutputString(oq, []byte(jsonStr), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_UUID:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_ENUM:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i]); err != nil {
				return err
			}
		default:
			return moerr.NewInternalError(oq.ctx, "unsupported column type %d ", mysqlColumn.ColumnType())
		}
	}
	oq.Rows++
	_, err := EndOfLine(oq)
	return err
}

func exportDataToCSVFile2(ep *ExportConfig) error {
	var tmp *BatchByte
	select {
	case tmp = <-ep.ByteChan:
	default:
	}
	if tmp != nil {
		if tmp.err != nil {
			return tmp.err
		}
		ep.BatchMap[tmp.index] = tmp.writeByte
	}

	value, ok := ep.BatchMap[ep.WriteIndex.Load()+1]
	if !ok {
		return nil
	}

	if err := writeToCSVFile(ep, value); err != nil {
		return err
	}
	ep.WriteIndex.Add(1)
	ep.BatchMap[ep.WriteIndex.Load()] = nil
	_, err := EndOfLine(ep)
	return err
}

func exportAllData(ep *ExportConfig) error {
	var tmp *BatchByte
	for {
		tmp = nil
		if ep.WriteIndex == ep.Index {
			break
		}
		select {
		case tmp = <-ep.ByteChan:
		default:
		}
		if tmp != nil {
			if tmp.err != nil {
				return tmp.err
			}
			ep.BatchMap[tmp.index] = tmp.writeByte
		}

		value, ok := ep.BatchMap[ep.WriteIndex.Load()+1]
		if !ok {
			continue
		}
		if err := writeToCSVFile(ep, value); err != nil {
			return err
		}
		ep.WriteIndex.Add(1)
		ep.BatchMap[ep.WriteIndex.Load()] = nil
	}
	ep.First = false
	ep.FileCnt = 0
	ep.ByteChan = nil
	ep.BatchMap = nil
	return nil
}

var _ CsvWriter = &ExportConfig{}

func (ec *ExportConfig) init() {
	ec.ByteChan = make(chan *BatchByte, 10)
	ec.BatchMap = make(map[int32][]byte)
	ec.Index.Store(0)
	ec.WriteIndex.Store(0)
}

func (ec *ExportConfig) resetLineStr() {
	ec.lineStr = ec.lineStr[:0]
}

func (ec *ExportConfig) Write(execCtx *ExecCtx, bat *batch.Batch) error {
	ec.Index.Add(1)
	copied, err := bat.Dup(execCtx.ses.GetMemPool())
	if err != nil {
		return err
	}
	go constructByte(execCtx.reqCtx, execCtx.ses, copied, ec.Index.Load(), ec.ByteChan, ec)
	if err = exportDataToCSVFile(ec); err != nil {
		execCtx.ses.Error(execCtx.reqCtx,
			"Error occurred while exporting to CSV file",
			zap.Error(err))
		return err
	}
	return nil
}

func (ec *ExportConfig) Close() {
	if ec != nil {
		ec.mrs = nil
		ec.lineStr = nil
		ec.ctx = nil
	}
}

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
	"bytes"
	"context"
	"fmt"
	"io"
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
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type ExportConfig struct {
	// configs from user input
	userConfig *tree.ExportParam

	// curFileSize
	CurFileSize uint64
	Rows        uint64
	FileCnt     uint
	ColumnFlag  []bool
	Symbol      [][]byte
	// default flush size
	DefaultBufSize int64

	//file service & buffer for the line
	writeParam
	FileService fileservice.FileService
	Ctx         context.Context
	AsyncReader *io.PipeReader
	AsyncWriter *io.PipeWriter
	AsyncGroup  *errgroup.Group
	mrs         *MysqlResultSet
	ctx         context.Context
	service     string
}

type writeParam struct {
	First      bool
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
	columnsSet := make(map[string]int)
	for i := 0; i < len(mrs.Columns); i++ {
		columnsSet[mrs.Columns[i].Name()] = i
	}
	ep.ColumnFlag = make([]bool, len(mrs.Columns))
	for i := 0; i < len(ep.userConfig.ForceQuote); i++ {
		col, ok := columnsSet[ep.userConfig.ForceQuote[i]]
		if ok {
			ep.ColumnFlag[col] = true
		}
	}
}

var openNewFile = func(ctx context.Context, ep *ExportConfig, mrs *MysqlResultSet) error {
	var err error
	var filePath string
	ep.CurFileSize = 0

	ep.AsyncReader, ep.AsyncWriter = io.Pipe()
	if len(ep.userConfig.StageFilePath) != 0 {
		filePath = getExportFilePath(ep.userConfig.StageFilePath, ep.FileCnt)
	} else {
		filePath = getExportFilePath(ep.userConfig.FilePath, ep.FileCnt)
	}

	// fileservice is determined from filepath
	// if filepath is SHARED:/path, return getGlobalPu().FileService, filepath
	// otherwise, return fileservice.GetForETL(ctx, nil, filepath)
	fspath, err := fileservice.ParsePath(filePath)
	if err != nil {
		return err
	}

	var readPath string
	if fspath.Service == defines.SharedFileServiceName {
		ep.FileService = getPu(ep.service).FileService
		readPath = filePath

	} else {
		ep.FileService, readPath, err = fileservice.GetForETL(ctx, nil, filePath)
		if err != nil {
			return err
		}

	}

	asyncWriteFunc := func() error {
		vec := fileservice.IOVector{
			FilePath: readPath,
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
	}

	ep.Rows = 0
	return nil
}

func getExportFilePath(filename string, fileCnt uint) string {
	if fileCnt == 0 {
		return filename
	} else {
		return fmt.Sprintf("%s.%d", filename, fileCnt)
	}
}

var formatOutputString = func(oq *ExportConfig, tmp, symbol []byte, enclosed byte, flag bool, buffer *bytes.Buffer) error {
	var err error
	if flag && enclosed != 0 {
		if _, err = buffer.Write([]byte{enclosed}); err != nil {
			return err
		}
	}
	if _, err = buffer.Write(tmp); err != nil {
		return err
	}
	if flag && enclosed != 0 {
		if _, err = buffer.Write([]byte{enclosed}); err != nil {
			return err
		}
	}
	if _, err = buffer.Write(symbol); err != nil {
		return err
	}
	return nil
}

var Close = func(ep *ExportConfig) error {
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

var Write = func(ep *ExportConfig, output []byte) (int, error) {
	n, err := ep.AsyncWriter.Write(output)
	if err != nil {
		err2 := ep.AsyncWriter.CloseWithError(err)
		if err2 != nil {
			return 0, err2
		}
	}
	return n, err
}

// wrtieToCSVFile function may create a new file. Make sure the output buffer contains the complete CSV row to keep the CSV parser happy.
func writeToCSVFile(ep *ExportConfig, output []byte) error {
	if ep.userConfig.MaxFileSize != 0 && ep.CurFileSize+uint64(len(output)) > ep.userConfig.MaxFileSize {
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
	ep.CurFileSize += uint64(len(output))
	return nil
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

	buffer := &bytes.Buffer{}

	for i := 0; i < bat.RowCount(); i++ {
		for j, vec := range bat.Vecs {
			if vec.GetNulls().Contains(uint64(i)) {
				formatOutputString(ep, []byte("\\N"), symbol[j], closeby, flag[j], buffer)
				continue
			}
			switch vec.GetType().Oid { //get col
			case types.T_json:
				val := types.DecodeJson(vec.GetBytesAt(i))
				formatOutputString(ep, []byte(formatJsonString(val.String(), flag[j], terminated)), symbol[j], closeby, flag[j], buffer)
			case types.T_bool:
				val := vector.GetFixedAtNoTypeCheck[bool](vec, i)
				if val {
					formatOutputString(ep, []byte("true"), symbol[j], closeby, flag[j], buffer)
				} else {
					formatOutputString(ep, []byte("false"), symbol[j], closeby, flag[j], buffer)
				}
			case types.T_bit:
				val := vector.GetFixedAtNoTypeCheck[uint64](vec, i)
				bitLength := vec.GetType().Width
				byteLength := (bitLength + 7) / 8
				b := types.EncodeUint64(&val)[:byteLength]
				slices.Reverse(b)
				formatOutputString(ep, b, symbol[j], closeby, flag[j], buffer)
			case types.T_int8:
				val := vector.GetFixedAtNoTypeCheck[int8](vec, i)
				formatOutputString(ep, []byte(strconv.FormatInt(int64(val), 10)), symbol[j], closeby, flag[j], buffer)
			case types.T_int16:
				val := vector.GetFixedAtNoTypeCheck[int16](vec, i)
				formatOutputString(ep, []byte(strconv.FormatInt(int64(val), 10)), symbol[j], closeby, flag[j], buffer)
			case types.T_int32:
				val := vector.GetFixedAtNoTypeCheck[int32](vec, i)
				formatOutputString(ep, []byte(strconv.FormatInt(int64(val), 10)), symbol[j], closeby, flag[j], buffer)
			case types.T_int64:
				val := vector.GetFixedAtNoTypeCheck[int64](vec, i)
				formatOutputString(ep, []byte(strconv.FormatInt(int64(val), 10)), symbol[j], closeby, flag[j], buffer)
			case types.T_uint8:
				val := vector.GetFixedAtNoTypeCheck[uint8](vec, i)
				formatOutputString(ep, []byte(strconv.FormatUint(uint64(val), 10)), symbol[j], closeby, flag[j], buffer)
			case types.T_uint16:
				val := vector.GetFixedAtNoTypeCheck[uint16](vec, i)
				formatOutputString(ep, []byte(strconv.FormatUint(uint64(val), 10)), symbol[j], closeby, flag[j], buffer)
			case types.T_uint32:
				val := vector.GetFixedAtNoTypeCheck[uint32](vec, i)
				formatOutputString(ep, []byte(strconv.FormatUint(uint64(val), 10)), symbol[j], closeby, flag[j], buffer)
			case types.T_uint64:
				val := vector.GetFixedAtNoTypeCheck[uint64](vec, i)
				formatOutputString(ep, []byte(strconv.FormatUint(uint64(val), 10)), symbol[j], closeby, flag[j], buffer)
			case types.T_float32:
				val := vector.GetFixedAtNoTypeCheck[float32](vec, i)
				if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
					formatOutputString(ep, []byte(strconv.FormatFloat(float64(val), 'f', -1, 32)), symbol[j], closeby, flag[j], buffer)
				} else {
					formatOutputString(ep, []byte(strconv.FormatFloat(float64(val), 'f', int(vec.GetType().Scale), 64)), symbol[j], closeby, flag[j], buffer)
				}
			case types.T_float64:
				val := vector.GetFixedAtNoTypeCheck[float64](vec, i)
				if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
					formatOutputString(ep, []byte(strconv.FormatFloat(float64(val), 'f', -1, 32)), symbol[j], closeby, flag[j], buffer)
				} else {
					formatOutputString(ep, []byte(strconv.FormatFloat(float64(val), 'f', int(vec.GetType().Scale), 64)), symbol[j], closeby, flag[j], buffer)
				}
			case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
				value := addEscapeToString(vec.GetBytesAt(i))
				formatOutputString(ep, value, symbol[j], closeby, true, buffer)
			case types.T_array_float32:
				arrStr := types.BytesToArrayToString[float32](vec.GetBytesAt(i))
				value := addEscapeToString(util2.UnsafeStringToBytes(arrStr))
				formatOutputString(ep, value, symbol[j], closeby, true, buffer)
			case types.T_array_float64:
				arrStr := types.BytesToArrayToString[float64](vec.GetBytesAt(i))
				value := addEscapeToString(util2.UnsafeStringToBytes(arrStr))
				formatOutputString(ep, value, symbol[j], closeby, true, buffer)
			case types.T_date:
				val := vector.GetFixedAtNoTypeCheck[types.Date](vec, i)
				formatOutputString(ep, []byte(val.String()), symbol[j], closeby, flag[j], buffer)
			case types.T_datetime:
				scale := vec.GetType().Scale
				val := vector.GetFixedAtNoTypeCheck[types.Datetime](vec, i).String2(scale)
				formatOutputString(ep, []byte(val), symbol[j], closeby, flag[j], buffer)
			case types.T_time:
				scale := vec.GetType().Scale
				val := vector.GetFixedAtNoTypeCheck[types.Time](vec, i).String2(scale)
				formatOutputString(ep, []byte(val), symbol[j], closeby, flag[j], buffer)
			case types.T_timestamp:
				scale := vec.GetType().Scale
				timeZone := ses.GetTimeZone()
				val := vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, i).String2(timeZone, scale)
				formatOutputString(ep, []byte(val), symbol[j], closeby, flag[j], buffer)
			case types.T_decimal64:
				scale := vec.GetType().Scale
				val := vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, i).Format(scale)
				formatOutputString(ep, []byte(val), symbol[j], closeby, flag[j], buffer)
			case types.T_decimal128:
				scale := vec.GetType().Scale
				val := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, i).Format(scale)
				formatOutputString(ep, []byte(val), symbol[j], closeby, flag[j], buffer)
			case types.T_uuid:
				val := vector.GetFixedAtNoTypeCheck[types.Uuid](vec, i).String()
				formatOutputString(ep, []byte(val), symbol[j], closeby, flag[j], buffer)
			case types.T_Rowid:
				val := vector.GetFixedAtNoTypeCheck[types.Rowid](vec, i)
				formatOutputString(ep, []byte(val.String()), symbol[j], closeby, flag[j], buffer)
			case types.T_Blockid:
				val := vector.GetFixedAtNoTypeCheck[types.Blockid](vec, i)
				formatOutputString(ep, []byte(val.String()), symbol[j], closeby, flag[j], buffer)
			case types.T_enum:
				val := vector.GetFixedAtNoTypeCheck[types.Enum](vec, i).String()
				formatOutputString(ep, []byte(val), symbol[j], closeby, flag[j], buffer)
			default:
				ses.Error(ctx,
					"Failed to construct byte due to unsupported type",
					zap.Int("typeOid", int(vec.GetType().Oid)))
				ByteChan <- &BatchByte{
					err: moerr.NewInternalErrorf(ctx, "constructByte : unsupported type %d", vec.GetType().Oid),
				}
				bat.Clean(ses.GetMemPool())
				return
			}
		}
	}

	// copy data. byteBuffer.Bytes() is not able to pass to channel
	reslen := buffer.Len()
	result := make([]byte, reslen)
	copy(result, buffer.Bytes())
	buffer = nil

	ByteChan <- &BatchByte{
		index:     index,
		writeByte: result,
		err:       nil,
	}
	ses.writeCsvBytes.Add(int64(reslen)) // statistic out traffic, CASE 2: select into
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

func exportDataFromResultSetToCSVFile(oq *ExportConfig) error {
	symbol := oq.Symbol
	closeby := oq.userConfig.Fields.EnclosedBy.Value
	flag := oq.ColumnFlag

	buffer := &bytes.Buffer{}

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
			if err = formatOutputString(oq, []byte{'\\', 'N'}, symbol[i], closeby, false, buffer); err != nil {
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
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i], buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_BOOL:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i], buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_BIT:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i], buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_YEAR:
			value, err := oq.mrs.GetInt64(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if mysqlColumn.ColumnType() == defines.MYSQL_TYPE_YEAR {
				if value == 0 {
					if err = formatOutputString(oq, []byte("0000"), symbol[i], closeby, flag[i], buffer); err != nil {
						return err
					}
				} else {
					var lineStr []byte
					lineStr = strconv.AppendInt(lineStr, value, 10)
					if err = formatOutputString(oq, lineStr, symbol[i], closeby, flag[i], buffer); err != nil {
						return err
					}
				}
			} else {
				var lineStr []byte
				lineStr = strconv.AppendInt(lineStr, value, 10)
				if err = formatOutputString(oq, lineStr, symbol[i], closeby, flag[i], buffer); err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_FLOAT, defines.MYSQL_TYPE_DOUBLE:
			value, err := oq.mrs.GetFloat64(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			lineStr := []byte(fmt.Sprintf("%v", value))
			if err = formatOutputString(oq, lineStr, symbol[i], closeby, flag[i], buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_LONGLONG:
			if uint32(mysqlColumn.Flag())&defines.UNSIGNED_FLAG != 0 {
				if value, err := oq.mrs.GetUint64(oq.ctx, 0, i); err != nil {
					return err
				} else {
					var lineStr []byte
					lineStr = strconv.AppendUint(lineStr, value, 10)
					if err = formatOutputString(oq, lineStr, symbol[i], closeby, flag[i], buffer); err != nil {
						return err
					}
				}
			} else {
				if value, err := oq.mrs.GetInt64(oq.ctx, 0, i); err != nil {
					return err
				} else {
					var lineStr []byte
					lineStr = strconv.AppendInt(lineStr, value, 10)
					if err = formatOutputString(oq, lineStr, symbol[i], closeby, flag[i], buffer); err != nil {
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
			if err = formatOutputString(oq, value.([]byte), symbol[i], closeby, true, buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_DATE:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value.(types.Date).String()), symbol[i], closeby, flag[i], buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_TIME:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value.(types.Time).String()), symbol[i], closeby, flag[i], buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_DATETIME:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value.(string)), symbol[i], closeby, flag[i], buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_TIMESTAMP:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i], buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_JSON:
			value, err := oq.mrs.GetValue(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			jsonStr := value.(bytejson.ByteJson).String()
			if err = formatOutputString(oq, []byte(jsonStr), symbol[i], closeby, flag[i], buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_UUID:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i], buffer); err != nil {
				return err
			}
		case defines.MYSQL_TYPE_ENUM:
			value, err := oq.mrs.GetString(oq.ctx, 0, i)
			if err != nil {
				return err
			}
			if err = formatOutputString(oq, []byte(value), symbol[i], closeby, flag[i], buffer); err != nil {
				return err
			}
		default:
			return moerr.NewInternalErrorf(oq.ctx, "unsupported column type %d ", mysqlColumn.ColumnType())
		}
	}

	// write the complete CSV row to CSV Writer
	err := writeToCSVFile(oq, buffer.Bytes())
	if err != nil {
		return err
	}
	oq.Rows++
	return err
}

func exportDataFromBatchToCSVFile(ep *ExportConfig) error {
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

	err := writeToCSVFile(ep, value)
	if err != nil {
		return err
	}
	ep.WriteIndex.Add(1)
	ep.BatchMap[ep.WriteIndex.Load()] = nil
	return err
}

func exportAllDataFromBatches(ep *ExportConfig) error {
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

func (ec *ExportConfig) Write(execCtx *ExecCtx, crs *perfcounter.CounterSet, bat *batch.Batch) error {
	ec.Index.Add(1)
	copied, err := bat.Dup(execCtx.ses.GetMemPool())
	if err != nil {
		return err
	}
	go constructByte(execCtx.reqCtx, execCtx.ses, copied, ec.Index.Load(), ec.ByteChan, ec)

	if err = exportDataFromBatchToCSVFile(ec); err != nil {
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
		ec.ctx = nil
	}
}

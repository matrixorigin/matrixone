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
	"strconv"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"golang.org/x/sync/errgroup"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type ExportParam struct {
	*tree.ExportParam
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
	FileService    fileservice.FileService
	LineBuffer     *bytes.Buffer
	Ctx            context.Context
	AsyncReader    *io.PipeReader
	AsyncWriter    *io.PipeWriter
	AsyncGroup     *errgroup.Group
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

func initExportFileParam(ep *ExportParam, mrs *MysqlResultSet) {
	ep.DefaultBufSize *= 1024 * 1024
	n := (int)(mrs.GetColumnCount())
	if n <= 0 {
		return
	}
	ep.Symbol = make([][]byte, n)
	for i := 0; i < n-1; i++ {
		ep.Symbol[i] = []byte(ep.Fields.Terminated)
	}
	ep.Symbol[n-1] = []byte(ep.Lines.TerminatedBy)
	ep.ColumnFlag = make([]bool, len(mrs.Name2Index))
	for i := 0; i < len(ep.ForceQuote); i++ {
		col, ok := mrs.Name2Index[ep.ForceQuote[i]]
		if ok {
			ep.ColumnFlag[col] = true
		}
	}
}

var openNewFile = func(ctx context.Context, ep *ExportParam, mrs *MysqlResultSet) error {
	lineSize := ep.LineSize
	var err error
	ep.CurFileSize = 0
	if !ep.UseFileService {
		filePath := getExportFilePath(ep.FilePath, ep.FileCnt)
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
		filePath := getExportFilePath(ep.FilePath, ep.FileCnt)

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
	if ep.Header {
		var header string
		n := len(mrs.Columns)
		if n == 0 {
			return nil
		}
		for i := 0; i < n-1; i++ {
			header += mrs.Columns[i].Name() + ep.Fields.Terminated
		}
		header += mrs.Columns[n-1].Name() + ep.Lines.TerminatedBy
		if ep.MaxFileSize != 0 && uint64(len(header)) >= ep.MaxFileSize {
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

var formatOutputString = func(oq *outputQueue, tmp, symbol []byte, enclosed byte, flag bool) error {
	var err error
	if flag {
		if err = writeToCSVFile(oq, []byte{enclosed}); err != nil {
			return err
		}
	}
	if err = writeToCSVFile(oq, tmp); err != nil {
		return err
	}
	if flag {
		if err = writeToCSVFile(oq, []byte{enclosed}); err != nil {
			return err
		}
	}
	if err = writeToCSVFile(oq, symbol); err != nil {
		return err
	}
	return nil
}

var Flush = func(ep *ExportParam) error {
	if !ep.UseFileService {
		return ep.Writer.Flush()
	}
	return nil
}

var Seek = func(ep *ExportParam) (int64, error) {
	if !ep.UseFileService {
		return ep.File.Seek(int64(ep.CurFileSize-ep.LineSize), io.SeekStart)
	}
	return 0, nil
}

var Read = func(ep *ExportParam) (int, error) {
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

var Truncate = func(ep *ExportParam) error {
	if !ep.UseFileService {
		return ep.File.Truncate(int64(ep.CurFileSize - ep.LineSize))
	} else {
		return nil
	}
}

var Close = func(ep *ExportParam) error {
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

var Write = func(ep *ExportParam, output []byte) (int, error) {
	if !ep.UseFileService {
		return ep.Writer.Write(output)
	} else {
		return ep.LineBuffer.Write(output)
	}
}

var EndOfLine = func(ep *ExportParam) (int, error) {
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

func writeToCSVFile(oq *outputQueue, output []byte) error {
	if oq.ep.MaxFileSize != 0 && oq.ep.CurFileSize+uint64(len(output)) > oq.ep.MaxFileSize {
		if oq.ep.Rows == 0 {
			return moerr.NewInternalError(oq.ctx, "the OneLine size is over the maxFileSize")
		}

		if err := Flush(oq.ep); err != nil {
			return err
		}
		if oq.ep.LineSize != 0 {
			if _, err := Seek(oq.ep); err != nil {
				return err
			}
			for {
				if n, err := Read(oq.ep); err != nil {
					return err
				} else if uint64(n) == oq.ep.LineSize {
					break
				}
			}
			if err := Truncate(oq.ep); err != nil {
				return err
			}
		}
		if err := Close(oq.ep); err != nil {
			return err
		}
		if err := openNewFile(oq.ctx, oq.ep, oq.mrs); err != nil {
			return err
		}
	}

	if err := writeDataToCSVFile(oq.ep, output); err != nil {
		return err
	}
	return nil
}

var writeDataToCSVFile = func(ep *ExportParam, output []byte) error {
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

func exportDataToCSVFile(oq *outputQueue) error {
	oq.ep.LineSize = 0

	symbol := oq.ep.Symbol
	closeby := oq.ep.Fields.EnclosedBy
	flag := oq.ep.ColumnFlag
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
		default:
			return moerr.NewInternalError(oq.ctx, "unsupported column type %d ", mysqlColumn.ColumnType())
		}
	}
	oq.ep.Rows++
	_, err := EndOfLine(oq.ep)
	return err
}

package frontend

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var OpenFile = os.OpenFile

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

func initExportFileParam(ep *tree.ExportParam, mrs *MysqlResultSet) {
	ep.DefaultBufSize *= 1024 * 1024
	n := (int)(mrs.GetColumnCount())
	if n <= 0 {
		return
	}
	ep.Symbol = make([]string, n)
	for i := 0; i < n-1; i++ {
		ep.Symbol[i] = ep.Fields.Terminated
	}
	ep.Symbol[n-1] = ep.Lines.TerminatedBy
	ep.ColumnFlag = make([]bool, len(mrs.Name2Index))
	for i := 0; i < len(ep.ForceQuote); i++ {
		col, ok := mrs.Name2Index[ep.ForceQuote[i]]
		if ok {
			ep.ColumnFlag[col] = true
		}
	}
}

var openNewFile = func(ep *tree.ExportParam, mrs *MysqlResultSet) error {
	lineSize := ep.LineSize
	var err error
	ep.CurFileSize = 0
	filePath := getExportFilePath(ep.FilePath, ep.FileCnt)
	ep.File, err = OpenFile(filePath, os.O_RDWR|os.O_EXCL|os.O_CREATE, 0o666)
	if err != nil {
		return err
	}
	ep.Writer = bufio.NewWriterSize(ep.File, int(ep.DefaultBufSize))
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
			return errors.New("the header line size is over the maxFileSize")
		}
		if err := writeDataToCSVFile(ep, []byte(header)); err != nil {
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
		err = writeToCSVFile(oq, []byte{enclosed})
		if err != nil {
			return err
		}
	}
	err = writeToCSVFile(oq, tmp)
	if err != nil {
		return err
	}
	if flag {
		err = writeToCSVFile(oq, []byte{enclosed})
		if err != nil {
			return err
		}
	}
	err = writeToCSVFile(oq, symbol)
	if err != nil {
		return err
	}
	return nil
}

var Flush = func(ep *tree.ExportParam) error {
	return ep.Writer.Flush()
}

var Seek = func(ep *tree.ExportParam) (int64, error) {
	return ep.File.Seek(int64(ep.CurFileSize-ep.LineSize), io.SeekStart)
}

var Read = func(ep *tree.ExportParam) (int, error) {
	ep.OutputStr = make([]byte, ep.LineSize)
	return ep.File.Read(ep.OutputStr)
}

var Truncate = func(ep *tree.ExportParam) error {
	return ep.File.Truncate(int64(ep.CurFileSize - ep.LineSize))
}

var Close = func(ep *tree.ExportParam) error {
	return ep.File.Close()
}

var Write = func(ep *tree.ExportParam, output []byte) (int, error) {
	return ep.Writer.Write(output)
}

func writeToCSVFile(oq *outputQueue, output []byte) error {
	if oq.ep.MaxFileSize != 0 && oq.ep.CurFileSize+uint64(len(output)) > oq.ep.MaxFileSize {
		if oq.ep.Rows == 0 {
			return errors.New("the OneLine size is over the maxFileSize")
		}
		oq.ep.FileCnt++
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
		if err := openNewFile(oq.ep, oq.mrs); err != nil {
			return err
		}
	}

	if err := writeDataToCSVFile(oq.ep, output); err != nil {
		return err
	}
	return nil
}

var writeDataToCSVFile = func(ep *tree.ExportParam, output []byte) error {
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

func exportDataToCSVFile(oq *outputQueue) error {
	oq.ep.LineSize = 0

	for i := uint64(0); i < oq.mrs.GetColumnCount(); i++ {
		column, err := oq.mrs.GetColumn(i)
		if err != nil {
			return err
		}
		mysqlColumn, ok := column.(*MysqlColumn)
		if !ok {
			return fmt.Errorf("sendColumn need MysqlColumn")
		}
		if isNil, err1 := oq.mrs.ColumnIsNull(0, i); err1 != nil {
			return err1
		} else if isNil {
			//NULL is output as \N
			formatOutputString(oq, []byte{'\\', 'N'}, []byte(oq.ep.Symbol[i]), oq.ep.Fields.EnclosedBy, false)
			continue
		}
		switch mysqlColumn.ColumnType() {
		case defines.MYSQL_TYPE_DECIMAL:
			return fmt.Errorf("unsupported Decimal")
		case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_YEAR:
			if value, err2 := oq.mrs.GetInt64(0, i); err2 != nil {
				return err2
			} else {
				if mysqlColumn.ColumnType() == defines.MYSQL_TYPE_YEAR {
					if value == 0 {
						if err := formatOutputString(oq, []byte("0000"), []byte(oq.ep.Symbol[i]), oq.ep.Fields.EnclosedBy, oq.ep.ColumnFlag[i]); err != nil {
							return err
						}
					} else {
						if err := formatOutputString(oq, []byte(fmt.Sprintf("%d", value)), []byte(oq.ep.Symbol[i]), oq.ep.Fields.EnclosedBy, oq.ep.ColumnFlag[i]); err != nil {
							return err
						}
					}
				} else {
					if err := formatOutputString(oq, []byte(fmt.Sprintf("%d", value)), []byte(oq.ep.Symbol[i]), oq.ep.Fields.EnclosedBy, oq.ep.ColumnFlag[i]); err != nil {
						return err
					}
				}
			}
		case defines.MYSQL_TYPE_FLOAT, defines.MYSQL_TYPE_DOUBLE:
			if value, err2 := oq.mrs.GetFloat64(0, i); err2 != nil {
				return err2
			} else {
				if err := formatOutputString(oq, []byte(fmt.Sprintf("%v", value)), []byte(oq.ep.Symbol[i]), oq.ep.Fields.EnclosedBy, oq.ep.ColumnFlag[i]); err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_LONGLONG:
			if uint32(mysqlColumn.Flag())&defines.UNSIGNED_FLAG != 0 {
				if value, err2 := oq.mrs.GetUint64(0, i); err2 != nil {
					return err2
				} else {
					if err := formatOutputString(oq, []byte(fmt.Sprintf("%d", value)), []byte(oq.ep.Symbol[i]), oq.ep.Fields.EnclosedBy, oq.ep.ColumnFlag[i]); err != nil {
						return err
					}
				}
			} else {
				if value, err2 := oq.mrs.GetInt64(0, i); err2 != nil {
					return err2
				} else {
					if err := formatOutputString(oq, []byte(fmt.Sprintf("%d", value)), []byte(oq.ep.Symbol[i]), oq.ep.Fields.EnclosedBy, oq.ep.ColumnFlag[i]); err != nil {
						return err
					}
				}
			}
		case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING:
			if value, err2 := oq.mrs.GetString(0, i); err2 != nil {
				return err2
			} else {
				if err := formatOutputString(oq, []byte(value), []byte(oq.ep.Symbol[i]), oq.ep.Fields.EnclosedBy, oq.ep.ColumnFlag[i]); err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_DATE:
			if value, err2 := oq.mrs.GetValue(0, i); err2 != nil {
				return err2
			} else {
				if err := formatOutputString(oq, []byte(value.(types.Date).String()), []byte(oq.ep.Symbol[i]), oq.ep.Fields.EnclosedBy, oq.ep.ColumnFlag[i]); err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_DATETIME:
			if value, err2 := oq.mrs.GetValue(0, i); err2 != nil {
				return err2
			} else {
				if err := formatOutputString(oq, []byte(value.(types.Datetime).String()), []byte(oq.ep.Symbol[i]), oq.ep.Fields.EnclosedBy, oq.ep.ColumnFlag[i]); err != nil {
					return err
				}
			}
		case defines.MYSQL_TYPE_TIMESTAMP, defines.MYSQL_TYPE_TIME:
			return fmt.Errorf("unsupported DATE/DATETIME/TIMESTAMP/MYSQL_TYPE_TIME")
		default:
			return fmt.Errorf("unsupported column type %d ", mysqlColumn.ColumnType())
		}
	}
	oq.ep.Rows++
	return nil
}

package frontend

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

var OpenFile = os.OpenFile

func initExportFileParam(oq *outputQueue) {
	oq.ep.DefaultBufSize *= 1024 * 1024
	n := (int)(oq.mrs.GetColumnCount())
	if n <= 0 {
		return
	}
	oq.ep.Symbol = make([]string, n)
	for i := 0; i < n - 1; i++ {
		oq.ep.Symbol[i] = oq.ep.Fields.Terminated
	}
	oq.ep.Symbol[n - 1] = oq.ep.Lines.TerminatedBy
	oq.ep.ColumnFlag = make([]bool, len(oq.mrs.Name2Index))
	for i := 0; i < len(oq.ep.ForceQuote); i++ {
		col, ok := oq.mrs.Name2Index[oq.ep.ForceQuote[i]]
		if ok {
			oq.ep.ColumnFlag[col] = true
		}
	}
}

var openNewFile = func(oq *outputQueue) error {
	lineSize := oq.ep.LineSize
	var err error
	oq.ep.CurFileSize = 0
	filePath := getExportFilePath(oq.ep.FilePath, oq.ep.FileCnt)
	oq.file, err = OpenFile(filePath, os.O_RDWR | os.O_EXCL | os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	oq.writer = bufio.NewWriterSize(oq.file, int(oq.ep.DefaultBufSize))
	if oq.ep.Header {
		var header string
		n := len(oq.mrs.Columns)
		if n == 0 {
			return nil
		}
		for i := 0; i < n - 1; i++ {
			header += oq.mrs.Columns[i].Name() + oq.ep.Fields.Terminated
		}
		header += oq.mrs.Columns[n - 1].Name() + oq.ep.Lines.TerminatedBy
		if oq.ep.MaxFileSize != 0 && uint64(len(header)) >= oq.ep.MaxFileSize {
			return errors.New("the header line size is over the maxFileSize")
		}
		if err := writeDataToCSVFile(oq, []byte(header)); err != nil {
			return err
		}
	}
	if lineSize != 0 {
		oq.ep.LineSize = 0
		oq.ep.Rows = 0
		if err := writeDataToCSVFile(oq, oq.ep.OutputStr); err != nil {
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

var Flush = func(oq *outputQueue) error {
	return oq.writer.Flush()
}

var Seek = func(oq *outputQueue) (int64, error) {
	return oq.file.Seek(int64(oq.ep.CurFileSize - oq.ep.LineSize), io.SeekStart)
}

var Read = func(oq *outputQueue) (int, error) {
	oq.ep.OutputStr = make([]byte, oq.ep.LineSize)
	return oq.file.Read(oq.ep.OutputStr)
}

var Truncate = func(oq *outputQueue) error {
	return oq.file.Truncate(int64(oq.ep.CurFileSize - oq.ep.LineSize))
}

var Close = func(oq *outputQueue) error {
	return oq.file.Close()
}

var Write = func(oq *outputQueue, output []byte) (int, error) {
	return oq.writer.Write(output)
}

func writeToCSVFile(oq *outputQueue, output []byte) error {
	if oq.ep.MaxFileSize != 0 && oq.ep.CurFileSize + uint64(len(output)) > oq.ep.MaxFileSize {
		if oq.ep.Rows == 0 {
			return errors.New("The OneLine size is over the maxFileSize")
		}
		oq.ep.FileCnt++
		if err := Flush(oq); err != nil {
			return err
		}
		if oq.ep.LineSize != 0 {
			if _, err := Seek(oq); err != nil {
				return err
			}
			if _, err := Read(oq); err != nil {
				return err
			}
			if err := Truncate(oq); err != nil {
				return err
			}
		}
		if err := Close(oq); err != nil {
			return err
		}
		if err := openNewFile(oq); err != nil {
			return err
		}
	}

	if err := writeDataToCSVFile(oq, output); err != nil {
		return err
	}
	return nil
}

var writeDataToCSVFile = func(oq *outputQueue, output []byte) error {
	if _, err := Write(oq, output); err != nil {
		return err
	}
	oq.ep.LineSize += uint64(len(output))
	oq.ep.CurFileSize += uint64(len(output))
	return nil
}

var costTime time.Duration

func exportDataToCSVFile(oq *outputQueue) error {
	oq.ep.LineSize = 0
	begin := time.Now()
	costTime += time.Since(begin)

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
					return  err2
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
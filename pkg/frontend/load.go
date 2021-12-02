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

package frontend

import (
	"encoding/csv"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/simdcsv"
)

type LoadResult struct {
	Records, Deleted, Skipped, Warnings, WriteTimeout uint64
}

type DebugTime struct {
	row2col   time.Duration
	fillBlank time.Duration
	toStorage time.Duration

	writeBatch time.Duration
	resetBatch time.Duration

	prefix     time.Duration
	skip_bytes time.Duration

	process_field     time.Duration
	split_field       time.Duration
	split_before_loop time.Duration
	wait_loop         time.Duration
	handler_get       time.Duration
	wait_switch       time.Duration
	field_first_byte  time.Duration
	field_enclosed    time.Duration
	field_without     time.Duration
	field_skip_bytes  time.Duration

	callback       time.Duration
	asyncChan      time.Duration
	csvLineArray1  time.Duration
	csvLineArray2  time.Duration
	asyncChanLoop  time.Duration
	saveParsedLine time.Duration
	choose_true    time.Duration
	choose_false   time.Duration
}

type SharePart struct {
	//load reference
	load *tree.Load
	//how to handle errors during converting field
	ignoreFieldError bool

	//index of line in line array
	lineIdx     int
	maxFieldCnt int
	bytes       uint64

	lineCount uint64

	//batch
	batchSize int

	//map column id in from data to column id in table
	dataColumnId2TableColumnId []int

	cols      []*engine.AttributeDef
	attrName  []string
	timestamp uint64

	//simd csv
	simdCsvLineArray [][]string

	//storage
	dbHandler    engine.Database
	tableHandler engine.Relation

	//result of load
	result *LoadResult
}

type notifyEventType int

const (
	NOTIFY_EVENT_WRITE_BATCH_ERROR notifyEventType = iota
	NOTIFY_EVENT_WRITE_BATCH_RESULT
	NOTIFY_EVENT_READ_SIMDCSV_ERROR
	NOTIFY_EVENT_OUTPUT_SIMDCSV_ERROR
	NOTIFY_EVENT_END
)

type notifyEvent struct {
	neType notifyEventType
	err    error
	wbh    *WriteBatchHandler
}

func newNotifyEvent(t notifyEventType, e error, w *WriteBatchHandler) *notifyEvent {
	return &notifyEvent{
		neType: t,
		err:    e,
		wbh:    w,
	}
}

type PoolElement struct {
	bat       *batch.Batch
	lineArray [][]string
}

type ParseLineHandler struct {
	SharePart
	DebugTime

	simdCsvReader               *simdcsv.Reader
	closeOnceGetParsedLinesChan sync.Once
	//csv read put lines into the channel
	simdCsvGetParsedLinesChan chan simdcsv.LineOut
	//the count of writing routine
	simdCsvConcurrencyCountOfWriteBatch int
	//wait write routines to quit
	simdCsvWaitWriteRoutineToQuit *sync.WaitGroup
	simdCsvBatchPool              chan *PoolElement
	simdCsvNotiyEventChan         chan *notifyEvent
	closeOnce                     sync.Once

	closeRef *CloseLoadData
}

type WriteBatchHandler struct {
	SharePart
	DebugTime

	batchData   *batch.Batch
	pl          *PoolElement
	batchFilled int
	simdCsvErr  error

	closeRef *CloseLoadData
}

type CloseLoadData struct {
	stopLoadData chan interface{}
	onceClose    sync.Once
}

func NewCloseLoadData() *CloseLoadData {
	return &CloseLoadData{
		stopLoadData: make(chan interface{}),
	}
}

func (cld *CloseLoadData) Open() {
}

func (cld *CloseLoadData) Close() {
	cld.onceClose.Do(func() {
		close(cld.stopLoadData)
	})
}

func (plh *ParseLineHandler) getLineOutFromSimdCsvRoutine() error {
	wait_a := time.Now()
	defer func() {
		plh.asyncChan += time.Since(wait_a)
	}()

	var lineOut simdcsv.LineOut
	for {
		quit := false
		select {
		case <-plh.closeRef.stopLoadData:
			//fmt.Printf("----- get stop in getLineOutFromSimdCsvRoutine\n")
			quit = true
		case lineOut = <-plh.simdCsvGetParsedLinesChan:
		}

		if quit {
			break
		}

		wait_d := time.Now()
		if lineOut.Line == nil && lineOut.Lines == nil {
			break
		}
		if lineOut.Line != nil {
			//step 1 : skip dropped lines
			if plh.lineCount < plh.load.IgnoredLines {
				plh.lineCount++
				continue
			}

			wait_b := time.Now()
			//step 2 : append line into line array
			plh.simdCsvLineArray[plh.lineIdx] = lineOut.Line
			plh.lineIdx++
			plh.lineCount++
			plh.maxFieldCnt = Max(plh.maxFieldCnt, len(lineOut.Line))
			//for _, ll := range lineOut.Line {
			//	plh.bytes += uint64(utf8.RuneCount([]byte(ll)))
			//}

			plh.csvLineArray1 += time.Since(wait_b)

			if plh.lineIdx == plh.batchSize {
				//fmt.Printf("+++++ batch bytes %v B %v MB\n",plh.bytes,plh.bytes / 1024.0 / 1024.0)
				err := saveLinesToStorage(plh, false)
				if err != nil {
					return err
				}

				plh.lineIdx = 0
				plh.maxFieldCnt = 0
				//plh.bytes = 0
			}

		} else if lineOut.Lines != nil {
			from := 0
			countOfLines := len(lineOut.Lines)
			//step 1 : skip dropped lines
			if plh.lineCount < plh.load.IgnoredLines {
				skipped := MinUint64(uint64(countOfLines), plh.load.IgnoredLines-plh.lineCount)
				plh.lineCount += skipped
				from += int(skipped)
			}

			fill := 0
			//step 2 : append lines into line array
			for i := from; i < countOfLines; i += fill {
				fill = Min(countOfLines-i, plh.batchSize-plh.lineIdx)
				wait_c := time.Now()
				for j := 0; j < fill; j++ {
					plh.simdCsvLineArray[plh.lineIdx] = lineOut.Lines[i+j]
					plh.lineIdx++
					plh.maxFieldCnt = Max(plh.maxFieldCnt, len(lineOut.Lines[i+j]))
				}
				plh.csvLineArray2 += time.Since(wait_c)

				if plh.lineIdx == plh.batchSize {
					//fmt.Printf("+---+ batch bytes %v B %v MB\n",plh.bytes,plh.bytes / 1024.0 / 1024.0)
					err := saveLinesToStorage(plh, false)
					if err != nil {
						return err
					}

					plh.lineIdx = 0
					plh.maxFieldCnt = 0
				}
			}
		}
		plh.asyncChanLoop += time.Since(wait_d)
	}

	//last batch
	err := saveLinesToStorage(plh, true)
	if err != nil {
		return err
	}
	return nil
}

func (plh *ParseLineHandler) close() {
	plh.closeOnceGetParsedLinesChan.Do(func() {
		close(plh.simdCsvGetParsedLinesChan)
	})
	plh.closeOnce.Do(func() {
		close(plh.simdCsvBatchPool)
		close(plh.simdCsvNotiyEventChan)
		plh.simdCsvReader.Close()
	})
	plh.closeRef.Close()
}

/*
alloc space for the batch
*/
func makeBatch(handler *ParseLineHandler) *PoolElement {
	batchData := batch.New(true, handler.attrName)

	//fmt.Printf("----- batchSize %d attrName %v \n",batchSize,handler.attrName)

	batchSize := handler.batchSize

	//alloc space for vector
	for i := 0; i < len(handler.attrName); i++ {
		vec := vector.New(handler.cols[i].Attr.Type)
		switch vec.Typ.Oid {
		case types.T_int8:
			vec.Col = make([]int8, batchSize)
		case types.T_int16:
			vec.Col = make([]int16, batchSize)
		case types.T_int32:
			vec.Col = make([]int32, batchSize)
		case types.T_int64:
			vec.Col = make([]int64, batchSize)
		case types.T_uint8:
			vec.Col = make([]uint8, batchSize)
		case types.T_uint16:
			vec.Col = make([]uint16, batchSize)
		case types.T_uint32:
			vec.Col = make([]uint32, batchSize)
		case types.T_uint64:
			vec.Col = make([]uint64, batchSize)
		case types.T_float32:
			vec.Col = make([]float32, batchSize)
		case types.T_float64:
			vec.Col = make([]float64, batchSize)
		case types.T_char, types.T_varchar:
			vBytes := &types.Bytes{
				Offsets: make([]uint32, batchSize),
				Lengths: make([]uint32, batchSize),
				Data:    nil,
			}
			vec.Col = vBytes
		case types.T_date:
			vec.Col	= make([]types.Date,batchSize)
		default:
			panic("unsupported vector type")
		}
		batchData.Vecs[i] = vec
	}

	return &PoolElement{
		bat:       batchData,
		lineArray: make([][]string, handler.batchSize),
	}
}

/*
Init ParseLineHandler
*/
func initParseLineHandler(handler *ParseLineHandler) error {
	relation := handler.tableHandler
	load := handler.load

	var cols []*engine.AttributeDef = nil
	defs := relation.TableDefs()
	for _, def := range defs {
		attr,ok := def.(*engine.AttributeDef)
		if ok {
			cols = append(cols,attr)
		}
	}

	attrName := make([]string, len(cols))
	tableName2ColumnId := make(map[string]int)
	for i, col := range cols {
		attrName[i] = col.Attr.Name
		tableName2ColumnId[col.Attr.Name] = i
	}

	handler.cols = cols
	handler.attrName = attrName

	//define the peer column for LOAD DATA's column list.
	var dataColumnId2TableColumnId []int = nil
	if len(load.ColumnList) == 0 {
		dataColumnId2TableColumnId = make([]int, len(cols))
		for i := 0; i < len(cols); i++ {
			dataColumnId2TableColumnId[i] = i
		}
	} else {
		dataColumnId2TableColumnId = make([]int, len(load.ColumnList))
		for i, col := range load.ColumnList {
			switch realCol := col.(type) {
			case *tree.UnresolvedName:
				tid, ok := tableName2ColumnId[realCol.Parts[0]]
				if !ok {
					return fmt.Errorf("no such column %s", realCol.Parts[0])
				}
				dataColumnId2TableColumnId[i] = tid
			case *tree.VarExpr:
				//NOTE:variable like '@abc' will be passed by.
				dataColumnId2TableColumnId[i] = -1
			default:
				return fmt.Errorf("unsupported column type %v", realCol)
			}
		}
	}
	handler.dataColumnId2TableColumnId = dataColumnId2TableColumnId

	//allocate batch
	for j := 0; j < cap(handler.simdCsvBatchPool); j++ {
		batchData := makeBatch(handler)
		handler.simdCsvBatchPool <- batchData
	}
	return nil
}

/*
alloc a batch from the pool.
if the pool does not have batch anymore, the caller routine will be suspended.
*/
func allocBatch(handler *ParseLineHandler) *PoolElement {
	batchData := <-handler.simdCsvBatchPool
	return batchData
}

/*
return a batch into the pool
*/
func releaseBatch(handler *ParseLineHandler, pl *PoolElement) {
	//clear batch
	//clear vector.nulls.Nulls
	for _, vec := range pl.bat.Vecs {
		vec.Nsp = &nulls.Nulls{}
		switch vec.Typ.Oid {
		case types.T_char, types.T_varchar:
			vBytes := vec.Col.(*types.Bytes)
			vBytes.Data = vBytes.Data[:0]
		}
	}
	handler.simdCsvBatchPool <- pl
}

/**
it may be suspended, when the pool does not have enough batch
*/
func initWriteBatchHandler(handler *ParseLineHandler, wHandler *WriteBatchHandler) error {
	wHandler.ignoreFieldError = handler.ignoreFieldError
	wHandler.cols = handler.cols
	wHandler.dataColumnId2TableColumnId = handler.dataColumnId2TableColumnId
	wHandler.batchSize = handler.batchSize
	wHandler.attrName = handler.attrName
	wHandler.dbHandler = handler.dbHandler
	wHandler.tableHandler = handler.tableHandler
	wHandler.timestamp = handler.timestamp
	wHandler.result = &LoadResult{}
	wHandler.closeRef = handler.closeRef
	wHandler.lineCount = handler.lineCount

	wHandler.pl = allocBatch(handler)
	wHandler.simdCsvLineArray = wHandler.pl.lineArray
	for i := 0; i < handler.lineIdx; i++ {
		wHandler.simdCsvLineArray[i] = handler.simdCsvLineArray[i]
	}

	wHandler.batchData = wHandler.pl.bat
	return nil
}

func collectWriteBatchResult(handler *ParseLineHandler, wh *WriteBatchHandler, err error) {
	//fmt.Printf("++++> %d %d %d %d \n",
	//	wh.result.Skipped,
	//	wh.result.Deleted,
	//	wh.result.Warnings,
	//	wh.result.Records,
	//)
	if wh == nil {
		return
	}

	handler.result.Skipped += wh.result.Skipped
	handler.result.Deleted += wh.result.Deleted
	handler.result.Warnings += wh.result.Warnings
	handler.result.Records += wh.result.Records
	handler.result.WriteTimeout += wh.result.WriteTimeout
	//
	handler.row2col += wh.row2col
	handler.fillBlank += wh.fillBlank
	handler.toStorage += wh.toStorage

	handler.writeBatch += wh.writeBatch
	handler.resetBatch += wh.resetBatch

	//
	handler.callback += wh.callback
	handler.asyncChan += wh.asyncChan
	handler.asyncChanLoop += wh.asyncChanLoop
	handler.csvLineArray1 += wh.csvLineArray1
	handler.csvLineArray2 += wh.csvLineArray2
	handler.saveParsedLine += wh.saveParsedLine
	handler.choose_true += wh.choose_true
	handler.choose_false += wh.choose_false

	wh.batchData = nil
	wh.simdCsvLineArray = nil
	wh.simdCsvErr = nil
}

func makeParsedFailedError(tp, field, column string, line uint64, offset int) *MysqlError {
	return NewMysqlError(ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
		tp,
		field,
		column,
		line+uint64(offset))
}

func errorCanBeIgnored(err error) bool {
	switch err.(type) {
	case *MysqlError, *csv.ParseError:
		return false
	default:
		return true
	}
}

/*
isWriteBatchTimeoutError returns true when the err is a write batch timeout.
 */
func isWriteBatchTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	es := err.Error()
	if strings.Index(es,"exec timeout") != -1 {
		return true
	}
	return false
}

func rowToColumnAndSaveToStorage(handler *WriteBatchHandler, forceConvert bool) error {
	begin := time.Now()
	defer func() {
		handler.saveParsedLine += time.Since(begin)
		//fmt.Printf("-----saveParsedLinesToBatchSimdCsv %s\n",time.Since(begin))
	}()

	countOfLineArray := handler.lineIdx
	if !forceConvert {
		if countOfLineArray != handler.batchSize {
			fmt.Printf("---->countOfLineArray %d batchSize %d \n", countOfLineArray, handler.batchSize)
			panic("-----write a batch")
		}
	}

	batchData := handler.batchData
	columnFLags := make([]byte, len(batchData.Vecs))
	fetchCnt := 0
	var err error
	allFetchCnt := 0

	row2col := time.Duration(0)
	fillBlank := time.Duration(0)
	toStorage := time.Duration(0)
	//write batch of  lines
	//for lineIdx := 0; lineIdx < countOfLineArray; lineIdx += fetchCnt {
	//fill batch
	fetchCnt = countOfLineArray
	//fmt.Printf("-----fetchCnt %d len(lineArray) %d\n",fetchCnt,len(handler.simdCsvLineArray))
	fetchLines := handler.simdCsvLineArray[:fetchCnt]

	/*
		row to column
	*/

	batchBegin := handler.batchFilled
	ignoreFieldError := handler.ignoreFieldError
	result := handler.result
	choose := true

	//logutil.Infof("-----ignoreFieldError %v",handler.ignoreFieldError)

	if choose {
		wait_d := time.Now()
		for i, line := range fetchLines {
			//fmt.Printf("line %d %v \n",i,line)
			//wait_a := time.Now()
			rowIdx := batchBegin + i
			offset := i + 1
			base := handler.lineCount - uint64(fetchCnt)
			//fmt.Println(line)
			//fmt.Printf("------ linecount %d fetchcnt %d base %d offset %d \n",
			//	handler.lineCount,fetchCnt,base,offset)
			//record missing column
			for k := 0; k < len(columnFLags); k++ {
				columnFLags[k] = 0
			}

			for j, field := range line {
				//fmt.Printf("data col %d : %v \n",j,field)
				//where will column j go ?
				colIdx := -1
				if j < len(handler.dataColumnId2TableColumnId) {
					colIdx = handler.dataColumnId2TableColumnId[j]
				}
				//else{
				//	//mysql warning ER_WARN_TOO_MANY_RECORDS
				//	result.Warnings++
				//}
				//drop this field
				if colIdx == -1 {
					continue
				}

				isNullOrEmpty := len(field) == 0

				//put it into batch
				vec := batchData.Vecs[colIdx]
				vecAttr := batchData.Attrs[colIdx]

				//record colIdx
				columnFLags[colIdx] = 1

				//fmt.Printf("data set col %d : %v \n",j,field)

				switch vec.Typ.Oid {
				case types.T_int8:
					cols := vec.Col.([]int8)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp,uint64(rowIdx))
					} else {
						d, err := strconv.ParseInt(field, 10, 8)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
							}
							//mysql warning ER_TRUNCATED_WRONG_VALUE_FOR_FIELD
							result.Warnings++
							d = 0
							//break
						}
						cols[rowIdx] = int8(d)
					}
				case types.T_int16:
					cols := vec.Col.([]int16)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp,uint64(rowIdx))
					} else {
						d, err := strconv.ParseInt(field, 10, 16)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[rowIdx] = int16(d)
					}
				case types.T_int32:
					cols := vec.Col.([]int32)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp,uint64(rowIdx))
					} else {
						d, err := strconv.ParseInt(field, 10, 32)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[rowIdx] = int32(d)
					}
				case types.T_int64:
					cols := vec.Col.([]int64)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp,uint64(rowIdx))
					} else {
						d, err := strconv.ParseInt(field, 10, 64)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[rowIdx] = d
					}
				case types.T_uint8:
					cols := vec.Col.([]uint8)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp,uint64(rowIdx))
					} else {
						d, err := strconv.ParseUint(field, 10, 8)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[rowIdx] = uint8(d)
					}
				case types.T_uint16:
					cols := vec.Col.([]uint16)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp,uint64(rowIdx))
					} else {
						d, err := strconv.ParseUint(field, 10, 16)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[rowIdx] = uint16(d)
					}
				case types.T_uint32:
					cols := vec.Col.([]uint32)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp,uint64(rowIdx))
					} else {
						d, err := strconv.ParseUint(field, 10, 32)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[rowIdx] = uint32(d)
					}
				case types.T_uint64:
					cols := vec.Col.([]uint64)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp,uint64(rowIdx))
					} else {
						d, err := strconv.ParseUint(field, 10, 64)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[rowIdx] = uint64(d)
					}
				case types.T_float32:
					cols := vec.Col.([]float32)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp,uint64(rowIdx))
					} else {
						d, err := strconv.ParseFloat(field, 32)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[rowIdx] = float32(d)
					}
				case types.T_float64:
					cols := vec.Col.([]float64)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp,uint64(rowIdx))
					} else {
						fs := field
						//fmt.Printf("==== > field string [%s] \n",fs)
						d, err := strconv.ParseFloat(fs, 64)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[rowIdx] = d
					}
				case types.T_char, types.T_varchar:
					vBytes := vec.Col.(*types.Bytes)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp,uint64(rowIdx))
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
						nulls.Add(vec.Nsp,uint64(rowIdx))
					} else {
						fs := field
						//fmt.Printf("==== > field string [%s] \n",fs)
						d, err := types.ParseDate(fs)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[rowIdx] = d
					}
				default:
					panic("unsupported oid")
				}
			}
			//row2col += time.Since(wait_a)

			//wait_b := time.Now()
			//the row does not have field
			for k := 0; k < len(columnFLags); k++ {
				if 0 == columnFLags[k] {
					vec := batchData.Vecs[k]
					switch vec.Typ.Oid {
					case types.T_char, types.T_varchar:
						vBytes := vec.Col.(*types.Bytes)
						vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
						vBytes.Lengths[rowIdx] = uint32(0)
					}
					nulls.Add(vec.Nsp,uint64(rowIdx))

					//mysql warning ER_WARN_TOO_FEW_RECORDS
					//result.Warnings++
				}
			}
			//fillBlank += time.Since(wait_b)
		}
		handler.choose_true += time.Since(wait_d)
	} else {
		wait_d := time.Now()
		//record missing column
		for k := 0; k < len(columnFLags); k++ {
			columnFLags[k] = 0
		}

		wait_a := time.Now()
		//column
		for j := 0; j < handler.maxFieldCnt; j++ {
			//where will column j go ?
			colIdx := -1
			if j < len(handler.dataColumnId2TableColumnId) {
				colIdx = handler.dataColumnId2TableColumnId[j]
			}
			//drop this field
			if colIdx == -1 {
				continue
			}

			//put it into batch
			vec := batchData.Vecs[colIdx]

			columnFLags[j] = 1

			switch vec.Typ.Oid {
			case types.T_int8:
				cols := vec.Col.([]int8)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp,uint64(i))
					} else {
						field := line[j]
						d, err := strconv.ParseInt(field, 10, 8)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return err
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[i] = int8(d)
					}
				}
			case types.T_int16:
				cols := vec.Col.([]int16)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp,uint64(i))
					} else {
						field := line[j]
						d, err := strconv.ParseInt(field, 10, 16)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return err
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[i] = int16(d)
					}
				}
			case types.T_int32:
				cols := vec.Col.([]int32)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp,uint64(i))
					} else {
						field := line[j]
						d, err := strconv.ParseInt(field, 10, 32)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return err
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[i] = int32(d)
					}
				}
			case types.T_int64:
				cols := vec.Col.([]int64)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp,uint64(i))
					} else {
						field := line[j]
						d, err := strconv.ParseInt(field, 10, 64)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return err
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[i] = d
					}
				}
			case types.T_uint8:
				cols := vec.Col.([]uint8)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp,uint64(i))
					} else {
						field := line[j]
						d, err := strconv.ParseUint(field, 10, 8)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return err
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[i] = uint8(d)
					}
				}
			case types.T_uint16:
				cols := vec.Col.([]uint16)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp,uint64(i))
					} else {
						field := line[j]
						d, err := strconv.ParseUint(field, 10, 16)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return err
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[i] = uint16(d)
					}
				}
			case types.T_uint32:
				cols := vec.Col.([]uint32)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp,uint64(i))
					} else {
						field := line[j]
						d, err := strconv.ParseUint(field, 10, 32)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return err
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[i] = uint32(d)
					}
				}
			case types.T_uint64:
				cols := vec.Col.([]uint64)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp,uint64(i))
					} else {
						field := line[j]
						d, err := strconv.ParseUint(field, 10, 64)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return err
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[i] = uint64(d)
					}
				}
			case types.T_float32:
				cols := vec.Col.([]float32)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp,uint64(i))
					} else {
						field := line[j]
						d, err := strconv.ParseFloat(field, 32)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return err
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[i] = float32(d)
					}
				}
			case types.T_float64:
				cols := vec.Col.([]float64)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp,uint64(i))
					} else {
						field := line[j]
						//fmt.Printf("==== > field string [%s] \n",fs)
						d, err := strconv.ParseFloat(field, 64)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return err
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[i] = d
					}
				}
			case types.T_char, types.T_varchar:
				vBytes := vec.Col.(*types.Bytes)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp,uint64(i))
						vBytes.Offsets[i] = uint32(len(vBytes.Data))
						vBytes.Lengths[i] = uint32(len(line[j]))
					} else {
						field := line[j]
						vBytes.Offsets[i] = uint32(len(vBytes.Data))
						vBytes.Data = append(vBytes.Data, field...)
						vBytes.Lengths[i] = uint32(len(field))
					}
				}
			case types.T_date:
				cols := vec.Col.([]types.Date)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp,uint64(i))
					} else {
						field := line[j]
						//fmt.Printf("==== > field string [%s] \n",fs)
						d, err := types.ParseDate(field)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return err
							}
							result.Warnings++
							d = 0
							//break
						}
						cols[i] = d
					}
				}
			default:
				panic("unsupported oid")
			}
		}
		row2col += time.Since(wait_a)

		wait_b := time.Now()
		//the row does not have field
		for k := 0; k < len(columnFLags); k++ {
			if 0 == columnFLags[k] {
				vec := batchData.Vecs[k]
				//row
				for i := 0; i < countOfLineArray; i++ {
					switch vec.Typ.Oid {
					case types.T_char, types.T_varchar:
						vBytes := vec.Col.(*types.Bytes)
						vBytes.Offsets[i] = uint32(len(vBytes.Data))
						vBytes.Lengths[i] = uint32(0)
					}
					nulls.Add(vec.Nsp,uint64(i))
				}
			}
		}
		fillBlank += time.Since(wait_b)
		handler.choose_false += time.Since(wait_d)
	}

	handler.batchFilled = batchBegin + fetchCnt

	//if handler.batchFilled == handler.batchSize {
	//	minLen := math.MaxInt64
	//	maxLen := 0
	//	for _, vec := range batchData.Vecs {
	//		fmt.Printf("len %d type %d %s \n",vec.Length(),vec.Typ.Oid,vec.Typ.String())
	//		minLen = Min(vec.Length(),int(minLen))
	//		maxLen = Max(vec.Length(),int(maxLen))
	//	}
	//
	//	if minLen != maxLen{
	//		logutil.Errorf("vector length mis equal %d %d",minLen,maxLen)
	//		return fmt.Errorf("vector length mis equal %d %d",minLen,maxLen)
	//	}
	//}

	wait_c := time.Now()
	/*
		write batch into the engine
	*/
	//the second parameter must be FALSE here
	err = writeBatchToStorage(handler,forceConvert)

	toStorage += time.Since(wait_c)

	allFetchCnt += fetchCnt
	//}

	handler.row2col += row2col
	handler.fillBlank += fillBlank
	handler.toStorage += toStorage

	//fmt.Printf("----- row2col %s fillBlank %s toStorage %s\n",
	//	row2col,fillBlank,toStorage)

	if err != nil {
		logutil.Errorf("saveBatchToStorage failed. err:%v",err)
		return err
	}

	if allFetchCnt != countOfLineArray {
		return fmt.Errorf("allFetchCnt %d != countOfLineArray %d ", allFetchCnt, countOfLineArray)
	}

	return nil
}

/*
save batch to storage.
when force is true, batchsize will be changed.
*/
func writeBatchToStorage(handler *WriteBatchHandler,force bool) error {
	var err error = nil
	if handler.batchFilled == handler.batchSize{
		//batchBytes := 0
		//for _, vec := range handler.batchData.Vecs {
		//	//fmt.Printf("len %d type %d %s \n",vec.Length(),vec.Typ.Oid,vec.Typ.String())
		//	switch vec.Typ.Oid {
		//	case types.T_char, types.T_varchar:
		//		vBytes := vec.Col.(*types.Bytes)
		//		batchBytes += len(vBytes.Data)
		//	default:
		//		batchBytes += vec.Length() * int(vec.Typ.Size)
		//	}
		//}
		//
		//fmt.Printf("----batchBytes %v B %v MB\n",batchBytes,batchBytes / 1024.0 / 1024.0)
		//
		wait_a := time.Now()
		err = handler.tableHandler.Write(handler.timestamp,handler.batchData)
		if err == nil {
			handler.result.Records += uint64(handler.batchSize)
		}else if isWriteBatchTimeoutError(err) {
			logutil.Errorf("write failed. err: %v",err)
			handler.result.WriteTimeout += uint64(handler.batchSize)
			//clean timeout error
			err = nil
		}else{
			logutil.Errorf("write failed. err: %v",err)
			handler.result.Skipped += uint64(handler.batchSize)
		}

		handler.writeBatch += time.Since(wait_a)

		wait_b := time.Now()
		//clear batch
		//clear vector.nulls.Nulls
		for _, vec := range handler.batchData.Vecs {
			vec.Nsp = &nulls.Nulls{}
			switch vec.Typ.Oid {
			case types.T_char, types.T_varchar:
				vBytes := vec.Col.(*types.Bytes)
				vBytes.Data = vBytes.Data[:0]
			}
		}
		handler.batchFilled = 0

		handler.resetBatch += time.Since(wait_b)
	} else {
		if force {
			//first, remove redundant rows at last
			needLen := handler.batchFilled
			if needLen > 0 {
				//logutil.Infof("needLen: %d batchSize %d", needLen, handler.batchSize)
				for _, vec := range handler.batchData.Vecs {
					//fmt.Printf("needLen %d %d type %d %s \n",needLen,i,vec.Typ.Oid,vec.Typ.String())
					//remove nulls.NUlls
					for j := uint64(handler.batchFilled); j < uint64(handler.batchSize); j++ {
						nulls.Del(vec.Nsp,j)
					}
					//remove row
					switch vec.Typ.Oid {
					case types.T_int8:
						cols := vec.Col.([]int8)
						vec.Col = cols[:needLen]
					case types.T_int16:
						cols := vec.Col.([]int16)
						vec.Col = cols[:needLen]
					case types.T_int32:
						cols := vec.Col.([]int32)
						vec.Col = cols[:needLen]
					case types.T_int64:
						cols := vec.Col.([]int64)
						vec.Col = cols[:needLen]
					case types.T_uint8:
						cols := vec.Col.([]uint8)
						vec.Col = cols[:needLen]
					case types.T_uint16:
						cols := vec.Col.([]uint16)
						vec.Col = cols[:needLen]
					case types.T_uint32:
						cols := vec.Col.([]uint32)
						vec.Col = cols[:needLen]
					case types.T_uint64:
						cols := vec.Col.([]uint64)
						vec.Col = cols[:needLen]
					case types.T_float32:
						cols := vec.Col.([]float32)
						vec.Col = cols[:needLen]
					case types.T_float64:
						cols := vec.Col.([]float64)
						vec.Col = cols[:needLen]
					case types.T_char, types.T_varchar: //bytes is different
						vBytes := vec.Col.(*types.Bytes)
						//fmt.Printf("saveBatchToStorage before data %s \n",vBytes.String())
						if len(vBytes.Offsets) > needLen {
							vec.Col = vBytes.Window(0, needLen)
						}

						//fmt.Printf("saveBatchToStorage after data %s \n",vBytes.String())
					case types.T_date:
						cols := vec.Col.([]types.Date)
						vec.Col = cols[:needLen]
					}
				}

				//for _, vec := range handler.batchData.Vecs {
				//	fmt.Printf("len %d type %d %s \n",vec.Length(),vec.Typ.Oid,vec.Typ.String())
				//}

				err = handler.tableHandler.Write(handler.timestamp, handler.batchData)
				if err == nil {
					handler.result.Records += uint64(needLen)
				}else if isWriteBatchTimeoutError(err) {
					logutil.Errorf("write failed. err: %v",err)
					handler.result.WriteTimeout += uint64(needLen)
					//clean timeout error
					err = nil
				}else{
					logutil.Errorf("write failed. err:%v \n", err)
					handler.result.Skipped += uint64(needLen)
				}
			}
		}
	}
	return err
}

func saveLinesToStorage(handler *ParseLineHandler, force bool) error {
	writeHandler := &WriteBatchHandler{
		SharePart: SharePart{
			lineIdx:     handler.lineIdx,
			maxFieldCnt: handler.maxFieldCnt,
		},
	}
	err := initWriteBatchHandler(handler, writeHandler)
	if err != nil {
		writeHandler.simdCsvErr = err
		return err
	}

	handler.simdCsvWaitWriteRoutineToQuit.Add(1)
	go func() {
		defer handler.simdCsvWaitWriteRoutineToQuit.Done()

		//step 3 : save into storage
		err = rowToColumnAndSaveToStorage(writeHandler, force)
		writeHandler.simdCsvErr = err

		releaseBatch(handler, writeHandler.pl)
		writeHandler.batchData = nil
		writeHandler.simdCsvLineArray = nil

		if err != nil {
			handler.simdCsvNotiyEventChan <- newNotifyEvent(NOTIFY_EVENT_WRITE_BATCH_ERROR, err, writeHandler)
		} else {
			handler.simdCsvNotiyEventChan <- newNotifyEvent(NOTIFY_EVENT_WRITE_BATCH_RESULT, nil, writeHandler)
		}

		//this is the last one
		if force {
			handler.simdCsvNotiyEventChan <- newNotifyEvent(NOTIFY_EVENT_END, nil, nil)
		}
	}()
	return nil
}

/*
LoadLoop reads data from stream, extracts the fields, and saves into the table
*/
func (mce *MysqlCmdExecutor) LoadLoop(load *tree.Load, dbHandler engine.Database, tableHandler engine.Relation) (*LoadResult, error) {
	defer func() {
		if er := recover(); er != nil {
			logutil.Errorf("loadLoop panic")
		}
	}()
	ses := mce.routine.GetSession()

	//begin:=  time.Now()
	//defer func() {
	//	fmt.Printf("-----load loop exit %s\n",time.Since(begin))
	//}()

	result := &LoadResult{}

	/*
		step1 : read block from file
	*/
	dataFile, err := os.Open(load.File)
	if err != nil {
		logutil.Errorf("open file failed. err:%v", err)
		return nil, err
	}
	defer func() {
		err := dataFile.Close()
		if err != nil {
			logutil.Errorf("close file failed. err:%v", err)
		}
	}()

	//processTime := time.Now()
	process_block := time.Duration(0)

	curBatchSize := int(ses.Pu.SV.GetBatchSizeInLoadData())
	channelSize := 100
	//simdcsv
	handler := &ParseLineHandler{
		SharePart: SharePart{
			load:             load,
			lineIdx:          0,
			simdCsvLineArray: make([][]string, curBatchSize),
			dbHandler:        dbHandler,
			tableHandler:     tableHandler,
			lineCount:        0,
			batchSize:        curBatchSize,
			result:           result,
		},
		simdCsvGetParsedLinesChan:     make(chan simdcsv.LineOut, channelSize),
		simdCsvWaitWriteRoutineToQuit: &sync.WaitGroup{},
	}

	handler.simdCsvConcurrencyCountOfWriteBatch = Min(int(ses.Pu.SV.GetLoadDataConcurrencyCount()), runtime.NumCPU())
	handler.simdCsvConcurrencyCountOfWriteBatch = Max(1, handler.simdCsvConcurrencyCountOfWriteBatch)
	handler.simdCsvBatchPool = make(chan *PoolElement, handler.simdCsvConcurrencyCountOfWriteBatch)

	//fmt.Printf("-----write concurrent count %d \n",handler.simdCsvConcurrencyCountOfWriteBatch)

	handler.ignoreFieldError = true
	dh := handler.load.DuplicateHandling
	if dh != nil {
		switch dh.(type) {
		case *tree.DuplicateKeyIgnore:
			handler.ignoreFieldError = true
		case *tree.DuplicateKeyError, *tree.DuplicateKeyReplace:
			handler.ignoreFieldError = false
		}
	}

	notifyChanSize := handler.simdCsvConcurrencyCountOfWriteBatch * 2
	notifyChanSize = Max(100, notifyChanSize)

	/*
		make close reference
	*/
	handler.closeRef = NewCloseLoadData()

	//put closeRef into the executor
	mce.loadDataClose = handler.closeRef

	handler.simdCsvReader = simdcsv.NewReaderWithOptions(dataFile,
		rune(load.Fields.Terminated[0]),
		'#',
		false,
		false)

	/*
		error channel
	*/
	handler.simdCsvNotiyEventChan = make(chan *notifyEvent, notifyChanSize)

	//release resources of handler
	defer handler.close()

	err = initParseLineHandler(handler)
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}

	/*
		read from the output channel of the simdcsv parser, make a batch,
		deliver it to async routine writing batch
	*/
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := handler.getLineOutFromSimdCsvRoutine()
		if err != nil {
			logutil.Errorf("get line from simdcsv failed. err:%v", err)
			handler.simdCsvNotiyEventChan <- newNotifyEvent(NOTIFY_EVENT_OUTPUT_SIMDCSV_ERROR, err, nil)
		}
	}()

	/*
		get lines from simdcsv, deliver them to the output channel.
	*/
	wg.Add(1)
	go func() {
		defer wg.Done()
		wait_b := time.Now()

		err := handler.simdCsvReader.ReadLoop(handler.simdCsvGetParsedLinesChan)
		if err != nil {
			handler.simdCsvNotiyEventChan <- newNotifyEvent(NOTIFY_EVENT_READ_SIMDCSV_ERROR, err, nil)
		}
		process_block += time.Since(wait_b)
	}()

	/*
		collect statistics from every batch.
	*/
	var ne *notifyEvent = nil
	for {
		quit := false
		select {
		case <-handler.closeRef.stopLoadData:
			//get obvious cancel
			quit = true
			//fmt.Printf("----- get stop in load \n")

		case ne = <-handler.simdCsvNotiyEventChan:
			switch ne.neType {
			case NOTIFY_EVENT_WRITE_BATCH_RESULT:
				collectWriteBatchResult(handler, ne.wbh, nil)
			case NOTIFY_EVENT_END:
				err = nil
				quit = true
			case NOTIFY_EVENT_READ_SIMDCSV_ERROR,
				NOTIFY_EVENT_OUTPUT_SIMDCSV_ERROR,
				NOTIFY_EVENT_WRITE_BATCH_ERROR:
				if !errorCanBeIgnored(ne.err) {
					err = ne.err
					quit = true
				}
				collectWriteBatchResult(handler, ne.wbh, ne.err)
			default:
				logutil.Errorf("get unsupported notify event %d", ne.neType)
				quit = true
			}
		}

		if quit {
			//
			handler.simdCsvReader.Close()
			handler.closeOnceGetParsedLinesChan.Do(func() {
				close(handler.simdCsvGetParsedLinesChan)
			})

			break
		}
	}

	wg.Wait()

	/*
		drain event channel
	*/
	quit := false
	for {
		select {
		case ne = <-handler.simdCsvNotiyEventChan:
		default:
			quit = true
		}

		if quit {
			break
		}

		switch ne.neType {
		case NOTIFY_EVENT_WRITE_BATCH_RESULT:
			collectWriteBatchResult(handler, ne.wbh, nil)
		case NOTIFY_EVENT_END:
		case NOTIFY_EVENT_READ_SIMDCSV_ERROR,
			NOTIFY_EVENT_OUTPUT_SIMDCSV_ERROR,
			NOTIFY_EVENT_WRITE_BATCH_ERROR:
			collectWriteBatchResult(handler, ne.wbh, ne.err)
		default:
		}
	}

	//wait write to quit
	handler.simdCsvWaitWriteRoutineToQuit.Wait()

	//fmt.Printf("-----total row2col %s fillBlank %s toStorage %s\n",
	//	handler.row2col,handler.fillBlank,handler.toStorage)
	//fmt.Printf("-----write batch %s reset batch %s\n",
	//	handler.writeBatch,handler.resetBatch)
	//fmt.Printf("----- simdcsv end %s " +
	//	"stage1_first_chunk %s stage1_end %s " +
	//	"stage2_first_chunkinfo - [begin end] [%s %s ] [%s %s ] [%s %s ] " +
	//	"readLoop_first_records %s \n",
	//	handler.simdCsvReader.End,
	//	handler.simdCsvReader.Stage1_first_chunk,
	//	handler.simdCsvReader.Stage1_end,
	//	handler.simdCsvReader.Stage2_first_chunkinfo[0],
	//	handler.simdCsvReader.Stage2_end[0],
	//	handler.simdCsvReader.Stage2_first_chunkinfo[1],
	//	handler.simdCsvReader.Stage2_end[1],
	//	handler.simdCsvReader.Stage2_first_chunkinfo[2],
	//	handler.simdCsvReader.Stage2_end[2],
	//	handler.simdCsvReader.ReadLoop_first_records,
	//	)
	//
	//fmt.Printf("-----call_back %s " +
	//	"process_block - callback %s " +
	//	"asyncChan %s asyncChanLoop %s asyncChan - asyncChanLoop %s " +
	//	"csvLineArray1 %s csvLineArray2 %s saveParsedLineToBatch %s " +
	//	"choose_true %s choose_false %s \n",
	//	handler.callback,
	//	process_block - handler.callback,
	//	handler.asyncChan,
	//	handler.asyncChanLoop,
	//	handler.asyncChan -	handler.asyncChanLoop,
	//	handler.csvLineArray1,
	//	handler.csvLineArray2,
	//	handler.saveParsedLine,
	//	handler.choose_true,
	//	handler.choose_false,
	//	)
	//
	//	fmt.Printf("-----process time %s \n",time.Since(processTime))

	return result, err
}

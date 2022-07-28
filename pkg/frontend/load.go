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
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/simdcsv"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

	// prefix time.Duration
	// skip_bytes time.Duration

	// process_field     time.Duration
	// split_field       time.Duration
	// split_before_loop time.Duration
	// wait_loop         time.Duration
	// handler_get       time.Duration
	// wait_switch       time.Duration
	// field_first_byte  time.Duration
	// field_enclosed    time.Duration
	// field_without     time.Duration
	// field_skip_bytes  time.Duration

	callback       time.Duration
	asyncChan      atomic.Value // time.Duration
	csvLineArray1  atomic.Value // time.Duration
	csvLineArray2  time.Duration
	asyncChanLoop  atomic.Value // time.Duration
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
	batchSize      int
	skipWriteBatch bool

	//map column id in from data to column id in table
	dataColumnId2TableColumnId []int

	cols      []*engine.AttributeDef
	attrName  []string
	timestamp uint64

	//simd csv
	simdCsvLineArray [][]string

	//storage
	storage        engine.Engine
	dbHandler      engine.Database
	tableHandler   engine.Relation
	dbName         string
	tableName      string
	txnHandler     *TxnHandler
	oneTxnPerBatch bool
	ses            *Session

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

const NULL_FLAG = "\\N"

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
	id        int
	bat       *batch.Batch
	lineArray [][]string
}

type ThreadInfo struct {
	threadCnt int32
	startTime atomic.Value
}

func (t *ThreadInfo) SetTime(tmp time.Time) {
	t.startTime.Store(tmp)
}

func (t *ThreadInfo) GetTime() (val interface{}) {
	return t.startTime.Load()
}

func (t *ThreadInfo) SetCnt(id int32) {
	atomic.StoreInt32(&t.threadCnt, id)
}

func (t *ThreadInfo) GetCnt() int32 {
	return atomic.LoadInt32(&t.threadCnt)
}

type ParseLineHandler struct {
	SharePart
	DebugTime

	threadInfo                  map[int]*ThreadInfo
	simdCsvReader               *simdcsv.Reader
	closeOnceGetParsedLinesChan sync.Once
	//csv read put lines into the channel
	simdCsvGetParsedLinesChan atomic.Value // chan simdcsv.LineOut
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
	*ThreadInfo

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

func getLineOutChan(v atomic.Value) chan simdcsv.LineOut {
	return v.Load().(chan simdcsv.LineOut)
}

func (plh *ParseLineHandler) getLineOutFromSimdCsvRoutine() error {
	wait_a := time.Now()
	defer func() {
		AtomicAddDuration(plh.asyncChan, time.Since(wait_a))
	}()

	var lineOut simdcsv.LineOut
	for {
		quit := false
		select {
		case <-plh.closeRef.stopLoadData:
			logutil.Infof("----- get stop in getLineOutFromSimdCsvRoutine")
			quit = true
		case lineOut = <-getLineOutChan(plh.simdCsvGetParsedLinesChan):
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

			AtomicAddDuration(plh.csvLineArray1, time.Since(wait_b))

			if plh.lineIdx == plh.batchSize {
				//logutil.Infof("+++++ batch bytes %v B %v MB",plh.bytes,plh.bytes / 1024.0 / 1024.0)
				err := saveLinesToStorage(plh, false)
				if err != nil {
					return err
				}

				plh.lineIdx = 0
				plh.maxFieldCnt = 0
				plh.bytes = 0
			}
		}
		AtomicAddDuration(plh.asyncChanLoop, time.Since(wait_d))
	}

	//last batch
	err := saveLinesToStorage(plh, true)
	if err != nil {
		return err
	}
	return nil
}

func AtomicAddDuration(v atomic.Value, t interface{}) {
	var ti time.Duration = 0
	switch t := t.(type) {
	case time.Duration:
		ti = t
	case atomic.Value:
		tx := t
		if tx.Load() != nil {
			ti = tx.Load().(time.Duration)
		}
	}
	if v.Load() == nil {
		v.Store(time.Duration(0) + ti)
	} else {
		v.Store(v.Load().(time.Duration) + ti)
	}
}

func (plh *ParseLineHandler) close() {
	plh.closeOnceGetParsedLinesChan.Do(func() {
		close(getLineOutChan(plh.simdCsvGetParsedLinesChan))
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
func makeBatch(handler *ParseLineHandler, id int) *PoolElement {
	batchData := batch.New(true, handler.attrName)

	//logutil.Infof("----- batchSize %d attrName %v",batchSize,handler.attrName)

	batchSize := handler.batchSize

	//alloc space for vector
	for i := 0; i < len(handler.attrName); i++ {
		vec := vector.New(handler.cols[i].Attr.Type)
		switch vec.Typ.Oid {
		case types.T_bool:
			vec.Col = make([]bool, batchSize)
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
			vec.Col = make([]types.Date, batchSize)
		case types.T_datetime:
			vec.Col = make([]types.Datetime, batchSize)
		case types.T_decimal64:
			vec.Col = make([]types.Decimal64, batchSize)
		case types.T_decimal128:
			vec.Col = make([]types.Decimal128, batchSize)
		case types.T_timestamp:
			vec.Col = make([]types.Timestamp, batchSize)
		default:
			panic("unsupported vector type")
		}
		batchData.Vecs[i] = vec
	}

	return &PoolElement{
		id:        id,
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
	ctx := context.TODO()
	defs, err := relation.TableDefs(ctx)
	if err != nil {
		return err
	}
	for _, def := range defs {
		attr, ok := def.(*engine.AttributeDef)
		if ok {
			cols = append(cols, attr)
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
	var dataColumnId2TableColumnId []int
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
		batchData := makeBatch(handler, j)
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
	wHandler.storage = handler.storage
	wHandler.dbName = handler.dbName
	wHandler.dbHandler = handler.dbHandler
	wHandler.tableHandler = handler.tableHandler
	wHandler.tableName = handler.tableName
	wHandler.txnHandler = handler.txnHandler
	wHandler.ses = handler.ses
	wHandler.oneTxnPerBatch = handler.oneTxnPerBatch
	wHandler.timestamp = handler.timestamp
	wHandler.result = &LoadResult{}
	wHandler.closeRef = handler.closeRef
	wHandler.lineCount = handler.lineCount
	wHandler.skipWriteBatch = handler.skipWriteBatch

	wHandler.pl = allocBatch(handler)
	wHandler.ThreadInfo = handler.threadInfo[wHandler.pl.id]
	wHandler.simdCsvLineArray = wHandler.pl.lineArray
	for i := 0; i < handler.lineIdx; i++ {
		wHandler.simdCsvLineArray[i] = handler.simdCsvLineArray[i]
	}

	wHandler.batchData = wHandler.pl.bat
	return nil
}

func collectWriteBatchResult(handler *ParseLineHandler, wh *WriteBatchHandler, err error) {
	//logutil.Infof("++++> %d %d %d %d",
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
	AtomicAddDuration(handler.asyncChan, wh.asyncChan)
	AtomicAddDuration(handler.asyncChanLoop, wh.asyncChanLoop)
	AtomicAddDuration(handler.csvLineArray1, wh.csvLineArray1)
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
	return errors.Is(err, context.DeadlineExceeded)
}

func judgeInterge(field string) bool {
	for i := 0; i < len(field); i++ {
		if field[i] > '9' || field[i] < '0' {
			return false
		}
	}
	return true
}

func rowToColumnAndSaveToStorage(handler *WriteBatchHandler, forceConvert bool, row2colChoose bool) error {
	begin := time.Now()
	defer func() {
		handler.saveParsedLine += time.Since(begin)
		//logutil.Infof("-----saveParsedLinesToBatchSimdCsv %s",time.Since(begin))
	}()

	countOfLineArray := handler.lineIdx

	/*
		XXX: orig code commented out the panic, therefore this
		branch is noop, generating a go warning.  panic will
		cause a test failure.

		Comment out the whole if block to make test pass.  Need
		to fix.

		if !forceConvert {
			if countOfLineArray != handler.batchSize {
				//	logutil.Infof("---->countOfLineArray %d batchSize %d ",countOfLineArray,handler.batchSize)
				panic("-----write a batch")
			}
		}
	*/

	batchData := handler.batchData
	columnFLags := make([]byte, len(batchData.Vecs))
	fetchCnt := 0
	var err error
	allFetchCnt := 0

	row2col := time.Duration(0)
	fillBlank := time.Duration(0)
	toStorage := time.Duration(0)
	fetchCnt = countOfLineArray
	//logutil.Infof("-----fetchCnt %d len(lineArray) %d",fetchCnt,len(handler.simdCsvLineArray))
	fetchLines := handler.simdCsvLineArray[:fetchCnt]

	/*
		row to column
	*/

	batchBegin := handler.batchFilled
	ignoreFieldError := handler.ignoreFieldError
	result := handler.result

	//logutil.Infof("-----ignoreFieldError %v",handler.ignoreFieldError)
	if row2colChoose {
		wait_d := time.Now()
		for i, line := range fetchLines {
			//wait_a := time.Now()
			rowIdx := batchBegin + i
			offset := i + 1
			base := handler.lineCount - uint64(fetchCnt)
			//fmt.Println(line)
			//logutil.Infof("------ linecount %d fetchcnt %d base %d offset %d",
			//	handler.lineCount,fetchCnt,base,offset)
			//record missing column
			for k := 0; k < len(columnFLags); k++ {
				columnFLags[k] = 0
			}

			for j, lineStr := range line {
				//logutil.Infof("data col %d : %v",j,field)
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

				field := strings.TrimSpace(lineStr)

				isNullOrEmpty := len(field) == 0 || field == NULL_FLAG

				//put it into batch
				vec := batchData.Vecs[colIdx]
				vecAttr := batchData.Attrs[colIdx]

				//record colIdx
				columnFLags[colIdx] = 1

				//logutil.Infof("data set col %d : %v ",j,field)

				switch vec.Typ.Oid {
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
							return fmt.Errorf("the input value '%s' is not bool type", field)
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
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
							}
							cols[rowIdx] = int8(d)
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < math.MinInt8 || d > math.MaxInt8 {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
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
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
							}
							cols[rowIdx] = int16(d)
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < math.MinInt16 || d > math.MaxInt16 {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
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
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
							}
							cols[rowIdx] = int32(d)
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < math.MinInt32 || d > math.MaxInt32 {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
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
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
							}
							cols[rowIdx] = d
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < math.MinInt64 || d > math.MaxInt64 {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
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
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
							}
							cols[rowIdx] = uint8(d)
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < 0 || d > math.MaxUint8 {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
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
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
							}
							cols[rowIdx] = uint16(d)
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < 0 || d > math.MaxUint16 {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
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
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
							}
							cols[rowIdx] = uint32(d)
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < 0 || d > math.MaxUint32 {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
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
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = 0
							}
							cols[rowIdx] = d
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < 0 || d > math.MaxUint64 {
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
					}
				case types.T_float32:
					cols := vec.Col.([]float32)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp, uint64(rowIdx))
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
						nulls.Add(vec.Nsp, uint64(rowIdx))
					} else {
						fs := field
						//logutil.Infof("==== > field string [%s] ",fs)
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
						fs := field
						//logutil.Infof("==== > field string [%s] ",fs)
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
				case types.T_datetime:
					cols := vec.Col.([]types.Datetime)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp, uint64(rowIdx))
					} else {
						fs := field
						d, err := types.ParseDatetime(fs, vec.Typ.Precision)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
							}
							result.Warnings++
							d = 0
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
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = types.Decimal64_Zero
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
								if !ignoreFieldError {
									return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
								}
								result.Warnings++
								d = types.Decimal128_Zero
							}
						}
						cols[rowIdx] = d
					}
				case types.T_timestamp:
					cols := vec.Col.([]types.Timestamp)
					if isNullOrEmpty {
						nulls.Add(vec.Nsp, uint64(rowIdx))
					} else {
						fs := field
						d, err := types.ParseTimestamp(fs, vec.Typ.Precision)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return makeParsedFailedError(vec.Typ.String(), field, vecAttr, base, offset)
							}
							result.Warnings++
							d = types.Timestamp(0)
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
				if columnFLags[k] == 0 {
					vec := batchData.Vecs[k]
					switch vec.Typ.Oid {
					case types.T_char, types.T_varchar:
						vBytes := vec.Col.(*types.Bytes)
						vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
						vBytes.Lengths[rowIdx] = uint32(0)
					}
					nulls.Add(vec.Nsp, uint64(rowIdx))

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
			case types.T_bool:
				cols := vec.Col.([]bool)
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						if field == "true" || field == "1" {
							cols[i] = true
						} else if field == "false" || field == "0" {
							cols[i] = false
						} else {
							return fmt.Errorf("the input value '%s' is not bool type", field)
						}
					}
				}
			case types.T_int8:
				cols := vec.Col.([]int8)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						if judgeInterge(field) {
							d, err := strconv.ParseInt(field, 10, 8)
							if err != nil {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = int8(d)
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < math.MinInt8 || d > math.MaxInt8 {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = int8(d)
						}
					}
				}
			case types.T_int16:
				cols := vec.Col.([]int16)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						if judgeInterge(field) {
							d, err := strconv.ParseInt(field, 10, 16)
							if err != nil {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = int16(d)
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < math.MinInt16 || d > math.MaxInt16 {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = int16(d)
						}
					}
				}
			case types.T_int32:
				cols := vec.Col.([]int32)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						if judgeInterge(field) {
							d, err := strconv.ParseInt(field, 10, 32)
							if err != nil {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = int32(d)
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < math.MinInt32 || d > math.MaxInt32 {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = int32(d)
						}
					}
				}
			case types.T_int64:
				cols := vec.Col.([]int64)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						if judgeInterge(field) {
							d, err := strconv.ParseInt(field, 10, 64)
							if err != nil {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = d
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < math.MinInt64 || d > math.MaxInt64 {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = int64(d)
						}
					}
				}
			case types.T_uint8:
				cols := vec.Col.([]uint8)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						if judgeInterge(field) {
							d, err := strconv.ParseUint(field, 10, 8)
							if err != nil {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = uint8(d)
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < 0 || d > math.MaxUint8 {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = uint8(d)
						}
					}
				}
			case types.T_uint16:
				cols := vec.Col.([]uint16)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						if judgeInterge(field) {
							d, err := strconv.ParseUint(field, 10, 16)
							if err != nil {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = uint16(d)
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < 0 || d > math.MaxUint16 {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = uint16(d)
						}
					}
				}
			case types.T_uint32:
				cols := vec.Col.([]uint32)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						if judgeInterge(field) {
							d, err := strconv.ParseUint(field, 10, 32)
							if err != nil {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = uint32(d)
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < 0 || d > math.MaxUint32 {
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
				}
			case types.T_uint64:
				cols := vec.Col.([]uint64)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						if judgeInterge(field) {
							d, err := strconv.ParseUint(field, 10, 64)
							if err != nil {
								logutil.Errorf("parse field[%v] err:%v", field, err)
								if !ignoreFieldError {
									return err
								}
								result.Warnings++
								d = 0
							}
							cols[i] = d
						} else {
							d, err := strconv.ParseFloat(field, 64)
							if err != nil || d < 0 || d > math.MaxUint64 {
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
				}
			case types.T_float32:
				cols := vec.Col.([]float32)
				//row
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
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
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						//logutil.Infof("==== > field string [%s] ",fs)
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
						nulls.Add(vec.Nsp, uint64(i))
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
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						//logutil.Infof("==== > field string [%s] ",fs)
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
			case types.T_datetime:
				cols := vec.Col.([]types.Datetime)
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						//logutil.Infof("==== > field string [%s] ",fs)
						d, err := types.ParseDatetime(field, vec.Typ.Precision)
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
			case types.T_decimal64:
				cols := vec.Col.([]types.Decimal64)
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						//logutil.Infof("==== > field string [%s] ",fs)
						d, err := types.ParseStringToDecimal64(field, vec.Typ.Width, vec.Typ.Scale)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return err
							}
							result.Warnings++
							d = types.Decimal64_Zero
							//break
						}
						cols[i] = d
					}
				}
			case types.T_decimal128:
				cols := vec.Col.([]types.Decimal128)
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						//logutil.Infof("==== > field string [%s] ",fs)
						d, err := types.ParseStringToDecimal128(field, vec.Typ.Width, vec.Typ.Scale)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v", field, err)
							if !ignoreFieldError {
								return err
							}
							result.Warnings++
							d = types.Decimal128_Zero
							//break
						}
						cols[i] = d
					}
				}
			case types.T_timestamp:
				cols := vec.Col.([]types.Timestamp)
				for i := 0; i < countOfLineArray; i++ {
					line := fetchLines[i]
					if j >= len(line) || len(line[j]) == 0 {
						nulls.Add(vec.Nsp, uint64(i))
					} else {
						field := line[j]
						//logutil.Infof("==== > field string [%s] ",fs)
						d, err := types.ParseTimestamp(field, vec.Typ.Precision)
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
			if columnFLags[k] == 0 {
				vec := batchData.Vecs[k]
				//row
				for i := 0; i < countOfLineArray; i++ {
					switch vec.Typ.Oid {
					case types.T_char, types.T_varchar:
						vBytes := vec.Col.(*types.Bytes)
						vBytes.Offsets[i] = uint32(len(vBytes.Data))
						vBytes.Lengths[i] = uint32(0)
					}
					nulls.Add(vec.Nsp, uint64(i))
				}
			}
		}
		fillBlank += time.Since(wait_b)
		handler.choose_false += time.Since(wait_d)
	}

	handler.batchFilled = batchBegin + fetchCnt
	{
		handler.batchData.InitZsOne(handler.batchSize)
		handler.batchData.ExpandNulls()
	}

	//if handler.batchFilled == handler.batchSize {
	//	minLen := math.MaxInt64
	//	maxLen := 0
	//	for _, vec := range batchData.Vecs {
	//		logutil.Infof("len %d type %d %s ",vec.Length(),vec.Typ.Oid,vec.Typ.String())
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
	err = writeBatchToStorage(handler, forceConvert)

	toStorage += time.Since(wait_c)

	allFetchCnt += fetchCnt
	//}

	handler.row2col += row2col
	handler.fillBlank += fillBlank
	handler.toStorage += toStorage

	//logutil.Infof("----- row2col %s fillBlank %s toStorage %s",
	//	row2col,fillBlank,toStorage)

	if err != nil {
		logutil.Errorf("saveBatchToStorage failed. err:%v", err)
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
func writeBatchToStorage(handler *WriteBatchHandler, force bool) error {
	var err error = nil

	ctx := context.TODO()
	if handler.batchFilled == handler.batchSize {
		//batchBytes := 0
		//for _, vec := range handler.batchData.Vecs {
		//	//logutil.Infof("len %d type %d %s ",vec.Length(),vec.Typ.Oid,vec.Typ.String())
		//	switch vec.Typ.Oid {
		//	case types.T_char, types.T_varchar:
		//		vBytes := vec.Col.(*types.Bytes)
		//		batchBytes += len(vBytes.Data)
		//	default:
		//		batchBytes += vec.Length() * int(vec.Typ.Size)
		//	}
		//}
		//
		//logutil.Infof("----batchBytes %v B %v MB",batchBytes,batchBytes / 1024.0 / 1024.0)
		//
		wait_a := time.Now()
		handler.ThreadInfo.SetTime(wait_a)
		handler.ThreadInfo.SetCnt(1)
		//dbHandler := handler.dbHandler
		var dbHandler engine.Database
		var txnHandler *TxnHandler
		tableHandler := handler.tableHandler
		initSes := handler.ses
		tmpSes := NewSession(initSes.GetMysqlProtocol(), initSes.GuestMmu, initSes.Mempool, initSes.Pu, gSysVariables)
		if !handler.skipWriteBatch {
			if handler.oneTxnPerBatch {
				txnHandler = tmpSes.GetTxnHandler()
				dbHandler, err = tmpSes.GetStorage().Database(ctx, handler.dbName, engine.Snapshot(txnHandler.GetTxn().GetCtx()))
				if err != nil {
					goto handleError
				}
				tableHandler, err = dbHandler.Relation(ctx, handler.tableName)
				if err != nil {
					goto handleError
				}
			}
			err = tableHandler.Write(ctx, handler.batchData)
			if handler.oneTxnPerBatch {
				if err != nil {
					goto handleError
				}
				err = tmpSes.TxnCommitSingleStatement(nil)
				if err != nil {
					goto handleError
				}
			}
		}

	handleError:
		handler.ThreadInfo.SetCnt(0)
		if err == nil {
			handler.result.Records += uint64(handler.batchSize)
		} else if isWriteBatchTimeoutError(err) {
			logutil.Errorf("write failed. err: %v", err)
			handler.result.WriteTimeout += uint64(handler.batchSize)
			//clean timeout error
			err = nil
		} else {
			logutil.Errorf("write failed. err: %v", err)
			handler.result.Skipped += uint64(handler.batchSize)
		}

		if handler.oneTxnPerBatch && err != nil {
			err2 := tmpSes.TxnRollbackSingleStatement(nil)
			if err2 != nil {
				logutil.Errorf("rollback failed.error:%v", err2)
			}
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
					//logutil.Infof("needLen %d %d type %d %s ",needLen,i,vec.Typ.Oid,vec.Typ.String())
					//remove nulls.NUlls
					for j := uint64(handler.batchFilled); j < uint64(handler.batchSize); j++ {
						nulls.Del(vec.Nsp, j)
					}
					//remove row
					switch vec.Typ.Oid {
					case types.T_bool:
						cols := vec.Col.([]bool)
						vec.Col = cols[:needLen]
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
						//logutil.Infof("saveBatchToStorage before data %s ",vBytes.String())
						if len(vBytes.Offsets) > needLen {
							vec.Col = vBytes.Window(0, needLen)
						}

						//logutil.Infof("saveBatchToStorage after data %s ",vBytes.String())
					case types.T_date:
						cols := vec.Col.([]types.Date)
						vec.Col = cols[:needLen]
					case types.T_datetime:
						cols := vec.Col.([]types.Datetime)
						vec.Col = cols[:needLen]
					case types.T_decimal64:
						cols := vec.Col.([]types.Decimal64)
						vec.Col = cols[:needLen]
					case types.T_decimal128:
						cols := vec.Col.([]types.Decimal128)
						vec.Col = cols[:needLen]
					case types.T_timestamp:
						cols := vec.Col.([]types.Timestamp)
						vec.Col = cols[:needLen]
					}
				}

				//for _, vec := range handler.batchData.Vecs {
				//	logutil.Infof("len %d type %d %s ",vec.Length(),vec.Typ.Oid,vec.Typ.String())
				//}

				wait_a := time.Now()
				handler.ThreadInfo.SetTime(wait_a)
				handler.ThreadInfo.SetCnt(1)
				var txnHandler *TxnHandler
				tableHandler := handler.tableHandler
				// dbHandler := handler.dbHandler
				initSes := handler.ses
				tmpSes := NewSession(initSes.GetMysqlProtocol(), initSes.GuestMmu, initSes.Mempool, initSes.Pu, gSysVariables)
				var dbHandler engine.Database
				if !handler.skipWriteBatch {
					if handler.oneTxnPerBatch {
						txnHandler = tmpSes.GetTxnHandler()
						dbHandler, err = tmpSes.GetStorage().Database(ctx, handler.dbName, engine.Snapshot(txnHandler.GetTxn().GetCtx()))
						if err != nil {
							goto handleError2
						}
						//new relation
						tableHandler, err = dbHandler.Relation(ctx, handler.tableName)
						if err != nil {
							goto handleError2
						}
					}
					err = tableHandler.Write(ctx, handler.batchData)
					if handler.oneTxnPerBatch {
						if err != nil {
							goto handleError2
						}
						err = tmpSes.TxnCommitSingleStatement(nil)
						if err != nil {
							goto handleError2
						}
					}
				}
			handleError2:
				handler.ThreadInfo.SetCnt(0)
				if err == nil {
					handler.result.Records += uint64(needLen)
				} else if isWriteBatchTimeoutError(err) {
					logutil.Errorf("write failed. err: %v", err)
					handler.result.WriteTimeout += uint64(needLen)
					//clean timeout error
					err = nil
				} else {
					logutil.Errorf("write failed. err:%v \n", err)
					handler.result.Skipped += uint64(needLen)
				}

				if handler.oneTxnPerBatch && err != nil {
					err2 := tmpSes.TxnRollbackSingleStatement(nil)
					if err2 != nil {
						logutil.Errorf("rollback failed.error:%v", err2)
					}
				}
			}
		}
	}
	return err
}

//row2col algorithm
var row2colChoose bool = true

var saveLinesToStorage = func(handler *ParseLineHandler, force bool) error {
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
		err = rowToColumnAndSaveToStorage(writeHandler, force, row2colChoose)
		writeHandler.simdCsvErr = err

		releaseBatch(handler, writeHandler.pl)
		writeHandler.batchData = nil
		writeHandler.simdCsvLineArray = nil

		if err != nil {
			handler.simdCsvNotiyEventChan <- newNotifyEvent(NOTIFY_EVENT_WRITE_BATCH_ERROR, err, writeHandler)
		} else {
			handler.simdCsvNotiyEventChan <- newNotifyEvent(NOTIFY_EVENT_WRITE_BATCH_RESULT, nil, writeHandler)
		}
	}()
	return nil
}

func PrintThreadInfo(handler *ParseLineHandler, close *CloseFlag, a time.Duration) {
	for {
		if close.IsClosed() {
			logutil.Infof("load stream is over, start to leave.")
			return
		} else {
			for i, v := range handler.threadInfo {
				ret := v.GetTime()
				if ret == nil {
					continue
				} else {
					startTime := ret.(time.Time)
					threadCnt := v.GetCnt()
					if threadCnt == 1 {
						logutil.Infof("Print the ThreadInfo. id:%v, startTime:%v, spendTime:%v", i, startTime, time.Since(startTime))
					}
				}
			}
			time.Sleep(a * time.Second)
		}
	}
}

/*
LoadLoop reads data from stream, extracts the fields, and saves into the table
*/
func (mce *MysqlCmdExecutor) LoadLoop(load *tree.Load, dbHandler engine.Database, tableHandler engine.Relation, dbName string) (*LoadResult, error) {
	ses := mce.GetSession()

	var m sync.Mutex
	//begin:=  time.Now()
	//defer func() {
	//	logutil.Infof("-----load loop exit %s",time.Since(begin))
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
			storage:          ses.Pu.StorageEngine,
			dbHandler:        dbHandler,
			tableHandler:     tableHandler,
			tableName:        string(load.Table.Name()),
			dbName:           dbName,
			txnHandler:       ses.GetTxnHandler(),
			ses:              ses,
			oneTxnPerBatch:   ses.Pu.SV.GetOneTxnPerBatchDuringLoad(),
			lineCount:        0,
			batchSize:        curBatchSize,
			result:           result,
			skipWriteBatch:   ses.Pu.SV.GetLoadDataSkipWritingBatch(),
		},
		threadInfo:                    make(map[int]*ThreadInfo),
		simdCsvGetParsedLinesChan:     atomic.Value{},
		simdCsvWaitWriteRoutineToQuit: &sync.WaitGroup{},
	}
	handler.simdCsvGetParsedLinesChan.Store(make(chan simdcsv.LineOut, channelSize))

	handler.simdCsvConcurrencyCountOfWriteBatch = Min(int(ses.Pu.SV.GetLoadDataConcurrencyCount()), runtime.NumCPU())
	handler.simdCsvConcurrencyCountOfWriteBatch = Max(1, handler.simdCsvConcurrencyCountOfWriteBatch)
	handler.simdCsvBatchPool = make(chan *PoolElement, handler.simdCsvConcurrencyCountOfWriteBatch)
	for i := 0; i < handler.simdCsvConcurrencyCountOfWriteBatch; i++ {
		handler.threadInfo[i] = &ThreadInfo{}
	}

	//logutil.Infof("-----write concurrent count %d ",handler.simdCsvConcurrencyCountOfWriteBatch)

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

	//TODO: remove it after tae is ready
	if handler.oneTxnPerBatch {
		err = ses.TxnCommitSingleStatement(nil)
		if err != nil {
			return nil, err
		}
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

		m.Lock()
		defer m.Unlock()
		err := handler.simdCsvReader.ReadLoop(getLineOutChan(handler.simdCsvGetParsedLinesChan))
		if err != nil {
			handler.simdCsvNotiyEventChan <- newNotifyEvent(NOTIFY_EVENT_READ_SIMDCSV_ERROR, err, nil)
		}
		process_block += time.Since(wait_b)
	}()

	var statsWg sync.WaitGroup
	statsWg.Add(1)

	closechannel := CloseFlag{}
	var retErr error = nil
	go func() {
		defer statsWg.Done()
		/*
			collect statistics from every batch.
		*/
		var ne *notifyEvent = nil
		for {
			quit := false
			select {
			case <-handler.closeRef.stopLoadData:
				//get obvious cancel
				retErr = NewMysqlError(ER_QUERY_INTERRUPTED)
				quit = true
				//logutil.Infof("----- get stop in load ")

			case ne = <-handler.simdCsvNotiyEventChan:
				switch ne.neType {
				case NOTIFY_EVENT_WRITE_BATCH_RESULT:
					collectWriteBatchResult(handler, ne.wbh, nil)
				case NOTIFY_EVENT_END:
					retErr = nil
					quit = true
				case NOTIFY_EVENT_READ_SIMDCSV_ERROR,
					NOTIFY_EVENT_OUTPUT_SIMDCSV_ERROR,
					NOTIFY_EVENT_WRITE_BATCH_ERROR:
					if ses.IsTaeEngine() || !errorCanBeIgnored(ne.err) {
						retErr = ne.err
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
					m.Lock()
					defer m.Unlock()
					close(getLineOutChan(handler.simdCsvGetParsedLinesChan))
				})
				go func() {
					for closechannel.IsOpened() {
						select {
						case <-handler.simdCsvNotiyEventChan:
						default:
						}
					}
				}()
				break
			}
		}
	}()

	close := CloseFlag{}
	var a = time.Duration(ses.Pu.SV.GetPrintLogInterVal())
	go func() {
		PrintThreadInfo(handler, &close, a)
	}()

	//until now, the last writer has been counted.
	//There are no more new threads can be spawned.
	//wait csvReader and rowConverter to quit.
	wg.Wait()

	//until now, csvReader and rowConverter has quit.
	//wait writers to quit
	handler.simdCsvWaitWriteRoutineToQuit.Wait()

	//until now, all writers has quit.
	//tell stats to quit. NOTIFY_EVENT_END must be the last event in the queue.
	handler.simdCsvNotiyEventChan <- newNotifyEvent(NOTIFY_EVENT_END, nil, nil)

	//wait stats to quit
	statsWg.Wait()
	close.Close()
	closechannel.Close()

	return result, retErr
}

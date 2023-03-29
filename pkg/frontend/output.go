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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var _ outputPool = &outputQueue{}
var _ outputPool = &fakeOutputQueue{}

type outputQueue struct {
	ses          *Session
	ctx          context.Context
	proto        MysqlProtocol
	mrs          *MysqlResultSet
	rowIdx       uint64
	length       uint64
	ep           *ExportParam
	lineStr      []byte
	showStmtType ShowStatementType
}

func NewOutputQueue(ctx context.Context, ses *Session, columnCount int, mrs *MysqlResultSet, ep *ExportParam) *outputQueue {
	const countOfResultSet = 1
	if ctx == nil {
		ctx = ses.GetRequestContext()
	}
	if mrs == nil {
		//Create a new temporary result set per pipeline thread.
		mrs = &MysqlResultSet{}
		//Warning: Don't change ResultColumns in this.
		//Reference the shared ResultColumns of the session among multi-thread.
		sesMrs := ses.GetMysqlResultSet()
		mrs.Columns = sesMrs.Columns
		mrs.Name2Index = sesMrs.Name2Index

		//group row
		mrs.Data = make([][]interface{}, countOfResultSet)
		for i := 0; i < countOfResultSet; i++ {
			mrs.Data[i] = make([]interface{}, columnCount)
		}
	}

	if ep == nil {
		ep = ses.GetExportParam()
	}

	return &outputQueue{
		ctx:          ctx,
		ses:          ses,
		proto:        ses.GetMysqlProtocol(),
		mrs:          mrs,
		rowIdx:       0,
		length:       uint64(countOfResultSet),
		ep:           ep,
		showStmtType: ses.GetShowStmtType(),
	}
}

func (oq *outputQueue) resetLineStr() {
	oq.lineStr = oq.lineStr[:0]
}

func (oq *outputQueue) reset() {}

/*
getEmptyRow returns an empty space for filling data.
If there is no space, it flushes the data into the protocol
and returns an empty space then.
*/
func (oq *outputQueue) getEmptyRow() ([]interface{}, error) {
	if oq.rowIdx >= oq.length {
		if err := oq.flush(); err != nil {
			return nil, err
		}
	}

	row := oq.mrs.Data[oq.rowIdx]
	oq.rowIdx++
	return row, nil
}

/*
flush will force the data flushed into the protocol.
*/
func (oq *outputQueue) flush() error {
	if oq.rowIdx <= 0 {
		return nil
	}
	if oq.ep.Outfile {
		if err := exportDataToCSVFile(oq); err != nil {
			logErrorf(oq.ses.GetDebugString(), "export to csv file error %v", err)
			return err
		}
	} else {
		//send group of row
		if oq.showStmtType == ShowTableStatus {
			oq.rowIdx = 0
			return nil
		}

		if err := oq.proto.SendResultSetTextBatchRowSpeedup(oq.mrs, oq.rowIdx); err != nil {
			logErrorf(oq.ses.GetDebugString(), "flush error %v", err)
			return err
		}
	}
	oq.rowIdx = 0
	return nil
}

// extractRowFromEveryVector gets the j row from the every vector and outputs the row
func extractRowFromEveryVector(ses *Session, dataSet *batch.Batch, j int, oq outputPool) ([]interface{}, error) {
	row, err := oq.getEmptyRow()
	if err != nil {
		return nil, err
	}
	var rowIndex = j
	for i, vec := range dataSet.Vecs { //col index
		rowIndexBackup := rowIndex
		if vec.IsConstNull() {
			row[i] = nil
			continue
		}
		if vec.IsConst() {
			rowIndex = 0
		}

		err = extractRowFromVector(ses, vec, i, row, rowIndex)
		if err != nil {
			return nil, err
		}
		rowIndex = rowIndexBackup
	}
	//duplicate rows
	for i := int64(0); i < dataSet.Zs[j]-1; i++ {
		erow, rr := oq.getEmptyRow()
		if rr != nil {
			return nil, rr
		}
		for l := 0; l < len(dataSet.Vecs); l++ {
			erow[l] = row[l]
		}
	}
	return row, nil
}

// extractRowFromVector gets the rowIndex row from the i vector
func extractRowFromVector(ses *Session, vec *vector.Vector, i int, row []interface{}, rowIndex int) error {
	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(rowIndex)) {
		row[i] = nil
		return nil
	}

	switch vec.GetType().Oid { //get col
	case types.T_json:
		row[i] = types.DecodeJson(vec.GetBytesAt(rowIndex))
	case types.T_bool:
		row[i] = vector.GetFixedAt[bool](vec, rowIndex)
	case types.T_int8:
		row[i] = vector.GetFixedAt[int8](vec, rowIndex)
	case types.T_uint8:
		row[i] = vector.GetFixedAt[uint8](vec, rowIndex)
	case types.T_int16:
		row[i] = vector.GetFixedAt[int16](vec, rowIndex)
	case types.T_uint16:
		row[i] = vector.GetFixedAt[uint16](vec, rowIndex)
	case types.T_int32:
		row[i] = vector.GetFixedAt[int32](vec, rowIndex)
	case types.T_uint32:
		row[i] = vector.GetFixedAt[uint32](vec, rowIndex)
	case types.T_int64:
		row[i] = vector.GetFixedAt[int64](vec, rowIndex)
	case types.T_uint64:
		row[i] = vector.GetFixedAt[uint64](vec, rowIndex)
	case types.T_float32:
		val := vector.GetFixedAt[float32](vec, rowIndex)
		if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
			row[i] = val
		} else {
			row[i] = strconv.FormatFloat(float64(val), 'f', int(vec.GetType().Scale), 64)
		}
	case types.T_float64:
		val := vector.GetFixedAt[float64](vec, rowIndex)
		if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
			row[i] = val
		} else {
			row[i] = strconv.FormatFloat(val, 'f', int(vec.GetType().Scale), 64)
		}
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary:
		row[i] = vec.GetBytesAt(rowIndex)
	case types.T_date:
		row[i] = vector.GetFixedAt[types.Date](vec, rowIndex)
	case types.T_datetime:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Datetime](vec, rowIndex).String2(scale)
	case types.T_time:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Time](vec, rowIndex).String2(scale)
	case types.T_timestamp:
		scale := vec.GetType().Scale
		timeZone := ses.GetTimeZone()
		row[i] = vector.GetFixedAt[types.Timestamp](vec, rowIndex).String2(timeZone, scale)
	case types.T_decimal64:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Decimal64](vec, rowIndex).Format(scale)
	case types.T_decimal128:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Decimal128](vec, rowIndex).Format(scale)
	case types.T_uuid:
		row[i] = vector.GetFixedAt[types.Uuid](vec, rowIndex).ToString()
	case types.T_Rowid:
		row[i] = vector.GetFixedAt[types.Rowid](vec, rowIndex)
	default:
		logErrorf(ses.GetDebugString(), "extractRowFromVector : unsupported type %d", vec.GetType().Oid)
		return moerr.NewInternalError(ses.requestCtx, "extractRowFromVector : unsupported type %d", vec.GetType().Oid)
	}
	return nil
}

// fakeOutputQueue saves the data into the session.
type fakeOutputQueue struct {
	mrs *MysqlResultSet
}

func newFakeOutputQueue(mrs *MysqlResultSet) outputPool {
	return &fakeOutputQueue{mrs: mrs}
}

func (foq *fakeOutputQueue) resetLineStr() {}

func (foq *fakeOutputQueue) reset() {}

func (foq *fakeOutputQueue) getEmptyRow() ([]interface{}, error) {
	row := make([]interface{}, foq.mrs.GetColumnCount())
	foq.mrs.AddRow(row)
	return row, nil
}

func (foq *fakeOutputQueue) flush() error {
	return nil
}

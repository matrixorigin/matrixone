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

	"go.uber.org/zap"

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
	ep           *ExportConfig
	lineStr      []byte
	showStmtType ShowStatementType
}

func NewOutputQueue(ctx context.Context, ses *Session, columnCount int, mrs *MysqlResultSet, ep *ExportConfig) *outputQueue {
	const countOfResultSet = 1
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
		ep = ses.GetExportConfig()
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
	if oq.ep.needExportToFile() {
		if err := exportDataToCSVFile(oq); err != nil {
			logError(oq.ses, oq.ses.GetDebugString(),
				"Error occurred while exporting to CSV file",
				zap.Error(err))
			return err
		}
	} else {
		//send group of row
		if oq.showStmtType == ShowTableStatus {
			oq.rowIdx = 0
			return nil
		}

		if err := oq.proto.SendResultSetTextBatchRowSpeedup(oq.mrs, oq.rowIdx); err != nil {
			logError(oq.ses, oq.ses.GetDebugString(),
				"Flush error",
				zap.Error(err))
			return err
		}
	}
	oq.rowIdx = 0
	return nil
}

// extractRowFromEveryVector gets the j row from the every vector and outputs the row
// needCopyBytes : true -- make a copy of the bytes. else not.
// Case 1: needCopyBytes = false.
// For responding the client, we do not make a copy of the bytes. Because the data
// has been written into the tcp conn before the batch.Batch returned to the pipeline.
// Case 2: needCopyBytes = true.
// For the background execution, we need to make a copy of the bytes. Because the data
// has been saved in the session. Later the data will be used but then the batch.Batch has
// been returned to the pipeline and may be reused and changed by the pipeline.
func extractRowFromEveryVector(ctx context.Context, ses FeSession, dataSet *batch.Batch, j int, oq outputPool, needCopyBytes bool) ([]interface{}, error) {
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

		err = extractRowFromVector(ctx, ses, vec, i, row, rowIndex, needCopyBytes)
		if err != nil {
			return nil, err
		}
		rowIndex = rowIndexBackup
	}
	return row, nil
}

// extractRowFromVector gets the rowIndex row from the i vector
func extractRowFromVector(ctx context.Context, ses FeSession, vec *vector.Vector, i int, row []interface{}, rowIndex int, needCopyBytes bool) error {
	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(rowIndex)) {
		row[i] = nil
		return nil
	}

	switch vec.GetType().Oid { //get col
	case types.T_json:
		row[i] = types.DecodeJson(copyBytes(vec.GetBytesAt(rowIndex), needCopyBytes))
	case types.T_bool:
		row[i] = vector.GetFixedAt[bool](vec, rowIndex)
	case types.T_bit:
		row[i] = vector.GetFixedAt[uint64](vec, rowIndex)
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
		row[i] = vector.GetFixedAt[float32](vec, rowIndex)
	case types.T_float64:
		row[i] = vector.GetFixedAt[float64](vec, rowIndex)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary:
		row[i] = copyBytes(vec.GetBytesAt(rowIndex), needCopyBytes)
	case types.T_array_float32:
		// NOTE: Don't merge it with T_varchar. You will get raw binary in the SQL output
		//+------------------------------+
		//| abs(cast([1,2,3] as vecf32)) |
		//+------------------------------+
		//|   �?   @  @@                  |
		//+------------------------------+
		row[i] = vector.GetArrayAt[float32](vec, rowIndex)
	case types.T_array_float64:
		row[i] = vector.GetArrayAt[float64](vec, rowIndex)
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
	case types.T_Blockid:
		row[i] = vector.GetFixedAt[types.Blockid](vec, rowIndex)
	case types.T_TS:
		row[i] = vector.GetFixedAt[types.TS](vec, rowIndex)
	case types.T_enum:
		row[i] = copyBytes(vec.GetBytesAt(rowIndex), needCopyBytes)
	default:
		logError(ses, ses.GetDebugString(),
			"Failed to extract row from vector, unsupported type",
			zap.Int("typeID", int(vec.GetType().Oid)))
		return moerr.NewInternalError(ctx, "extractRowFromVector : unsupported type %d", vec.GetType().Oid)
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

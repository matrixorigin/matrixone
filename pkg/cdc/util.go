// Copyright 2022 Matrix Origin
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

package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

func NewMemQ[T any]() disttae.Queue[T] {
	return &memQ[T]{}
}

// TODO: add condition
type memQ[T any] struct {
	sync.Mutex
	data []T
}

func (mq *memQ[T]) Push(v T) {
	mq.Lock()
	defer mq.Unlock()
	mq.data = append(mq.data, v)
}

func (mq *memQ[T]) Pop() {
	mq.Lock()
	defer mq.Unlock()
	mq.data = mq.data[1:]
}

func (mq *memQ[T]) Front() T {
	mq.Lock()
	defer mq.Unlock()
	return mq.data[0]
}

func (mq *memQ[T]) Back() T {
	mq.Lock()
	defer mq.Unlock()
	return mq.data[len(mq.data)-1]
}

func (mq *memQ[T]) Size() int {
	mq.Lock()
	defer mq.Unlock()
	return len(mq.data)
}

func (mq *memQ[T]) Empty() bool {
	mq.Lock()
	defer mq.Unlock()
	return len(mq.data) == 0
}

// extractRowFromEveryVector gets the j row from the every vector and outputs the row
// skipFirstNCols denotes the first N columns should be skipped
func extractRowFromEveryVector(
	ctx context.Context,
	dataSet *batch.Batch,
	skipFirstNCols int,
	rowIndex int,
	row []any,
) error {
	for i, vec := range dataSet.Vecs { //col index
		if i < skipFirstNCols {
			continue
		}
		rowIndexBackup := rowIndex
		if vec.IsConstNull() {
			row[i] = nil
			continue
		}
		if vec.IsConst() {
			rowIndex = 0
		}

		err := extractRowFromVector(ctx, vec, i, row, rowIndex)
		if err != nil {
			return err
		}
		rowIndex = rowIndexBackup
	}
	return nil
}

// extractRowFromVector gets the rowIndex row from the i vector
func extractRowFromVector(ctx context.Context, vec *vector.Vector, i int, row []any, rowIndex int) error {
	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(rowIndex)) {
		row[i] = nil
		return nil
	}

	switch vec.GetType().Oid { //get col
	case types.T_json:
		row[i] = types.DecodeJson(copyBytes(vec.GetBytesAt(rowIndex)))
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
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		row[i] = copyBytes(vec.GetBytesAt(rowIndex))
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
		//TODO:get the right timezone
		//timeZone := ses.GetTimeZone()
		timeZone := time.UTC
		row[i] = vector.GetFixedAt[types.Timestamp](vec, rowIndex).String2(timeZone, scale)
	case types.T_decimal64:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Decimal64](vec, rowIndex).Format(scale)
	case types.T_decimal128:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Decimal128](vec, rowIndex).Format(scale)
	case types.T_uuid:
		row[i] = vector.GetFixedAt[types.Uuid](vec, rowIndex).String()
	case types.T_Rowid:
		row[i] = vector.GetFixedAt[types.Rowid](vec, rowIndex)
	case types.T_Blockid:
		row[i] = vector.GetFixedAt[types.Blockid](vec, rowIndex)
	case types.T_TS:
		row[i] = vector.GetFixedAt[types.TS](vec, rowIndex)
	case types.T_enum:
		row[i] = vector.GetFixedAt[types.Enum](vec, rowIndex)
	default:
		logutil.Errorf(
			"Failed to extract row from vector, unsupported type",
			zap.Int("typeID", int(vec.GetType().Oid)))
		return moerr.NewInternalError(ctx, "extractRowFromVector : unsupported type %d", vec.GetType().Oid)
	}
	return nil
}

func copyBytes(src []byte) []byte {
	if len(src) > 0 {
		dst := make([]byte, len(src))
		copy(dst, src)
		return dst
	} else {
		return []byte{}
	}
}

func convertColIntoSql(
	ctx context.Context,
	data any,
	typ *types.Type,
	sqlBuff []byte) ([]byte, error) {
	if data == nil {
		sqlBuff = appendString(sqlBuff, "NULL")
		return sqlBuff, nil
	}
	var temp string
	switch typ.Oid { //get col
	case types.T_json:
		temp = data.(bytejson.ByteJson).String()
		sqlBuff = appendString(sqlBuff, temp)
	case types.T_bool:
		b := data.(bool)
		if b {
			temp = "true"
		} else {
			temp = "false"
		}
		sqlBuff = appendString(sqlBuff, temp)
	case types.T_bit:
		value := data.(uint64)
		bitLength := typ.Width
		byteLength := (bitLength + 7) / 8
		b := types.EncodeUint64(&value)[:byteLength]
		slices.Reverse(b)
		sqlBuff = appendBytes(sqlBuff, b)
	case types.T_int8:
		value := data.(int8)
		sqlBuff = appendInt64(sqlBuff, int64(value))
	case types.T_uint8:
		value := data.(uint8)
		sqlBuff = appendUint64(sqlBuff, uint64(value))
	case types.T_int16:
		value := data.(int16)
		sqlBuff = appendInt64(sqlBuff, int64(value))
	case types.T_uint16:
		value := data.(uint16)
		sqlBuff = appendUint64(sqlBuff, uint64(value))
	case types.T_int32:
		value := data.(int32)
		sqlBuff = appendInt64(sqlBuff, int64(value))
	case types.T_uint32:
		value := data.(uint32)
		sqlBuff = appendUint64(sqlBuff, uint64(value))
	case types.T_int64:
		value := data.(int64)
		sqlBuff = appendInt64(sqlBuff, value)
	case types.T_uint64:
		value := data.(uint64)
		sqlBuff = appendUint64(sqlBuff, value)
	case types.T_float32:
		value := data.(float32)
		sqlBuff = appendFloat64(sqlBuff, float64(value), 32)
	case types.T_float64:
		value := data.(float64)
		sqlBuff = appendFloat64(sqlBuff, value, 64)
	case types.T_char,
		types.T_varchar,
		types.T_blob,
		types.T_text,
		types.T_binary,
		types.T_varbinary,
		types.T_datalink:
		value := data.([]byte)
		sqlBuff = appendByte(sqlBuff, '"')
		sqlBuff = appendBytes(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '"')
	case types.T_array_float32:
		// NOTE: Don't merge it with T_varchar. You will get raw binary in the SQL output
		//+------------------------------+
		//| abs(cast([1,2,3] as vecf32)) |
		//+------------------------------+
		//|   �?   @  @@                  |
		//+------------------------------+
		value := data.(float32)
		sqlBuff = appendFloat64(sqlBuff, float64(value), 32)
	case types.T_array_float64:
		value := data.(float64)
		sqlBuff = appendFloat64(sqlBuff, value, 64)
	case types.T_date:
		value := data.(types.Date)
		sqlBuff = appendByte(sqlBuff, '"')
		sqlBuff = appendString(sqlBuff, value.String())
		sqlBuff = appendByte(sqlBuff, '"')
	case types.T_datetime:
		value := data.(string)
		sqlBuff = appendByte(sqlBuff, '"')
		sqlBuff = appendString(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '"')
	case types.T_time:
		value := data.(string)
		sqlBuff = appendByte(sqlBuff, '"')
		sqlBuff = appendString(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '"')
	case types.T_timestamp:
		value := data.(string)
		sqlBuff = appendByte(sqlBuff, '"')
		sqlBuff = appendString(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '"')
	case types.T_decimal64:
		value := data.(string)
		sqlBuff = appendByte(sqlBuff, '"')
		sqlBuff = appendString(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '"')
	case types.T_decimal128:
		value := data.(string)
		sqlBuff = appendByte(sqlBuff, '"')
		sqlBuff = appendString(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '"')
	case types.T_uuid:
		value := data.(string)
		sqlBuff = appendByte(sqlBuff, '"')
		sqlBuff = appendString(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '"')
	case types.T_Rowid:
		value := data.(types.Rowid)
		sqlBuff = appendByte(sqlBuff, '"')
		sqlBuff = appendString(sqlBuff, value.String())
		sqlBuff = appendByte(sqlBuff, '"')
	case types.T_Blockid:
		value := data.(types.Blockid)
		sqlBuff = appendByte(sqlBuff, '"')
		sqlBuff = appendString(sqlBuff, value.String())
		sqlBuff = appendByte(sqlBuff, '"')
	case types.T_TS:
		value := data.(types.TS)
		sqlBuff = appendByte(sqlBuff, '"')
		sqlBuff = appendString(sqlBuff, value.ToString())
		sqlBuff = appendByte(sqlBuff, '"')
	case types.T_enum:
		value := data.(types.Enum)
		sqlBuff = appendByte(sqlBuff, '"')
		sqlBuff = appendString(sqlBuff, value.String())
		sqlBuff = appendByte(sqlBuff, '"')
	default:
		logutil.Errorf(
			"Failed to extract row from vector, unsupported type",
			zap.Int("typeID", int(typ.Oid)))
		return nil, moerr.NewInternalError(ctx, "extractRowFromVector : unsupported type %d", typ.Oid)
	}

	return sqlBuff, nil
}

func appendByte(buf []byte, d byte) []byte {
	return append(buf, d)
}

func appendBytes(buf []byte, data []byte) []byte {
	return append(buf, data...)
}

func appendString(buf []byte, s string) []byte {
	return appendBytes(buf, []byte(s))
}

func appendInt64(buf []byte, value int64) []byte {
	return strconv.AppendInt(buf, value, 10)
}

func appendUint64(buf []byte, value uint64) []byte {
	return strconv.AppendUint(buf, value, 10)
}

func appendFloat64(buf []byte, value float64, bitSize int) []byte {
	if !math.IsInf(value, 0) {
		buf = strconv.AppendFloat(buf, value, 'f', -1, bitSize)
	} else {
		if math.IsInf(value, 1) {
			buf = append(buf, []byte("+Infinity")...)
		} else {
			buf = append(buf, []byte("-Infinity")...)
		}
	}
	return buf
}

func openDbConn(
	user, password string,
	ip string,
	port int) (db *sql.DB, err error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?readTimeout=30s&timeout=30s&writeTimeout=30s",
		user,
		password,
		ip,
		port)
	for i := 0; i < 3; i++ {
		db, err = tryConn(dsn)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if err != nil {
		// TODO throw error instead of panic
		panic(err)
	}

	// TODO check table existence
	return
}

func tryConn(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	} else {
		db.SetConnMaxLifetime(time.Minute * 3)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		time.Sleep(time.Millisecond * 100)

		//ping opens the connection
		err = db.Ping()
		if err != nil {
			return nil, err
		}
	}
	return db, err
}

type ActiveRoutine struct {
	Cancel chan struct{}
	Pause  chan struct{}
}

func NewCdcActiveRoutine() *ActiveRoutine {
	activeRoutine := &ActiveRoutine{}
	activeRoutine.Cancel = make(chan struct{})
	activeRoutine.Pause = make(chan struct{})
	return activeRoutine
}

func TimestampToStr(ts timestamp.Timestamp) string {
	return fmt.Sprintf("%d-%d", ts.PhysicalTime, ts.LogicalTime)
}

func StrToTimestamp(tsStr string) (ts timestamp.Timestamp, err error) {
	splits := strings.Split(tsStr, "-")
	if len(splits) != 2 {
		err = moerr.NewInternalErrorNoCtx("strToTimestamp : invalid timestamp string %s", tsStr)
		return
	}

	if ts.PhysicalTime, err = strconv.ParseInt(splits[0], 10, 64); err != nil {
		return
	}

	logicalTime, err := strconv.ParseUint(splits[1], 10, 32)
	if err != nil {
		return
	}

	ts.LogicalTime = uint32(logicalTime)
	return
}

func updateWatermark(outputWMarkAtomic *atomic.Pointer[timestamp.Timestamp], curWMark timestamp.Timestamp) {
	outputWMarkPtr := outputWMarkAtomic.Load()
	if outputWMarkPtr == nil {
		return
	}
	if curWMark.Greater(*outputWMarkPtr) {
		*outputWMarkPtr = curWMark
	}
}

type SqlFile struct {
	file *os.File
}

func NewSqlFile(fname string) (*SqlFile, error) {
	ret := &SqlFile{}
	var err error
	ret.file, err = os.OpenFile("./tee", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (sfile SqlFile) Record(row []byte) error {
	_, err := sfile.file.Write(row)
	if err != nil {
		return err
	}
	_, err = sfile.file.WriteString("\n")
	if err != nil {
		return err
	}
	return err
}

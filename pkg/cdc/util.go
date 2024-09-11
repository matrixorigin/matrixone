// Copyright 2024 Matrix Origin
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
	"crypto/aes"
	"crypto/cipher"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// extractRowFromEveryVector gets the j row from the every vector and outputs the row
// bat columns layout:
// 1. data: user defined cols | cpk (if need) | commit-ts
// 2. tombstone: pk/cpk | commit-ts
// return user defined cols for data or only one cpk column for tombstone
func extractRowFromEveryVector(
	ctx context.Context,
	dataSet *batch.Batch,
	rowIndex int,
	row []any,
) error {
	for i := 0; i < len(row); i++ {
		vec := dataSet.Vecs[i]
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
		logutil.Error(
			"Failed to extract row from vector, unsupported type",
			zap.Int("typeID", int(vec.GetType().Oid)))
		return moerr.NewInternalErrorf(ctx, "extractRowFromVector : unsupported type %d", vec.GetType().Oid)
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
		sqlBuff = appendByte(sqlBuff, '\'')
		temp = data.(bytejson.ByteJson).String()
		sqlBuff = appendString(sqlBuff, temp)
		sqlBuff = appendByte(sqlBuff, '\'')
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
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendBytes(sqlBuff, b)
		sqlBuff = appendByte(sqlBuff, '\'')
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
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendBytes(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '\'')
	case types.T_array_float32:
		// NOTE: Don't merge it with T_varchar. You will get raw binary in the SQL output
		//+------------------------------+
		//| abs(cast([1,2,3] as vecf32)) |
		//+------------------------------+
		//|   �?   @  @@                  |
		//+------------------------------+
		value := data.([]float32)
		sqlBuff = appendString(sqlBuff, floatArrayToString(value))
	case types.T_array_float64:
		value := data.([]float64)
		sqlBuff = appendString(sqlBuff, floatArrayToString(value))
	case types.T_date:
		value := data.(types.Date)
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendString(sqlBuff, value.String())
		sqlBuff = appendByte(sqlBuff, '\'')
	case types.T_datetime:
		value := data.(string)
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendString(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '\'')
	case types.T_time:
		value := data.(string)
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendString(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '\'')
	case types.T_timestamp:
		value := data.(string)
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendString(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '\'')
	case types.T_decimal64:
		value := data.(string)
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendString(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '\'')
	case types.T_decimal128:
		value := data.(string)
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendString(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '\'')
	case types.T_uuid:
		value := data.(string)
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendString(sqlBuff, value)
		sqlBuff = appendByte(sqlBuff, '\'')
	case types.T_Rowid:
		value := data.(types.Rowid)
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendString(sqlBuff, value.String())
		sqlBuff = appendByte(sqlBuff, '\'')
	case types.T_Blockid:
		value := data.(types.Blockid)
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendString(sqlBuff, value.String())
		sqlBuff = appendByte(sqlBuff, '\'')
	case types.T_TS:
		value := data.(types.TS)
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendString(sqlBuff, value.ToString())
		sqlBuff = appendByte(sqlBuff, '\'')
	case types.T_enum:
		value := data.(types.Enum)
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendString(sqlBuff, value.String())
		sqlBuff = appendByte(sqlBuff, '\'')
	default:
		logutil.Error(
			"Failed to extract row from vector, unsupported type",
			zap.Int("typeID", int(typ.Oid)))
		return nil, moerr.NewInternalErrorf(ctx, "extractRowFromVector : unsupported type %d", typ.Oid)
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

func floatArrayToString[T float32 | float64](arr []T) string {
	str := "'["
	for i, v := range arr {
		if i == 0 {
			str += fmt.Sprintf("%f", v)
		} else {
			str += fmt.Sprintf(",%f", v)
		}
	}
	str += "]'"
	return str
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
		if db, err = tryConn(dsn); err == nil {
			// TODO check table existence
			return
		}
		time.Sleep(time.Second)
	}
	_, _ = fmt.Fprintf(os.Stderr, "^^^^^ openDbConn failed\n")
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

func GetTxnOp(
	ctx context.Context,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
) (client.TxnOperator, error) {
	nowTs := cnEngine.LatestLogtailAppliedTime()
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		"readMultipleTables",
		0)
	return cnTxnClient.New(ctx, nowTs, createByOpt)
}

func GetTableDef(
	ctx context.Context,
	txnOp client.TxnOperator,
	cnEngine engine.Engine,
	tblId uint64,
) (*plan.TableDef, error) {
	if err := cnEngine.New(ctx, txnOp); err != nil {
		return nil, err
	}

	_, _, rel, err := cnEngine.GetRelationById(ctx, txnOp, tblId)
	if err != nil {
		return nil, err
	}

	return rel.CopyTableDef(ctx), nil
}

func TrimSpace(values []string) []string {
	if len(values) == 0 {
		return values
	}
	ret := make([]string, 0)
	ForEach[string](values, func(v string) {
		res := strings.TrimSpace(v)
		if len(res) > 0 {
			ret = append(ret)
		}
	})
	return ret
}

func Deduplicate[T ~string | ~int](values []T) []T {
	if len(values) == 0 {
		return values
	}
	set := make(map[T]struct{})
	ForEach(values, func(val T) {
		if _, ok := set[val]; !ok {
			set[val] = struct{}{}
		}
	})
	ret := make([]T, 0)
	for key := range set {
		ret = append(ret, key)
	}
	return ret
}

func ForEach[T any](values []T, fn func(T)) {
	if len(values) == 0 || fn == nil {
		return
	}
	for _, value := range values {
		fn(value)
	}
}

func ForEachWithError[T any](values []T, fn func(T) error) error {
	if len(values) == 0 || fn == nil {
		return nil
	}
	for _, value := range values {
		err := fn(value)
		if err != nil {
			return err
		}
	}
	return nil
}

const (
	aesKey = "test-aes-key-not-use-it-in-cloud"
)

func AesCFBEncode(data []byte) (string, error) {
	return aesCFBEncode(data, []byte(aesKey))
}

func aesCFBEncode(data []byte, aesKey []byte) (string, error) {
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return "", err
	}

	encoded := make([]byte, aes.BlockSize+len(data))
	iv := encoded[:aes.BlockSize]
	salt := generate_salt(aes.BlockSize)
	copy(iv, salt)
	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(encoded[aes.BlockSize:], data)
	return hex.EncodeToString(encoded), nil
}

func AesCFBDecode(ctx context.Context, data string) (string, error) {
	return aesCFBDecode(ctx, data, []byte(aesKey))
}
func aesCFBDecode(ctx context.Context, data string, aesKey []byte) (string, error) {
	encodedData, err := hex.DecodeString(data)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return "", err
	}
	if len(encodedData) < aes.BlockSize {
		return "", moerr.NewInternalError(ctx, "encoded string is too short")
	}
	iv := encodedData[:aes.BlockSize]
	encodedData = encodedData[aes.BlockSize:]
	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(encodedData, encodedData)
	return string(encodedData), nil
}

func generate_salt(n int) []byte {
	buf := make([]byte, n)
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	r.Read(buf)
	for i := 0; i < n; i++ {
		buf[i] &= 0x7f
		if buf[i] == 0 || buf[i] == '$' {
			buf[i]++
		}
	}
	return buf
}

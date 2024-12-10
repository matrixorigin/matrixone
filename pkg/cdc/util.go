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
	cryptorand "crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// extractRowFromEveryVector gets the j row from the every vector and outputs the row
// bat columns layout:
// 1. data: user defined cols | cpk (if needed) | commit-ts
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

//func extractRowFromWantedVecs(
//	ctx context.Context,
//	dataSet *batch.Batch,
//	rowIndex int,
//	wantedVecIdxes []int,
//	row []any,
//) error {
//	for i := 0; i < len(row); i++ {
//		vec := dataSet.Vecs[wantedVecIdxes[i]]
//		rowIndexBackup := rowIndex
//		if vec.IsConstNull() {
//			row[i] = nil
//			continue
//		}
//		if vec.IsConst() {
//			rowIndex = 0
//		}
//
//		err := extractRowFromVector(ctx, vec, i, row, rowIndex)
//		if err != nil {
//			return err
//		}
//		rowIndex = rowIndexBackup
//	}
//	return nil
//}

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
		row[i] = vector.GetFixedAtWithTypeCheck[bool](vec, rowIndex)
	case types.T_bit:
		row[i] = vector.GetFixedAtWithTypeCheck[uint64](vec, rowIndex)
	case types.T_int8:
		row[i] = vector.GetFixedAtWithTypeCheck[int8](vec, rowIndex)
	case types.T_uint8:
		row[i] = vector.GetFixedAtWithTypeCheck[uint8](vec, rowIndex)
	case types.T_int16:
		row[i] = vector.GetFixedAtWithTypeCheck[int16](vec, rowIndex)
	case types.T_uint16:
		row[i] = vector.GetFixedAtWithTypeCheck[uint16](vec, rowIndex)
	case types.T_int32:
		row[i] = vector.GetFixedAtWithTypeCheck[int32](vec, rowIndex)
	case types.T_uint32:
		row[i] = vector.GetFixedAtWithTypeCheck[uint32](vec, rowIndex)
	case types.T_int64:
		row[i] = vector.GetFixedAtWithTypeCheck[int64](vec, rowIndex)
	case types.T_uint64:
		row[i] = vector.GetFixedAtWithTypeCheck[uint64](vec, rowIndex)
	case types.T_float32:
		row[i] = vector.GetFixedAtWithTypeCheck[float32](vec, rowIndex)
	case types.T_float64:
		row[i] = vector.GetFixedAtWithTypeCheck[float64](vec, rowIndex)
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
		row[i] = vector.GetFixedAtWithTypeCheck[types.Date](vec, rowIndex)
	case types.T_datetime:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAtWithTypeCheck[types.Datetime](vec, rowIndex).String2(scale)
	case types.T_time:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAtWithTypeCheck[types.Time](vec, rowIndex).String2(scale)
	case types.T_timestamp:
		scale := vec.GetType().Scale
		//TODO:get the right timezone
		//timeZone := ses.GetTimeZone()
		timeZone := time.UTC
		row[i] = vector.GetFixedAtWithTypeCheck[types.Timestamp](vec, rowIndex).String2(timeZone, scale)
	case types.T_decimal64:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAtWithTypeCheck[types.Decimal64](vec, rowIndex).Format(scale)
	case types.T_decimal128:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAtWithTypeCheck[types.Decimal128](vec, rowIndex).Format(scale)
	case types.T_uuid:
		row[i] = vector.GetFixedAtWithTypeCheck[types.Uuid](vec, rowIndex).String()
	case types.T_Rowid:
		row[i] = vector.GetFixedAtWithTypeCheck[types.Rowid](vec, rowIndex)
	case types.T_Blockid:
		row[i] = vector.GetFixedAtWithTypeCheck[types.Blockid](vec, rowIndex)
	case types.T_TS:
		row[i] = vector.GetFixedAtWithTypeCheck[types.TS](vec, rowIndex)
	case types.T_enum:
		row[i] = vector.GetFixedAtWithTypeCheck[types.Enum](vec, rowIndex)
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
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendString(sqlBuff, types.ArrayToString(value))
		sqlBuff = appendByte(sqlBuff, '\'')
	case types.T_array_float64:
		value := data.([]float64)
		sqlBuff = appendByte(sqlBuff, '\'')
		sqlBuff = appendString(sqlBuff, types.ArrayToString(value))
		sqlBuff = appendByte(sqlBuff, '\'')
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

//func getPkIdxesAndTypes(ctx context.Context, tableDef *plan.TableDef) (pkIdxes []int, pkAndTsTypes []*types.Type, err error) {
//	pkIdxes = make([]int, 0, len(tableDef.Pkey.Names))
//	pkAndTsTypes = make([]*types.Type, 0, len(tableDef.Pkey.Names)+1)
//	for _, colName := range tableDef.Pkey.Names {
//		idx, ok := tableDef.Name2ColIndex[colName]
//		if !ok {
//			err = moerr.NewInternalErrorf(ctx, "pk column %s not found", colName)
//			return
//		}
//
//		pkIdxes = append(pkIdxes, int(idx))
//
//		col := tableDef.Cols[idx]
//		pkAndTsTypes = append(pkAndTsTypes, &types.Type{
//			Oid:   types.T(col.Typ.Id),
//			Width: col.Typ.Width,
//			Scale: col.Typ.Scale,
//		})
//	}
//	pkAndTsTypes = append(pkAndTsTypes, &types.Type{
//		Oid:  types.T_TS,
//		Size: types.TxnTsSize,
//	})
//	return
//}
//
//// getPksFromBat gets the pk from the bat, pk is separated by ',' if it's a multi-column pk
//func getAllPkAndTsFromBat(
//	ctx context.Context,
//	bat *batch.Batch,
//	tableDef *plan.TableDef,
//	isDelete bool,
//) (pks []string, err error) {
//	if bat == nil || len(bat.Vecs) == 0 {
//		return
//	}
//
//	pkIdxes, pkAndTsTypes, err := getPkIdxesAndTypes(ctx, tableDef)
//	if err != nil {
//		return
//	}
//
//	// pk and ts
//	var wantedIdxes []int
//	if isDelete {
//		wantedIdxes = []int{0, 1}
//	} else {
//		wantedIdxes = append(pkIdxes, len(bat.Vecs)-1)
//	}
//	readRow := make([]any, len(wantedIdxes))
//	dataRow := make([]any, len(pkAndTsTypes))
//
//	pks = make([]string, bat.Vecs[0].Length())
//	for i := 0; i < len(pks); i++ {
//		if pks[i], err = getPkAndTsFromRow(ctx, bat, isDelete, i, pkAndTsTypes, wantedIdxes, readRow, dataRow); err != nil {
//			return
//		}
//	}
//	return
//}
//
//func getRowPkAndTsFromBat(
//	ctx context.Context,
//	bat *batch.Batch,
//	tableDef *plan.TableDef,
//	isDelete bool,
//	offset int,
//) (s string, err error) {
//	if bat == nil || len(bat.Vecs) == 0 || offset < 0 || offset >= bat.Vecs[0].Length() {
//		return
//	}
//
//	pkIdxes, pkAndTsTypes, err := getPkIdxesAndTypes(ctx, tableDef)
//	if err != nil {
//		return
//	}
//
//	// pk and ts
//	var wantedIdxes []int
//	if isDelete {
//		wantedIdxes = []int{0, 1}
//	} else {
//		wantedIdxes = append(pkIdxes, len(bat.Vecs)-1)
//	}
//	readRow := make([]any, len(wantedIdxes))
//	dataRow := make([]any, len(pkAndTsTypes))
//
//	return getPkAndTsFromRow(ctx, bat, isDelete, offset, pkAndTsTypes, wantedIdxes, readRow, dataRow)
//}
//
//func getPkAndTsFromRow(
//	ctx context.Context,
//	bat *batch.Batch,
//	isDelete bool,
//	offset int,
//	pkAndTsTypes []*types.Type,
//	wantedIdxes []int,
//	readRow []any,
//	dataRow []any,
//) (s string, err error) {
//	if err = extractRowFromWantedVecs(ctx, bat, offset, wantedIdxes, readRow); err != nil {
//		return
//	}
//
//	row := &readRow
//	if isDelete && len(pkAndTsTypes) > 2 {
//		// composite pk
//		var pkTuple types.Tuple
//		if pkTuple, _, err = unpackWithSchema(readRow[0].([]byte)); err != nil {
//			return
//		}
//
//		for j := range pkTuple {
//			dataRow[j] = pkTuple[j]
//		}
//		dataRow[len(pkTuple)] = readRow[1]
//
//		row = &dataRow
//	}
//
//	pkBytes := make([]byte, 0, 64)
//	for j := range pkAndTsTypes {
//		if j != 0 {
//			pkBytes = appendByte(pkBytes, ',')
//		}
//		if pkBytes, err = convertColIntoSql(ctx, (*row)[j], pkAndTsTypes[j], pkBytes); err != nil {
//			return
//		}
//	}
//	s = string(pkBytes)
//	return
//}

var OpenDbConn = func(user, password string, ip string, port int, timeout string) (db *sql.DB, err error) {
	logutil.Infof("openDbConn timeout = %s", timeout)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?readTimeout=%s&timeout=%s&writeTimeout=%s&multiStatements=true",
		user, password, ip, port, timeout, timeout, timeout)
	for i := 0; i < 3; i++ {
		if db, err = tryConn(dsn); err == nil {
			// TODO check table existence
			return
		}
		v2.CdcMysqlConnErrorCounter.Inc()
		time.Sleep(time.Second)
	}
	logutil.Error("cdc task OpenDbConn failed")
	return
}

var openDb = sql.Open

var tryConn = func(dsn string) (*sql.DB, error) {
	db, err := openDb("mysql-mo", dsn)
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

var GetTxnOp = func(
	ctx context.Context,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	info string,
) (client.TxnOperator, error) {
	nowTs := cnEngine.LatestLogtailAppliedTime()
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		info,
		0)
	return cnTxnClient.New(ctx, nowTs, createByOpt)
}

var GetTxn = func(
	ctx context.Context,
	cnEngine engine.Engine,
	txnOp client.TxnOperator,
) error {
	return cnEngine.New(ctx, txnOp)
}

var FinishTxnOp = func(ctx context.Context, inputErr error, txnOp client.TxnOperator, cnEngine engine.Engine) {
	//same timeout value as it in frontend
	ctx2, cancel := context.WithTimeoutCause(ctx, cnEngine.Hints().CommitOrRollbackTimeout, moerr.CauseFinishTxnOp)
	defer cancel()
	if inputErr != nil {
		_ = txnOp.Rollback(ctx2)
	} else {
		_ = txnOp.Commit(ctx2)
	}
}

var GetRelationById = func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator, tableId uint64) (dbName string, tblName string, rel engine.Relation, err error) {
	return cnEngine.GetRelationById(ctx, txnOp, tableId)
}

var GetSnapshotTS = func(txnOp client.TxnOperator) timestamp.Timestamp {
	return txnOp.SnapshotTS()
}

var CollectChanges = func(ctx context.Context, rel engine.Relation, fromTs, toTs types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
	return rel.CollectChanges(ctx, fromTs, toTs, mp)
}

var EnterRunSql = func(txnOp client.TxnOperator) {
	txnOp.EnterRunSql()
}

var ExitRunSql = func(txnOp client.TxnOperator) {
	txnOp.ExitRunSql()
}

var GetTableDef = func(
	ctx context.Context,
	txnOp client.TxnOperator,
	cnEngine engine.Engine,
	tblId uint64,
) (*plan.TableDef, error) {
	_, _, rel, err := cnEngine.GetRelationById(ctx, txnOp, tblId)
	if err != nil {
		return nil, err
	}

	return rel.CopyTableDef(ctx), nil
}

const (
	InitKeyId           = "4e3da275-5003-4ca0-8667-5d3cdbecdd35"
	InsertDataKeyFormat = "replace into mo_catalog.mo_data_key (account_id, key_id, encrypted_key) values (%d, '%s', '%s')"
)

var AesKey string

func AesCFBEncode(data []byte) (string, error) {
	return aesCFBEncodeWithKey(data, []byte(AesKey))
}

func aesCFBEncodeWithKey(data []byte, aesKey []byte) (string, error) {
	if len(aesKey) == 0 {
		return "", moerr.NewInternalErrorNoCtx("AesKey is not initialized")
	}

	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return "", err
	}

	encoded := make([]byte, aes.BlockSize+len(data))
	iv := encoded[:aes.BlockSize]
	salt := generateSalt(aes.BlockSize)
	copy(iv, salt)
	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(encoded[aes.BlockSize:], data)
	return hex.EncodeToString(encoded), nil
}

func AesCFBDecode(ctx context.Context, data string) (string, error) {
	return AesCFBDecodeWithKey(ctx, data, []byte(AesKey))
}

func AesCFBDecodeWithKey(ctx context.Context, data string, aesKey []byte) (string, error) {
	if len(aesKey) == 0 {
		return "", moerr.NewInternalErrorNoCtx("AesKey is not initialized")
	}

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

func generateSalt(n int) []byte {
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

var (
	encrypt        = aesCFBEncodeWithKey
	cryptoRandRead = cryptorand.Read
)

func GetInitDataKeySql(kek string) (_ string, err error) {
	aesKey := make([]byte, 32)
	if _, err = cryptoRandRead(aesKey); err != nil {
		return
	}

	encryptedKey, err := encrypt(aesKey, []byte(kek))
	if err != nil {
		return
	}

	return fmt.Sprintf(InsertDataKeyFormat, catalog.System_Account, InitKeyId, encryptedKey), nil
}

func batchRowCount(bat *batch.Batch) int {
	if bat == nil || len(bat.Vecs) == 0 {
		return 0
	}
	return bat.Vecs[0].Length()
}

// AddSingleQuotesJoin [a, b, c] -> 'a','b','c'
func AddSingleQuotesJoin(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return "'" + strings.Join(s, "','") + "'"
}

func GenDbTblKey(dbName, tblName string) string {
	return dbName + "." + tblName
}

func SplitDbTblKey(dbTblKey string) (dbName, tblName string) {
	s := strings.Split(dbTblKey, ".")
	if len(s) != 2 {
		return
	}
	return s[0], s[1]
}

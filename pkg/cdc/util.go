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
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/mysql"
)

// escapeSQLString escapes special characters in SQL string literals to prevent SQL injection.
//
// It follows the SQL standard escaping rules:
//  1. Single quotes (') are escaped as double single quotes (”)
//  2. Backslashes (\) are escaped as double backslashes (\\)
//
// This function is critical for security: it prevents SQL injection attacks by ensuring
// that user-provided strings (TaskId, DBName, TableName, ErrMsg) cannot break out of
// string literals and execute arbitrary SQL commands.
//
// Example:
//
//	Input:  "task'; DROP TABLE users; --"
//	Output: "task''; DROP TABLE users; --"
//	Result: The SQL will treat this as a literal string value, not as SQL commands
func escapeSQLString(s string) string {
	// Replace backslash first (before replacing quotes) to avoid double-escaping
	s = strings.ReplaceAll(s, `\`, `\\`)
	// Replace single quotes with double single quotes (SQL standard escaping)
	s = strings.ReplaceAll(s, "'", "''")
	return s
}

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

		if err := extractRowFromVector(ctx, vec, i, row, rowIndex); err != nil {
			return err
		}
		rowIndex = rowIndexBackup
	}
	return nil
}

// extractRowFromVector gets the rowIndex row from the i vector
func extractRowFromVector(ctx context.Context, vec *vector.Vector, i int, row []any, rowIndex int) error {
	if !vector.SupportsAny(vec.GetType().Oid) {
		logutil.Error(
			"Failed to extract row from vector, unsupported type",
			zap.Int("typeID", int(vec.GetType().Oid)))
		return moerr.NewInternalErrorf(ctx, "extractRowFromVector : unsupported type %d", vec.GetType().Oid)
	}
	row[i] = vector.ExportValue(vec, rowIndex)
	return nil
}

// convertColIntoSql renders a value as a SQL literal. Quoted text and JSON
// literals use backslash escapes, which are only valid when the parsing session
// does not enable NO_BACKSLASH_ESCAPES; makeMysqlConfig establishes that dialect
// on every sink session. Binary and bit values use SQL-mode-independent x'...'
// hex literals.
func convertColIntoSql(
	ctx context.Context,
	data any,
	typ *types.Type,
	sqlBuff []byte) ([]byte, error) {
	// a timestamp is written in UTC
	return types.AppendSQLLiteral(sqlBuff, *typ, data, time.UTC)
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

const openDbConnAttemptTimeout = 10 * time.Second

var OpenDbConn = func(
	ctx context.Context,
	user, password string,
	ip string,
	port int,
	timeout string,
) (db *sql.DB, err error) {
	logutil.Info("cdc.util.open_db_conn", zap.String("timeout", timeout))
	cfg, err := makeMysqlConfig(user, password, ip, port, timeout)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 3; i++ {
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, openDbConnAttemptTimeout)
		db, err = tryConn(attemptCtx, cfg)
		cancelAttempt()
		if err == nil {
			// The caller may lose lifecycle authority between tryConn's final
			// check and this publication point. Close instead of handing the
			// connection to an obsolete generation.
			if ctxErr := ctx.Err(); ctxErr != nil {
				if db != nil {
					_ = db.Close()
				}
				return nil, ctxErr
			}
			// TODO check table existence
			return
		}
		v2.CdcMysqlConnErrorCounter.Inc()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if i == 2 {
			break
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	logutil.Error("cdc.util.open_db_conn_failed", zap.Error(err))
	err = newRetryableConnectionError(err)
	return
}

func makeMysqlConfig(user, password string, ip string, port int, timeout string) (*mysql.Config, error) {
	timeoutDuration, err := time.ParseDuration(timeout)
	if err != nil {
		return nil, err
	}
	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(ip, strconv.Itoa(port))
	cfg.Timeout = timeoutDuration
	cfg.ReadTimeout = timeoutDuration
	cfg.WriteTimeout = timeoutDuration
	cfg.MultiStatements = true
	// The driver applies Params on every physical connection after the
	// handshake (and therefore after the server-side init_connect), so each
	// pooled or reconnected sink session uses the lexical dialect assumed by
	// convertColIntoSql.
	cfg.Params = map[string]string{"sql_mode": sinkSessionSQLModeExpr}
	return cfg, nil
}

// sinkSessionSQLModeExpr removes NO_BACKSLASH_ESCAPES from the sink session
// sql_mode and keeps every other mode unchanged. All SQL text on a CDC sink
// session is generated by CDC, whose string literals use backslash escapes
// (see convertColIntoSql); under NO_BACKSLASH_ESCAPES those escapes would be
// stored as data or would terminate the literal early. The comma wrapping
// matches only the whole mode name at any position without leaving an empty
// element behind.
const sinkSessionSQLModeExpr = "TRIM(BOTH ',' FROM REPLACE(CONCAT(',', @@SESSION.sql_mode, ','), ',NO_BACKSLASH_ESCAPES,', ','))"

var openDbWithConnector = sql.OpenDB

var tryConn = func(ctx context.Context, cfg *mysql.Config) (*sql.DB, error) {
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	db := openDbWithConnector(connector)
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	timer := time.NewTimer(time.Millisecond * 100)
	select {
	case <-ctx.Done():
		timer.Stop()
		_ = db.Close()
		return nil, ctx.Err()
	case <-timer.C:
	}

	//ping opens the connection
	err = db.PingContext(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	// Cancellation can race a successful ping. Do not publish a connection to a
	// generation that has already lost its lifecycle authority.
	if err = ctx.Err(); err != nil {
		_ = db.Close()
		return nil, err
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

var collectBoundaryHook struct {
	sync.RWMutex
	fn func(types.TS, types.TS)
}

// SetCDCCollectBoundaryHookForTest observes the actual source collection
// boundary used by a running CDC reader. It is intended for integration tests
// that must distinguish durable-checkpoint recovery from a replay from the
// original task creation boundary.
func SetCDCCollectBoundaryHookForTest(fn func(types.TS, types.TS)) (restore func()) {
	collectBoundaryHook.Lock()
	previous := collectBoundaryHook.fn
	collectBoundaryHook.fn = fn
	collectBoundaryHook.Unlock()
	return func() {
		collectBoundaryHook.Lock()
		collectBoundaryHook.fn = previous
		collectBoundaryHook.Unlock()
	}
}

var CollectChanges = func(ctx context.Context, rel engine.Relation, fromTs, toTs types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
	collectBoundaryHook.RLock()
	hook := collectBoundaryHook.fn
	collectBoundaryHook.RUnlock()
	if hook != nil {
		hook(fromTs, toTs)
	}
	return rel.CollectChanges(ctx, fromTs, toTs, true, mp)
}

func enterRunSQL(
	ctx context.Context,
	txnOp client.TxnOperator,
	sql string,
	rejectionAware bool,
) (func(), error) {
	if txnOp == nil {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, cancel := context.WithCancel(ctx)
	var (
		token uint64
		err   error
	)
	if rejectionAware {
		token, err = client.TryEnterRunSqlWithTokenAndSQL(txnOp, cancel, sql)
	} else {
		token = txnOp.EnterRunSqlWithTokenAndSQL(cancel, sql)
	}
	if err != nil {
		cancel()
		return func() {}, err
	}
	return func() {
		txnOp.ExitRunSqlWithToken(token)
		cancel()
	}, nil
}

// EnterRunSql preserves the original, non-rejecting API contract. Callers that
// need an admission error should use TryEnterRunSql.
var EnterRunSql = func(ctx context.Context, txnOp client.TxnOperator, sql string) func() {
	finish, _ := enterRunSQL(ctx, txnOp, sql, false)
	return finish
}

// TryEnterRunSql registers one SQL execution and reports admission rejection.
var TryEnterRunSql = func(ctx context.Context, txnOp client.TxnOperator, sql string) (func(), error) {
	return enterRunSQL(ctx, txnOp, sql, true)
}

var GetTableDef = func(
	ctx context.Context,
	txnOp client.TxnOperator,
	cnEngine engine.Engine,
	tblId uint64,
) (*plan.TableDef, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, time.Minute*5, moerr.CauseGetTableDef)
	defer cancel()
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

func AesCFBEncodeWithKey(data []byte, aesKey []byte) (string, error) {
	return aesCFBEncodeWithKey(data, aesKey)
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
	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(encoded[aes.BlockSize:], data)
	return hex.EncodeToString(encoded), nil
}

func AesCFBDecode(ctx context.Context, data string) (string, error) {
	return AesCFBDecodeWithKey(ctx, data, []byte(AesKey))
}

var AesCFBDecodeWithKey = func(ctx context.Context, data string, aesKey []byte) (string, error) {
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
	stream := cipher.NewCTR(block, iv)
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

// AddSingleQuotesJoin [a, b, c] -> 'a','b','c'
func AddSingleQuotesJoin(s []string) string {
	if len(s) == 0 {
		return ""
	}
	escaped := make([]string, len(s))
	for i, value := range s {
		escaped[i] = escapeSQLString(value)
	}
	return "'" + strings.Join(escaped, "','") + "'"
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

// uriHasPrefix
func uriHasPrefix(uri string, prefix string) bool {
	if len(uri) < len(prefix) || strings.ToLower(uri[:len(prefix)]) != prefix {
		return false
	}
	return true
}

/*
ExtractUriInfo extracts the uriInfo
return serialized uriInfo
*/
func ExtractUriInfo(
	ctx context.Context,
	uri string,
	uriPrefix string,
) (string, UriInfo, error) {
	ok, uriInfo := compositedUriInfo(uri, uriPrefix)
	if !ok {
		return "", UriInfo{}, moerr.NewInternalErrorf(ctx, "invalid uri format: %s", uri)
	}

	jsonUriInfo, err := JsonEncode(&uriInfo)
	if err != nil {
		return "", UriInfo{}, err
	}
	return jsonUriInfo, uriInfo, nil
}

// compositedUriInfo uri according to the format: mysql://root:111@127.0.0.1:6001
// if valid, return true and extracted info
func compositedUriInfo(uri string, uriPrefix string) (bool, UriInfo) {
	if !uriHasPrefix(uri, uriPrefix) {
		return false, UriInfo{}
	}
	//locate user password
	rest := uri[len(uriPrefix):]
	atIdx := strings.LastIndex(rest, "@")
	if atIdx <= 0 || atIdx == len(rest)-1 {
		return false, UriInfo{}
	}
	userInfo := rest[:atIdx]
	hostInfo := rest[atIdx+1:]

	colonIdx := strings.LastIndex(userInfo, ":")
	if colonIdx <= 0 || colonIdx == len(userInfo)-1 {
		return false, UriInfo{}
	}
	userName, err := url.PathUnescape(userInfo[:colonIdx])
	if err != nil || userName == "" {
		return false, UriInfo{}
	}
	passwordStart := len(uriPrefix) + colonIdx + 1
	passwordEnd := len(uriPrefix) + atIdx
	password, err := url.PathUnescape(uri[passwordStart:passwordEnd])
	if err != nil {
		return false, UriInfo{}
	}

	sep3 := strings.Split(hostInfo, ":")
	if len(sep3) != 2 || len(sep3[0]) == 0 || len(sep3[1]) == 0 {
		return false, UriInfo{}
	}
	ip := sep3[0]
	port := sep3[1]
	portInt32, err := strconv.ParseUint(port, 10, 32)
	if err != nil || portInt32 > 65535 {
		return false, UriInfo{}
	}
	return true, UriInfo{
		User:          userName,
		Password:      password,
		Ip:            ip,
		Port:          int(portInt32),
		PasswordStart: passwordStart,
		PasswordEnd:   passwordEnd,
	}
}

// ParseFrequencyToDuration parses a frequency string (e.g., "1h", "30m") to time.Duration
func ParseFrequencyToDuration(freq string) time.Duration {
	return parseFrequencyToDuration(freq)
}

func parseFrequencyToDuration(freq string) time.Duration {
	if strings.HasSuffix(freq, "h") {
		hours, _ := strconv.Atoi(strings.TrimSuffix(freq, "h"))
		return time.Duration(hours) * time.Hour
	}
	minutes, _ := strconv.Atoi(strings.TrimSuffix(freq, "m"))
	return time.Duration(minutes) * time.Minute
}

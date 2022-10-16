// Copyright 2021 - 2022 Matrix Origin
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

package moerr

import (
	"bytes"
	"context"
	"encoding"
	"encoding/gob"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/stack"
)

const MySQLDefaultSqlState = "HY000"

const (
	// 0 - 99 is OK.  They do not contain info, and are special handled
	// using a static instance, no alloc.
	Ok              uint16 = 0
	OkStopCurrRecur uint16 = 1
	OkExpectedEOF   uint16 = 2
	OkMax           uint16 = 99

	// 100 - 200 is Info
	ErrInfo     uint16 = 100
	ErrLoadInfo uint16 = 101

	// 100 - 200 is WARNING
	ErrWarn uint16 = 200
	// In some cases, for example, varchar(N), truncated is a warning instead error.
	ErrWarnDataTruncated uint16 = 201

	// Group 1: Internal errors
	ErrStart            uint16 = 20100
	ErrInternal         uint16 = 20101
	ErrNYI              uint16 = 20102
	ErrOOM              uint16 = 20103
	ErrQueryInterrupted uint16 = 20104
	ErrNotSupported     uint16 = 20105

	// Group 2: numeric and functions
	ErrDivByZero                   uint16 = 20200
	ErrOutOfRange                  uint16 = 20201
	ErrDataTruncated               uint16 = 20202
	ErrInvalidArg                  uint16 = 20203
	ErrTruncatedWrongValueForField uint16 = 20204

	// Group 3: invalid input
	ErrBadConfig           uint16 = 20300
	ErrInvalidInput        uint16 = 20301
	ErrSyntaxError         uint16 = 20302
	ErrParseError          uint16 = 20303
	ErrConstraintViolation uint16 = 20304
	ErrDuplicate           uint16 = 20305
	ErrRoleGrantedToSelf   uint16 = 20306

	// Group 4: unexpected state and io errors
	ErrInvalidState                 uint16 = 20400
	ErrLogServiceNotReady           uint16 = 20401
	ErrBadDB                        uint16 = 20402
	ErrNoSuchTable                  uint16 = 20403
	ErrEmptyVector                  uint16 = 20404
	ErrFileNotFound                 uint16 = 20405
	ErrFileAlreadyExists            uint16 = 20406
	ErrUnexpectedEOF                uint16 = 20407
	ErrEmptyRange                   uint16 = 20408
	ErrSizeNotMatch                 uint16 = 20409
	ErrNoProgress                   uint16 = 20410
	ErrInvalidPath                  uint16 = 20411
	ErrShortWrite                   uint16 = 20412
	ErrInvalidWrite                 uint16 = 20413
	ErrShortBuffer                  uint16 = 20414
	ErrNoDB                         uint16 = 20415
	ErrNoWorkingStore               uint16 = 20416
	ErrNoHAKeeper                   uint16 = 20417
	ErrInvalidTruncateLsn           uint16 = 20418
	ErrNotLeaseHolder               uint16 = 20419
	ErrDBAlreadyExists              uint16 = 20420
	ErrTableAlreadyExists           uint16 = 20421
	ErrNoService                    uint16 = 20422
	ErrDupServiceName               uint16 = 20423
	ErrWrongService                 uint16 = 20424
	ErrBadS3Config                  uint16 = 20425
	ErrBadView                      uint16 = 20426
	ErrInvalidTask                  uint16 = 20427
	ErrInvalidServiceIndex          uint16 = 20428
	ErrDragonboatTimeout            uint16 = 20429
	ErrDragonboatTimeoutTooSmall    uint16 = 20430
	ErrDragonboatInvalidDeadline    uint16 = 20431
	ErrDragonboatRejected           uint16 = 20432
	ErrDragonboatInvalidPayloadSize uint16 = 20433
	ErrDragonboatShardNotReady      uint16 = 20434
	ErrDragonboatSystemClosed       uint16 = 20435
	ErrDragonboatInvalidRange       uint16 = 20436
	ErrDragonboatShardNotFound      uint16 = 20437
	ErrDragonboatOtherSystemError   uint16 = 20438

	// Group 5: rpc timeout
	// ErrRPCTimeout rpc timeout
	ErrRPCTimeout uint16 = 20500
	// ErrClientClosed rpc client closed
	ErrClientClosed uint16 = 20501
	// ErrBackendClosed backend closed
	ErrBackendClosed uint16 = 20502
	// ErrStreamClosed rpc stream closed
	ErrStreamClosed uint16 = 20503
	// ErrNoAvailableBackend no available backend
	ErrNoAvailableBackend uint16 = 20504

	// Group 6: txn
	// ErrTxnAborted read and write a transaction that has been rolled back.
	ErrTxnClosed uint16 = 20600
	// ErrTxnWriteConflict write conflict error for concurrent transactions
	ErrTxnWriteConflict uint16 = 20601
	// ErrMissingTxn missing transaction error
	ErrMissingTxn uint16 = 20602
	// ErrUnresolvedConflict read transaction encounters unresolved data
	ErrUnresolvedConflict uint16 = 20603
	// ErrTxnError TxnError wrapper
	ErrTxnError uint16 = 20604
	// ErrDNShardNotFound DNShard not found, need to get the latest DN list from HAKeeper
	ErrDNShardNotFound  uint16 = 20605
	ErrShardNotReported uint16 = 20606
	// Generic TAE error
	ErrTAEError                  uint16 = 20607
	ErrTAERead                   uint16 = 20608
	ErrRpcError                  uint16 = 20609
	ErrWaitTxn                   uint16 = 20610
	ErrTxnNotFound               uint16 = 20611
	ErrTxnNotActive              uint16 = 20612
	ErrTAEWrite                  uint16 = 20613
	ErrTAECommit                 uint16 = 20614
	ErrTAERollback               uint16 = 20615
	ErrTAEPrepare                uint16 = 20616
	ErrTAEPossibleDuplicate      uint16 = 20617
	ErrTxnRWConflict             uint16 = 20618
	ErrTxnWWConflict             uint16 = 20619
	ErrNotFound                  uint16 = 20620
	ErrTxnInternal               uint16 = 20621
	ErrTxnReadConflict           uint16 = 20622
	ErrPrimaryKeyDuplicated      uint16 = 20623
	ErrAppendableSegmentNotFound uint16 = 20624
	ErrAppendableBlockNotFound   uint16 = 20625

	// ErrEnd, the max value of MOErrorCode
	ErrEnd uint16 = 65535
)

type moErrorMsgItem struct {
	mysqlCode        uint16
	sqlStates        []string
	errorMsgOrFormat string
}

var errorMsgRefer = map[uint16]moErrorMsgItem{
	// OK code not in this table.  They do not have a mysql code, as
	// they are OK -- should not leak back to client.

	// Info
	ErrInfo:     {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "info: %s"},
	ErrLoadInfo: {ER_LOAD_INFO, []string{MySQLDefaultSqlState}, "load info: %d, %d, %d, %d, %d"},

	// Warn
	ErrWarn:              {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "warning: %s"},
	ErrWarnDataTruncated: {WARN_DATA_TRUNCATED, []string{MySQLDefaultSqlState}, "warning: data trucncated"},

	// Group 1: Internal errors
	ErrStart:            {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "internal error: error code start"},
	ErrInternal:         {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "internal error: %s"},
	ErrNYI:              {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "%s is not yet implemented"},
	ErrOOM:              {ER_ENGINE_OUT_OF_MEMORY, []string{MySQLDefaultSqlState}, "error: out of memory"},
	ErrQueryInterrupted: {ER_QUERY_INTERRUPTED, []string{MySQLDefaultSqlState}, "query interrupted"},
	ErrNotSupported:     {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "not supported: %s"},

	// Group 2: numeric
	ErrDivByZero:                   {ER_DIVISION_BY_ZERO, []string{MySQLDefaultSqlState}, "division by zero"},
	ErrOutOfRange:                  {ER_DATA_OUT_OF_RANGE, []string{MySQLDefaultSqlState}, "data out of range: data type %s, %s"},
	ErrDataTruncated:               {ER_DATA_TOO_LONG, []string{MySQLDefaultSqlState}, "data truncated: data type %s, %s"},
	ErrInvalidArg:                  {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "invalid argument %s, bad value %s"},
	ErrTruncatedWrongValueForField: {ER_TRUNCATED_WRONG_VALUE_FOR_FIELD, []string{MySQLDefaultSqlState}, "truncated type %s value %s for column %s, %d"},

	// Group 3: invalid input
	ErrBadConfig:           {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "invalid configuration: %s"},
	ErrInvalidInput:        {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "invalid input: %s"},
	ErrSyntaxError:         {ER_SYNTAX_ERROR, []string{MySQLDefaultSqlState}, "SQL syntax error: %s"},
	ErrParseError:          {ER_PARSE_ERROR, []string{MySQLDefaultSqlState}, "SQL parser error: %s"},
	ErrConstraintViolation: {ER_CHECK_CONSTRAINT_VIOLATED, []string{MySQLDefaultSqlState}, "constraint violation: %s"},
	ErrDuplicate:           {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "tae data: duplicate"},
	ErrRoleGrantedToSelf:   {ER_ROLE_GRANTED_TO_ITSELF, []string{MySQLDefaultSqlState}, "cannot grant role %s to %s"},

	// Group 4: unexpected state or file io error
	ErrInvalidState:                 {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "invalid state %s"},
	ErrLogServiceNotReady:           {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "log service not ready"},
	ErrBadDB:                        {ER_BAD_DB_ERROR, []string{MySQLDefaultSqlState}, "invalid database %s"},
	ErrNoSuchTable:                  {ER_NO_SUCH_TABLE, []string{MySQLDefaultSqlState}, "no such table %s.%s"},
	ErrEmptyVector:                  {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "empty vector"},
	ErrFileNotFound:                 {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "file %s is not found"},
	ErrFileAlreadyExists:            {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "file %s alread exists"},
	ErrUnexpectedEOF:                {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "unexpteded end of file %s"},
	ErrEmptyRange:                   {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "empty range of file %s"},
	ErrSizeNotMatch:                 {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "file %s size does not match"},
	ErrNoProgress:                   {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "file %s has no io progress"},
	ErrInvalidPath:                  {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "invalid file path %s"},
	ErrShortWrite:                   {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "file %s io short write"},
	ErrInvalidWrite:                 {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "file %s io invalid write"},
	ErrShortBuffer:                  {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "file %s io short buffer"},
	ErrNoDB:                         {ER_NO_DB_ERROR, []string{MySQLDefaultSqlState}, "not connect to a database"},
	ErrNoWorkingStore:               {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "no working store"},
	ErrNoHAKeeper:                   {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "cannot locate ha keeper"},
	ErrInvalidTruncateLsn:           {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "invalid truncate lsn, shard %d already truncated to %d"},
	ErrNotLeaseHolder:               {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "not lease holder, current lease holder ID %d"},
	ErrDBAlreadyExists:              {ER_DB_CREATE_EXISTS, []string{MySQLDefaultSqlState}, "database %s already exists"},
	ErrTableAlreadyExists:           {ER_TABLE_EXISTS_ERROR, []string{MySQLDefaultSqlState}, "table %s already exists"},
	ErrNoService:                    {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "service %s not found"},
	ErrDupServiceName:               {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "duplicate service name %s"},
	ErrWrongService:                 {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "wrong service, expecting %s, got %s"},
	ErrBadS3Config:                  {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "bad s3 config: %s"},
	ErrBadView:                      {ER_VIEW_INVALID, []string{MySQLDefaultSqlState}, "invalid view %s"},
	ErrInvalidTask:                  {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "invalid task, task runner %s, id %d"},
	ErrInvalidServiceIndex:          {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "invalid service idx %d"},
	ErrDragonboatTimeout:            {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "%s"},
	ErrDragonboatTimeoutTooSmall:    {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "%s"},
	ErrDragonboatInvalidDeadline:    {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "%s"},
	ErrDragonboatRejected:           {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "%s"},
	ErrDragonboatInvalidPayloadSize: {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "%s"},
	ErrDragonboatShardNotReady:      {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "%s"},
	ErrDragonboatSystemClosed:       {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "%s"},
	ErrDragonboatInvalidRange:       {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "%s"},
	ErrDragonboatShardNotFound:      {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "%s"},
	ErrDragonboatOtherSystemError:   {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "%s"},

	// Group 5: rpc timeout
	ErrRPCTimeout:         {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "rpc timeout"},
	ErrClientClosed:       {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "client closed"},
	ErrBackendClosed:      {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "backend closed"},
	ErrStreamClosed:       {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "stream closed"},
	ErrNoAvailableBackend: {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "no available backend"},

	// Group 6: txn
	ErrTxnClosed:                 {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "the transaction has been committed or aborted"},
	ErrTxnWriteConflict:          {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "txn write conflict %s"},
	ErrMissingTxn:                {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "missing txn"},
	ErrUnresolvedConflict:        {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "unresolved conflict"},
	ErrTxnError:                  {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "transaction error: %s"},
	ErrDNShardNotFound:           {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "dn shard uuid %s, id %d not found"},
	ErrShardNotReported:          {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "dn shard uuid %s, id %d not reported"},
	ErrTAEError:                  {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "tae error %s"},
	ErrTAERead:                   {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "tae read error"},
	ErrRpcError:                  {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "rpc error"},
	ErrWaitTxn:                   {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "txn wait error"},
	ErrTxnNotFound:               {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "txn not found"},
	ErrTxnNotActive:              {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "txn not active, state %s"},
	ErrTAEWrite:                  {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "tae write error"},
	ErrTAECommit:                 {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "tae commit error %s"},
	ErrTAERollback:               {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "tae rollback error %s"},
	ErrTAEPrepare:                {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "tae prepare error %s"},
	ErrTAEPossibleDuplicate:      {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "tae possible duplicate"},
	ErrTxnRWConflict:             {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "r-w conflict"},
	ErrTxnWWConflict:             {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "w-w conflict"},
	ErrNotFound:                  {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "not found"},
	ErrTxnInternal:               {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "txn internal error"},
	ErrTxnReadConflict:           {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "txn read conflict %s"},
	ErrPrimaryKeyDuplicated:      {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "duplicated primary key %v"},
	ErrAppendableSegmentNotFound: {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "appendable segment not found"},
	ErrAppendableBlockNotFound:   {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "appendable block not found"},

	// Group End: max value of MOErrorCode
	ErrEnd: {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "internal error: end of errcode code"},
}

func newWithDepth(ctx context.Context, code uint16, args ...any) *Error {
	var err *Error
	// We should try to find the corresponding error information and error code in mysql_error_define, in order to be more compatible with MySQL.
	// you can customize moerr if you can't find it.
	if t, ok := MysqlErrorMsgRefer[uint16(code)]; ok {
		if len(args) == 0 {
			err = &Error{
				code:      code,
				mysqlCode: code,
				message:   t.ErrorMsgOrFormat,
				sqlState:  MysqlErrorMsgRefer[code].SqlStates[0],
			}
		} else {
			err = &Error{
				code:      code,
				mysqlCode: code,
				message:   fmt.Sprintf(t.ErrorMsgOrFormat, args...),
				sqlState:  MysqlErrorMsgRefer[code].SqlStates[0],
			}
		}
	} else {
		item, has := errorMsgRefer[code]
		if !has {
			panic(NewInternalError("not exist MOErrorCode: %d", code))
		}
		if len(args) == 0 {
			err = &Error{
				code:      code,
				mysqlCode: item.mysqlCode,
				message:   item.errorMsgOrFormat,
				sqlState:  item.sqlStates[0],
			}
		} else {
			err = &Error{
				code:      code,
				mysqlCode: item.mysqlCode,
				message:   fmt.Sprintf(item.errorMsgOrFormat, args...),
				sqlState:  item.sqlStates[0],
			}
		}
	}
	_ = errutil.WithContextWithDepth(ctx, err, 2)
	return err
}

type Error struct {
	code      uint16
	mysqlCode uint16
	message   string
	sqlState  string
}

func (e *Error) Error() string {
	return e.message
}

func (e *Error) ErrorCode() uint16 {
	return e.code
}

func (e *Error) MySQLCode() uint16 {
	return e.mysqlCode
}

func (e *Error) SqlState() string {
	return e.sqlState
}

type encodingErr struct {
	Code      uint16
	MysqlCode uint16
	Message   string
	SqlState  string
}

var _ encoding.BinaryMarshaler = new(Error)

func (e *Error) MarshalBinary() ([]byte, error) {
	ee := encodingErr{
		Code:      e.code,
		MysqlCode: e.mysqlCode,
		Message:   e.message,
		SqlState:  e.sqlState,
	}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(ee); err != nil {
		return nil, ConvertGoError(err)
	}
	return buf.Bytes(), nil
}

var _ encoding.BinaryUnmarshaler = new(Error)

func (e *Error) UnmarshalBinary(data []byte) error {
	var ee encodingErr
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&ee); err != nil {
		return ConvertGoError(err)
	}
	e.code = ee.Code
	e.mysqlCode = ee.MysqlCode
	e.message = ee.Message
	e.sqlState = ee.SqlState
	return nil
}

func IsMoErrCode(e error, rc uint16) bool {
	if e == nil {
		return rc == Ok
	}

	me, ok := e.(*Error)
	if !ok {
		// This is not a moerr
		return false
	}
	return me.code == rc
}

// ConvertPanicError converts a runtime panic to internal error.
func ConvertPanicError(v interface{}) *Error {
	if e, ok := v.(*Error); ok {
		return e
	}
	return newWithDepth(Context(), ErrInternal, fmt.Sprintf("panic %v: %+v", v, stack.Callers(3)))
}

// ConvertGoError converts a go error into mo error.
// Note here we must return error, because nil error
// is the same as nil *Error -- Go strangeness.
func ConvertGoError(err error) error {
	// nil is nil
	if err == nil {
		return err
	}

	// alread a moerr, return it as is
	if _, ok := err.(*Error); ok {
		return err
	}

	// Convert a few well known os/go error.
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		// if io.EOF reaches here, we believe it is not expected.
		return NewUnexpectedEOF(err.Error())
	}

	return NewInternalError("convert go error to mo error %v", err)
}

func (e *Error) Succeeded() bool {
	return e.code < OkMax
}

// Special handling of OK code.   This code are not errors, but used to
// signal different success conditions.  One user is StopCurrRecur.
// TAE use it to loop over memory data structures.  They are tight,
// performance critical loops, so we cannot afford to new an Error and
// definitely not construct call stack and do logging.  Note that exactly
// because of these, Ok code does not have any contextual info.  It is
// just a code.
//
// For these, we have a local var, and caller can use GetOkXXX() to get
// *Error.  The returned *Error can be tested with either
//
//	   if err == GetOkXXX()
//	or if moerr.IsMoErrCode(err, moerr.OkXXX)
//
// They are both fast, one with less typing and the other is consistent
// with other error code checking.
var errOkStopCurrRecur = Error{OkStopCurrRecur, 0, "StopCurrRecur", "00000"}
var errOkExptededEOF = Error{OkExpectedEOF, 0, "ExpectedEOF", "00000"}

/*
GetOk is useless in general, should just use nil.

var errOk = Error{Ok, 0, "Succeeded", "00000"}
func GetOk() *Error {
	return &errOk
}
*/

func GetOkStopCurrRecur() *Error {
	return &errOkStopCurrRecur
}

func GetOkExpectedEOF() *Error {
	return &errOkExptededEOF
}

func NewInfo(msg string) *Error {
	return newWithDepth(Context(), ErrInfo, msg)
}

func NewLoadInfo(rec, del, skip, warn, writeTimeOut uint64) *Error {
	return newWithDepth(Context(), ErrLoadInfo, rec, del, skip, warn, writeTimeOut)
}

func NewWarn(msg string) *Error {
	return newWithDepth(Context(), ErrWarn, msg)
}

func NewBadS3Config(msg string) *Error {
	return newWithDepth(Context(), ErrBadS3Config, msg)
}

func NewInternalError(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrInternal, xmsg)
}

func NewNYI(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrNYI, xmsg)
}

func NewNotSupported(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrNotSupported, xmsg)
}

func NewOOM() *Error {
	return newWithDepth(Context(), ErrOOM)
}

func NewQueryInterrupted() *Error {
	return newWithDepth(Context(), ErrQueryInterrupted)
}

func NewDivByZero() *Error {
	return newWithDepth(Context(), ErrDivByZero)
}

func NewOutOfRange(typ string, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrOutOfRange, typ, xmsg)
}

func NewDataTruncated(typ string, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrDataTruncated, typ, xmsg)
}

func NewInvalidArg(arg string, val any) *Error {
	return newWithDepth(Context(), ErrInvalidArg, arg, fmt.Sprintf("%v", val))
}

func NewTruncatedValueForField(t, v, c string, idx int) *Error {
	return newWithDepth(Context(), ErrTruncatedWrongValueForField, t, v, c, idx)
}

func NewBadConfig(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrBadConfig, xmsg)
}

func NewInvalidInput(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrInvalidInput, xmsg)
}

func NewSyntaxError(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrSyntaxError, xmsg)
}

func NewParseError(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrParseError, xmsg)
}

func NewConstraintViolation(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrConstraintViolation, xmsg)
}

func NewEmptyVector() *Error {
	return newWithDepth(Context(), ErrEmptyVector)
}

func NewFileNotFound(f string) *Error {
	return newWithDepth(Context(), ErrFileNotFound, f)
}

func NewFileAlreadyExists(f string) *Error {
	return newWithDepth(Context(), ErrFileAlreadyExists, f)
}

func NewDBAlreadyExists(db string) *Error {
	return newWithDepth(Context(), ErrDBAlreadyExists, db)
}

func NewTableAlreadyExists(t string) *Error {
	return newWithDepth(Context(), ErrTableAlreadyExists, t)
}

func NewUnexpectedEOF(f string) *Error {
	return newWithDepth(Context(), ErrUnexpectedEOF, f)
}

func NewEmptyRange(f string) *Error {
	return newWithDepth(Context(), ErrEmptyRange, f)
}

func NewSizeNotMatch(f string) *Error {
	return newWithDepth(Context(), ErrSizeNotMatch, f)
}

func NewNoProgress(f string) *Error {
	return newWithDepth(Context(), ErrNoProgress, f)
}

func NewInvalidPath(f string) *Error {
	return newWithDepth(Context(), ErrInvalidPath, f)
}

func NewInvalidState(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrInvalidState, xmsg)
}

func NewInvalidTask(runner string, id uint64) *Error {
	return newWithDepth(Context(), ErrInvalidTask, runner, id)
}

func NewInvalidServiceIndex(idx int) *Error {
	return newWithDepth(Context(), ErrInvalidServiceIndex, idx)
}

func NewLogServiceNotReady() *Error {
	return newWithDepth(Context(), ErrLogServiceNotReady)
}

func NewBadDB(name string) *Error {
	return newWithDepth(Context(), ErrBadDB, name)
}

func NewNoDB() *Error {
	return newWithDepth(Context(), ErrNoDB)
}

func NewNoWorkingStore() *Error {
	return newWithDepth(Context(), ErrNoWorkingStore)
}

func NewNoService(name string) *Error {
	return newWithDepth(Context(), ErrNoService, name)
}

func NewDupServiceName(name string) *Error {
	return newWithDepth(Context(), ErrDupServiceName, name)
}

func NewWrongService(exp, got string) *Error {
	return newWithDepth(Context(), ErrWrongService, exp, got)
}

func NewNoHAKeeper() *Error {
	return newWithDepth(Context(), ErrNoHAKeeper)
}

func NewInvalidTruncateLsn(shardId, idx uint64) *Error {
	return newWithDepth(Context(), ErrInvalidTruncateLsn, shardId, idx)
}

func NewNotLeaseHolder(holderId uint64) *Error {
	return newWithDepth(Context(), ErrNotLeaseHolder, holderId)
}

func NewNoSuchTable(db, tbl string) *Error {
	return newWithDepth(Context(), ErrNoSuchTable, db, tbl)
}

func NewBadView(db, v string) *Error {
	return newWithDepth(Context(), ErrBadView, db, v)
}

func NewRPCTimeout() *Error {
	return newWithDepth(Context(), ErrRPCTimeout)
}

func NewClientClosed() *Error {
	return newWithDepth(Context(), ErrClientClosed)
}

func NewBackendClosed() *Error {
	return newWithDepth(Context(), ErrBackendClosed)
}

func NewStreamClosed() *Error {
	return newWithDepth(Context(), ErrStreamClosed)
}

func NewNoAvailableBackend() *Error {
	return newWithDepth(Context(), ErrNoAvailableBackend)
}

func NewTxnClosed() *Error {
	return newWithDepth(Context(), ErrTxnClosed)
}

func NewTxnWriteConflict(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrTxnWriteConflict, xmsg)
}

func NewMissingTxn() *Error {
	return newWithDepth(Context(), ErrMissingTxn)
}

func NewUnresolvedConflict() *Error {
	return newWithDepth(Context(), ErrUnresolvedConflict)
}

func NewTxnError(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrTxnError, xmsg)
}

func NewTAEError(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrTAEError, xmsg)
}

func NewDNShardNotFound(uuid string, id uint64) *Error {
	return newWithDepth(Context(), ErrDNShardNotFound, uuid, id)
}

func NewShardNotReported(uuid string, id uint64) *Error {
	return newWithDepth(Context(), ErrShardNotReported, uuid, id)
}

func NewDragonboatTimeout(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrDragonboatTimeout, xmsg)
}

func NewDragonboatTimeoutTooSmall(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrDragonboatTimeoutTooSmall, xmsg)
}

func NewDragonboatInvalidDeadline(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrDragonboatInvalidDeadline, xmsg)
}

func NewDragonboatRejected(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrDragonboatRejected, xmsg)
}

func NewDragonboatInvalidPayloadSize(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrDragonboatInvalidPayloadSize, xmsg)
}

func NewDragonboatShardNotReady(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrDragonboatShardNotReady, xmsg)
}

func NewDragonboatSystemClosed(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrDragonboatSystemClosed, xmsg)
}

func NewDragonboatInvalidRange(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrDragonboatInvalidRange, xmsg)
}

func NewDragonboatShardNotFound(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrDragonboatShardNotFound, xmsg)
}

func NewDragonboatOtherSystemError(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrDragonboatOtherSystemError, xmsg)
}

func NewTAERead() *Error {
	return newWithDepth(Context(), ErrTAERead)
}

func NewRpcError() *Error {
	return newWithDepth(Context(), ErrRpcError)
}

func NewWaitTxn() *Error {
	return newWithDepth(Context(), ErrWaitTxn)
}

func NewTxnNotFound() *Error {
	return newWithDepth(Context(), ErrTxnNotFound)
}

func NewTxnNotActive(st string) *Error {
	return newWithDepth(Context(), ErrTxnNotActive, st)
}

func NewTAEWrite() *Error {
	return newWithDepth(Context(), ErrTAEWrite)
}

func NewTAECommit(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrTAECommit, xmsg)
}

func NewTAERollback(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrTAERollback, xmsg)
}

func NewTAEPrepare(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrTAEPrepare, xmsg)
}

func NewTAEPossibleDuplicate() *Error {
	return newWithDepth(Context(), ErrTAEPossibleDuplicate)
}

func NewTxnRWConflict() *Error {
	return newWithDepth(Context(), ErrTxnRWConflict)
}

func NewTxnWWConflict() *Error {
	return newWithDepth(Context(), ErrTxnWWConflict)
}

func NewNotFound() *Error {
	return newWithDepth(Context(), ErrNotFound)
}

func NewDuplicate() *Error {
	return newWithDepth(Context(), ErrDuplicate)
}

func NewRoleGrantedToSelf(from, to string) *Error {
	return newWithDepth(Context(), ErrRoleGrantedToSelf, from, to)
}

func NewTxnInternal() *Error {
	return newWithDepth(Context(), ErrTxnInternal)
}

func NewTxnReadConflict(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newWithDepth(Context(), ErrTxnReadConflict, xmsg)
}

func NewPrimaryKeyDuplicated(k any) *Error {
	return newWithDepth(Context(), ErrPrimaryKeyDuplicated, k)
}

func NewAppendableSegmentNotFound() *Error {
	return newWithDepth(Context(), ErrAppendableSegmentNotFound)
}

func NewAppendableBlockNotFound() *Error {
	return newWithDepth(Context(), ErrAppendableBlockNotFound)
}

var contextFunc atomic.Value

func SetContextFunc(f func() context.Context) {
	contextFunc.Store(f)
}

// Context should be trace.DefaultContext
func Context() context.Context {
	return contextFunc.Load().(func() context.Context)()
}

func init() {
	SetContextFunc(func() context.Context { return context.Background() })
}

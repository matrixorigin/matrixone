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
	"context"
	"encoding"
	"encoding/hex"
	"fmt"
	"io"
	"sync/atomic"

	moerrpb "github.com/matrixorigin/matrixone/pkg/pb/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/stack"
)

const MySQLDefaultSqlState = "HY000"

const (
	// 0 - 99 is OK.  They do not contain info, and are special handled
	// using a static instance, no alloc.
	Ok              uint16 = 0
	OkStopCurrRecur uint16 = 1
	OkExpectedEOF   uint16 = 2 // Expected End Of File
	OkExpectedEOB   uint16 = 3 // Expected End of Batch
	OkExpectedDup   uint16 = 4 // Expected Duplicate

	OkExpectedPossibleDup uint16 = 5 // Expected Possible Duplicate

	OkMax uint16 = 99

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
	ErrBadConfig            uint16 = 20300
	ErrInvalidInput         uint16 = 20301
	ErrSyntaxError          uint16 = 20302
	ErrParseError           uint16 = 20303
	ErrConstraintViolation  uint16 = 20304
	ErrDuplicate            uint16 = 20305
	ErrRoleGrantedToSelf    uint16 = 20306
	ErrDuplicateEntry       uint16 = 20307
	ErrWrongValueCountOnRow uint16 = 20308
	ErrBadFieldError        uint16 = 20309

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
	ErrDropNonExistsDB              uint16 = 20439
	ErrResultFileNotFound           uint16 = 20440
	ErrFunctionAlreadyExists        uint16 = 20441
	ErrDropNonExistsFunction        uint16 = 20442
	ErrNoConfig                     uint16 = 20443
	ErrNoSuchSequence               uint16 = 20444

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
	// ErrBackendCannotConnect can not connect to remote backend
	ErrBackendCannotConnect uint16 = 20505

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
	ErrTAEDebug                  uint16 = 20626
	ErrDuplicateKey              uint16 = 20626

	// Group 7: lock service
	// ErrDeadLockDetected lockservice has detected a deadlock and should abort the transaction if it receives this error
	ErrDeadLockDetected uint16 = 20701
	// ErrLockTableBindChanged lockservice and lock table bind changed
	ErrLockTableBindChanged uint16 = 20702
	// ErrLockTableNotFound lock table not found on remote lock service instance
	ErrLockTableNotFound uint16 = 20703

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
	ErrWarnDataTruncated: {WARN_DATA_TRUNCATED, []string{MySQLDefaultSqlState}, "warning: data truncated"},

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
	ErrBadConfig:            {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "invalid configuration: %s"},
	ErrInvalidInput:         {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "invalid input: %s"},
	ErrSyntaxError:          {ER_SYNTAX_ERROR, []string{MySQLDefaultSqlState}, "SQL syntax error: %s"},
	ErrParseError:           {ER_PARSE_ERROR, []string{MySQLDefaultSqlState}, "SQL parser error: %s"},
	ErrConstraintViolation:  {ER_CHECK_CONSTRAINT_VIOLATED, []string{MySQLDefaultSqlState}, "constraint violation: %s"},
	ErrDuplicate:            {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "tae data: duplicate"},
	ErrRoleGrantedToSelf:    {ER_ROLE_GRANTED_TO_ITSELF, []string{MySQLDefaultSqlState}, "cannot grant role %s to %s"},
	ErrDuplicateEntry:       {ER_DUP_ENTRY, []string{MySQLDefaultSqlState}, "Duplicate entry '%s' for key '%s'"},
	ErrWrongValueCountOnRow: {ER_WRONG_VALUE_COUNT_ON_ROW, []string{MySQLDefaultSqlState}, "Column count doesn't match value count at row %d"},
	ErrBadFieldError:        {ER_BAD_FIELD_ERROR, []string{MySQLDefaultSqlState}, "Unknown column '%s' in '%s'"},

	// Group 4: unexpected state or file io error
	ErrInvalidState:                 {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "invalid state %s"},
	ErrLogServiceNotReady:           {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "log service not ready"},
	ErrBadDB:                        {ER_BAD_DB_ERROR, []string{MySQLDefaultSqlState}, "invalid database %s"},
	ErrNoSuchTable:                  {ER_NO_SUCH_TABLE, []string{MySQLDefaultSqlState}, "no such table %s.%s"},
	ErrNoSuchSequence:               {ER_NO_SUCH_TABLE, []string{MySQLDefaultSqlState}, "no such sequence %s.%s"},
	ErrEmptyVector:                  {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "empty vector"},
	ErrFileNotFound:                 {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "file %s is not found"},
	ErrFileAlreadyExists:            {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "file %s already exists"},
	ErrUnexpectedEOF:                {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "unexpected end of file %s"},
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
	ErrFunctionAlreadyExists:        {ER_UDF_ALREADY_EXISTS, []string{MySQLDefaultSqlState}, "function %s already exists"},
	ErrDropNonExistsFunction:        {ER_CANT_FIND_UDF, []string{MySQLDefaultSqlState}, "function %s doesn't exist"},
	ErrNoService:                    {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "service %s not found"},
	ErrDupServiceName:               {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "duplicate service name %s"},
	ErrWrongService:                 {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "wrong service, expecting %s, got %s"},
	ErrBadS3Config:                  {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "bad s3 config: %s"},
	ErrBadView:                      {ER_VIEW_INVALID, []string{MySQLDefaultSqlState}, "invalid view '%s.%s'"},
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
	ErrDropNonExistsDB:              {ER_DB_DROP_EXISTS, []string{MySQLDefaultSqlState}, "Can't drop database '%s'; database doesn't exist"},
	ErrResultFileNotFound:           {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "result file %s not found"},
	ErrNoConfig:                     {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "no configure: %s"},
	// Group 5: rpc timeout
	ErrRPCTimeout:           {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "rpc timeout"},
	ErrClientClosed:         {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "client closed"},
	ErrBackendClosed:        {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "backend closed"},
	ErrStreamClosed:         {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "stream closed"},
	ErrNoAvailableBackend:   {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "no available backend"},
	ErrBackendCannotConnect: {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "can not connect to remote backend"},

	// Group 6: txn
	ErrTxnClosed:                 {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "the transaction %s has been committed or aborted"},
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
	ErrDuplicateKey:              {ER_DUP_KEYNAME, []string{MySQLDefaultSqlState}, "duplicate key name '%s'"},

	// Group 7: lock service
	ErrDeadLockDetected:     {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "deadlock detected"},
	ErrLockTableBindChanged: {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "lock table bind chaged"},
	ErrLockTableNotFound:    {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "lock table not found on remote lock service"},

	// Group End: max value of MOErrorCode
	ErrEnd: {ER_UNKNOWN_ERROR, []string{MySQLDefaultSqlState}, "internal error: end of errcode code"},
}

func newError(ctx context.Context, code uint16, args ...any) *Error {
	var err *Error
	item, has := errorMsgRefer[code]
	if !has {
		panic(NewInternalError(ctx, "not exist MOErrorCode: %d", code))
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

var _ encoding.BinaryMarshaler = new(Error)

func (e *Error) MarshalBinary() ([]byte, error) {
	ee := moerrpb.Error{
		Code:      e.code,
		MysqlCode: e.mysqlCode,
		Message:   e.message,
		SqlState:  e.sqlState,
	}
	data := make([]byte, ee.ProtoSize())
	if _, err := ee.MarshalToSizedBuffer(data); err != nil {
		return nil, ConvertGoError(Context(), err)
	}
	return data, nil
}

var _ encoding.BinaryUnmarshaler = new(Error)

func (e *Error) UnmarshalBinary(data []byte) error {
	var ee moerrpb.Error
	if err := ee.Unmarshal(data); err != nil {
		return ConvertGoError(Context(), err)
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
func ConvertPanicError(ctx context.Context, v interface{}) *Error {
	if e, ok := v.(*Error); ok {
		return e
	}
	return newError(ctx, ErrInternal, fmt.Sprintf("panic %v: %+v", v, stack.Callers(3)))
}

// ConvertGoError converts a go error into mo error.
// Note here we must return error, because nil error
// is the same as nil *Error -- Go strangeness.
func ConvertGoError(ctx context.Context, err error) error {
	// nil is nil
	if err == nil {
		return err
	}

	// already a moerr, return it as is
	if _, ok := err.(*Error); ok {
		return err
	}

	// Convert a few well known os/go error.
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		// if io.EOF reaches here, we believe it is not expected.
		return NewUnexpectedEOF(ctx, err.Error())
	}

	return NewInternalError(ctx, "convert go error to mo error %v", err)
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
var errOkExpectedEOF = Error{OkExpectedEOF, 0, "ExpectedEOF", "00000"}
var errOkExpectedEOB = Error{OkExpectedEOB, 0, "ExpectedEOB", "00000"}
var errOkExpectedDup = Error{OkExpectedDup, 0, "ExpectedDup", "00000"}
var errOkExpectedPossibleDup = Error{OkExpectedPossibleDup, 0, "OkExpectedPossibleDup", "00000"}

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
	return &errOkExpectedEOF
}

func GetOkExpectedEOB() *Error {
	return &errOkExpectedEOB
}

func GetOkExpectedDup() *Error {
	return &errOkExpectedDup
}

func GetOkExpectedPossibleDup() *Error {
	return &errOkExpectedPossibleDup
}

func NewInfo(ctx context.Context, msg string) *Error {
	return newError(ctx, ErrInfo, msg)
}

func NewLoadInfo(ctx context.Context, rec, del, skip, warn, writeTimeOut uint64) *Error {
	return newError(ctx, ErrLoadInfo, rec, del, skip, warn, writeTimeOut)
}

func NewWarn(ctx context.Context, msg string) *Error {
	return newError(ctx, ErrWarn, msg)
}

func NewBadS3Config(ctx context.Context, msg string) *Error {
	return newError(ctx, ErrBadS3Config, msg)
}

func NewInternalError(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrInternal, xmsg)
}

func NewNYI(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrNYI, xmsg)
}

func NewNotSupported(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrNotSupported, xmsg)
}

func NewOOM(ctx context.Context) *Error {
	return newError(ctx, ErrOOM)
}

func NewQueryInterrupted(ctx context.Context) *Error {
	return newError(ctx, ErrQueryInterrupted)
}

func NewDivByZero(ctx context.Context) *Error {
	return newError(ctx, ErrDivByZero)
}

func NewOutOfRange(ctx context.Context, typ string, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrOutOfRange, typ, xmsg)
}

func NewDataTruncated(ctx context.Context, typ string, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrDataTruncated, typ, xmsg)
}

func NewInvalidArg(ctx context.Context, arg string, val any) *Error {
	return newError(ctx, ErrInvalidArg, arg, fmt.Sprintf("%v", val))
}

func NewTruncatedValueForField(ctx context.Context, t, v, c string, idx int) *Error {
	return newError(ctx, ErrTruncatedWrongValueForField, t, v, c, idx)
}

func NewBadConfig(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrBadConfig, xmsg)
}

func NewInvalidInput(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrInvalidInput, xmsg)
}

func NewSyntaxError(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrSyntaxError, xmsg)
}

func NewParseError(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrParseError, xmsg)
}

func NewConstraintViolation(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrConstraintViolation, xmsg)
}

func NewEmptyVector(ctx context.Context) *Error {
	return newError(ctx, ErrEmptyVector)
}

func NewFileNotFound(ctx context.Context, f string) *Error {
	return newError(ctx, ErrFileNotFound, f)
}

func NewResultFileNotFound(ctx context.Context, f string) *Error {
	return newError(ctx, ErrResultFileNotFound, f)
}

func NewNoConfig(ctx context.Context, f string) *Error {
	return newError(ctx, ErrNoConfig, f)
}

func NewFileAlreadyExists(ctx context.Context, f string) *Error {
	return newError(ctx, ErrFileAlreadyExists, f)
}

func NewDBAlreadyExists(ctx context.Context, db string) *Error {
	return newError(ctx, ErrDBAlreadyExists, db)
}

func NewTableAlreadyExists(ctx context.Context, t string) *Error {
	return newError(ctx, ErrTableAlreadyExists, t)
}

func NewUnexpectedEOF(ctx context.Context, f string) *Error {
	return newError(ctx, ErrUnexpectedEOF, f)
}

func NewEmptyRange(ctx context.Context, f string) *Error {
	return newError(ctx, ErrEmptyRange, f)
}

func NewSizeNotMatch(ctx context.Context, f string) *Error {
	return newError(ctx, ErrSizeNotMatch, f)
}

func NewNoProgress(ctx context.Context, f string) *Error {
	return newError(ctx, ErrNoProgress, f)
}

func NewInvalidPath(ctx context.Context, f string) *Error {
	return newError(ctx, ErrInvalidPath, f)
}

func NewInvalidState(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrInvalidState, xmsg)
}

func NewInvalidTask(ctx context.Context, runner string, id uint64) *Error {
	return newError(ctx, ErrInvalidTask, runner, id)
}

func NewInvalidServiceIndex(ctx context.Context, idx int) *Error {
	return newError(ctx, ErrInvalidServiceIndex, idx)
}

func NewLogServiceNotReady(ctx context.Context) *Error {
	return newError(ctx, ErrLogServiceNotReady)
}

func NewBadDB(ctx context.Context, name string) *Error {
	return newError(ctx, ErrBadDB, name)
}

func NewNoDB(ctx context.Context) *Error {
	return newError(ctx, ErrNoDB)
}

func NewNoWorkingStore(ctx context.Context) *Error {
	return newError(ctx, ErrNoWorkingStore)
}

func NewNoService(ctx context.Context, name string) *Error {
	return newError(ctx, ErrNoService, name)
}

func NewDupServiceName(ctx context.Context, name string) *Error {
	return newError(ctx, ErrDupServiceName, name)
}

func NewWrongService(ctx context.Context, exp, got string) *Error {
	return newError(ctx, ErrWrongService, exp, got)
}

func NewNoHAKeeper(ctx context.Context) *Error {
	return newError(ctx, ErrNoHAKeeper)
}

func NewInvalidTruncateLsn(ctx context.Context, shardId, idx uint64) *Error {
	return newError(ctx, ErrInvalidTruncateLsn, shardId, idx)
}

func NewNotLeaseHolder(ctx context.Context, holderId uint64) *Error {
	return newError(ctx, ErrNotLeaseHolder, holderId)
}

func NewNoSuchTable(ctx context.Context, db, tbl string) *Error {
	return newError(ctx, ErrNoSuchTable, db, tbl)
}

func NewNoSuchSequence(ctx context.Context, db, tbl string) *Error {
	return newError(ctx, ErrNoSuchSequence, db, tbl)
}

func NewBadView(ctx context.Context, db, v string) *Error {
	return newError(ctx, ErrBadView, db, v)
}

func NewRPCTimeout(ctx context.Context) *Error {
	return newError(ctx, ErrRPCTimeout)
}

func NewClientClosed(ctx context.Context) *Error {
	return newError(ctx, ErrClientClosed)
}

func NewBackendClosed(ctx context.Context) *Error {
	return newError(ctx, ErrBackendClosed)
}

func NewStreamClosed(ctx context.Context) *Error {
	return newError(ctx, ErrStreamClosed)
}

func NewNoAvailableBackend(ctx context.Context) *Error {
	return newError(ctx, ErrNoAvailableBackend)
}

func NewBackendCannotConnect(ctx context.Context) *Error {
	return newError(ctx, ErrBackendCannotConnect)
}

func NewTxnClosed(ctx context.Context, txnID []byte) *Error {
	id := "unknown"
	if len(txnID) > 0 {
		id = hex.EncodeToString(txnID)
	}
	return newError(ctx, ErrTxnClosed, id)
}

func NewTxnWriteConflict(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrTxnWriteConflict, xmsg)
}

func NewMissingTxn(ctx context.Context) *Error {
	return newError(ctx, ErrMissingTxn)
}

func NewUnresolvedConflict(ctx context.Context) *Error {
	return newError(ctx, ErrUnresolvedConflict)
}

func NewTxnError(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrTxnError, xmsg)
}

func NewTAEError(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrTAEError, xmsg)
}

func NewDNShardNotFound(ctx context.Context, uuid string, id uint64) *Error {
	return newError(ctx, ErrDNShardNotFound, uuid, id)
}

func NewShardNotReported(ctx context.Context, uuid string, id uint64) *Error {
	return newError(ctx, ErrShardNotReported, uuid, id)
}

func NewDragonboatTimeout(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrDragonboatTimeout, xmsg)
}

func NewDragonboatTimeoutTooSmall(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrDragonboatTimeoutTooSmall, xmsg)
}

func NewDragonboatInvalidDeadline(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrDragonboatInvalidDeadline, xmsg)
}

func NewDragonboatRejected(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrDragonboatRejected, xmsg)
}

func NewDragonboatInvalidPayloadSize(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrDragonboatInvalidPayloadSize, xmsg)
}

func NewDragonboatShardNotReady(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrDragonboatShardNotReady, xmsg)
}

func NewDragonboatSystemClosed(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrDragonboatSystemClosed, xmsg)
}

func NewDragonboatInvalidRange(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrDragonboatInvalidRange, xmsg)
}

func NewDragonboatShardNotFound(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrDragonboatShardNotFound, xmsg)
}

func NewDragonboatOtherSystemError(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrDragonboatOtherSystemError, xmsg)
}

func NewErrDropNonExistsDB(ctx context.Context, name string) *Error {
	return newError(ctx, ErrDropNonExistsDB, name)
}

func NewTAERead(ctx context.Context) *Error {
	return newError(ctx, ErrTAERead)
}

func NewRpcError(ctx context.Context, msg string) *Error {
	return newError(ctx, ErrRpcError, msg)
}

func NewWaitTxn(ctx context.Context) *Error {
	return newError(ctx, ErrWaitTxn)
}

func NewTxnNotFound(ctx context.Context) *Error {
	return newError(ctx, ErrTxnNotFound)
}

func NewTxnNotActive(ctx context.Context, st string) *Error {
	return newError(ctx, ErrTxnNotActive, st)
}

func NewTAEWrite(ctx context.Context) *Error {
	return newError(ctx, ErrTAEWrite)
}

func NewTAECommit(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrTAECommit, xmsg)
}

func NewTAERollback(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrTAERollback, xmsg)
}

func NewTAEPrepare(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrTAEPrepare, xmsg)
}

func NewTAEPossibleDuplicate(ctx context.Context) *Error {
	return newError(ctx, ErrTAEPossibleDuplicate)
}

func NewTxnRWConflict(ctx context.Context) *Error {
	return newError(ctx, ErrTxnRWConflict)
}

func NewTxnWWConflict(ctx context.Context) *Error {
	return newError(ctx, ErrTxnWWConflict)
}

func NewNotFound(ctx context.Context) *Error {
	return newError(ctx, ErrNotFound)
}

func NewDuplicate(ctx context.Context) *Error {
	return newError(ctx, ErrDuplicate)
}

func NewDuplicateEntry(ctx context.Context, entry string, key string) *Error {
	return newError(ctx, ErrDuplicateEntry, entry, key)
}

func NewWrongValueCountOnRow(ctx context.Context, row int) *Error {
	return newError(ctx, ErrWrongValueCountOnRow, row)
}

func NewBadFieldError(ctx context.Context, column, table string) *Error {
	return newError(ctx, ErrBadFieldError, column, table)
}

func NewRoleGrantedToSelf(ctx context.Context, from, to string) *Error {
	return newError(ctx, ErrRoleGrantedToSelf, from, to)
}

func NewTxnInternal(ctx context.Context) *Error {
	return newError(ctx, ErrTxnInternal)
}

func NewTxnReadConflict(ctx context.Context, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(ctx, ErrTxnReadConflict, xmsg)
}

func NewPrimaryKeyDuplicated(ctx context.Context, k any) *Error {
	return newError(ctx, ErrPrimaryKeyDuplicated, k)
}

func NewDuplicateKey(ctx context.Context, k string) *Error {
	return newError(ctx, ErrDuplicateKey, k)
}

func NewAppendableSegmentNotFound(ctx context.Context) *Error {
	return newError(ctx, ErrAppendableSegmentNotFound)
}

func NewAppendableBlockNotFound(ctx context.Context) *Error {
	return newError(ctx, ErrAppendableBlockNotFound)
}

func NewDeadLockDetected(ctx context.Context) *Error {
	return newError(ctx, ErrDeadLockDetected)
}

func NewLockTableBindChanged(ctx context.Context) *Error {
	return newError(ctx, ErrLockTableBindChanged)
}

func NewLockTableNotFound(ctx context.Context) *Error {
	return newError(ctx, ErrLockTableNotFound)
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

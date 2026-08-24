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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func pf1() {
	panic("foo")
}

func pf2(a, b int) int {
	return a / b
}

func pf3() {
	panic(NewInternalError(context.TODO(), fmt.Sprintf("%s %s %s %d", "foo", "bar", "zoo", 2)))
}

func PanicF(i int) (err *Error) {
	defer func() {
		if e := recover(); e != nil {
			err = ConvertPanicError(context.TODO(), e)
		}
	}()
	switch i {
	case 1:
		pf1()
	case 2:
		foo := pf2(1, 0)
		panic(foo)
	case 3:
		pf3()
	default:
		return nil
	}
	return
}

func TestPanicError(t *testing.T) {
	for i := 0; i <= 3; i++ {
		err := PanicF(i)
		if i == 0 {
			if err != nil {
				t.Errorf("No panic should be OK")
			}
		} else {
			if err == nil {
				t.Errorf("Uncaught panic")
			}
			if err.Succeeded() {
				t.Errorf("Caught OK panic")
			}
		}
	}
}

func TestNew_panic(t *testing.T) {
	defer func() {
		var err any
		if err = recover(); err != nil {
			require.Equal(t, "foobarzoo is not yet implemented", err.(*Error).Error())
			t.Logf("err: %+v", err)
		}
	}()
	panic(NewNYI(context.TODO(), "foobarzoo"))
}

func TestNew_MyErrorCode(t *testing.T) {
	err := NewDivByZero(context.TODO())
	require.Equal(t, ER_DIVISION_BY_ZERO, err.MySQLCode())

	err = NewQueryTimeout(context.TODO())
	require.Equal(t, ER_QUERY_TIMEOUT, err.MySQLCode())
	require.Equal(t, MySQLDefaultSqlState, err.SqlState())

	err = NewOutOfRange(context.TODO(), "int8", "1111")
	require.Equal(t, ER_DATA_OUT_OF_RANGE, err.MySQLCode())

	err = NewPreparedParamOutOfRange(context.TODO(), "unsigned integer", "EXECUTE")
	require.Equal(t, ErrPreparedParamOutOfRange, err.ErrorCode())
	require.Equal(t, ER_DATA_OUT_OF_RANGE, err.MySQLCode())
	require.Equal(t, "22003", err.SqlState())
	require.Equal(t, "unsigned integer value is out of range in 'EXECUTE'", err.Error())

	err = NewUnknownStmtHandler(context.TODO(), "stmt1", "DEALLOCATE PREPARE")
	require.Equal(t, ErrUnknownStmtHandler, err.ErrorCode())
	require.Equal(t, ER_UNKNOWN_STMT_HANDLER, err.MySQLCode())
	require.Equal(t, MySQLDefaultSqlState, err.SqlState())
	require.Equal(t,
		"Unknown prepared statement handler (stmt1) given to DEALLOCATE PREPARE",
		err.Error(),
	)
}

func TestWrongArgumentsMySQLError(t *testing.T) {
	err := NewWrongArguments(context.Background(), "nth_value")
	require.Equal(t, ErrWrongArguments, err.ErrorCode())
	require.Equal(t, ER_WRONG_ARGUMENTS, err.MySQLCode())
	require.Equal(t, MySQLDefaultSqlState, err.SqlState())
	require.Equal(t, "Incorrect arguments to nth_value", err.Error())
}

func TestWindowInvalidUseMySQLError(t *testing.T) {
	err := NewWindowInvalidUse(context.Background(), "row_number")
	require.Equal(t, ErrWindowInvalidUse, err.ErrorCode())
	require.Equal(t, ER_WINDOW_INVALID_WINDOW_FUNC_USE, err.MySQLCode())
	require.Equal(t, MySQLDefaultSqlState, err.SqlState())
	require.Equal(t, "You cannot use the window function 'row_number' in this context", err.Error())
}

func TestInvalidGroupFuncUseMySQLError(t *testing.T) {
	err := NewInvalidGroupFuncUse(context.Background())
	require.Equal(t, ErrInvalidGroupFuncUse, err.ErrorCode())
	require.Equal(t, ER_INVALID_GROUP_FUNC_USE, err.MySQLCode())
	require.Equal(t, MySQLDefaultSqlState, err.SqlState())
	require.Equal(t, "Invalid use of group function", err.Error())
}

func TestViewSelectTmpTableMySQLError(t *testing.T) {
	err := NewViewSelectTmpTable(context.Background(), "temp_for_view")
	require.Equal(t, ErrViewSelectTmpTable, err.ErrorCode())
	require.Equal(t, ER_VIEW_SELECT_TMPTABLE, err.MySQLCode())
	require.Equal(t, MySQLDefaultSqlState, err.SqlState())
	require.Equal(t, "View's SELECT refers to a temporary table 'temp_for_view'", err.Error())
}

func TestLockWaitTimeoutMySQLError(t *testing.T) {
	err := NewLockWaitTimeout(context.Background())
	require.Equal(t, ErrLockWaitTimeout, err.ErrorCode())
	require.Equal(t, ER_LOCK_WAIT_TIMEOUT, err.MySQLCode())
	require.Equal(t, MySQLDefaultSqlState, err.SqlState())
	require.Equal(t, "Lock wait timeout exceeded; try restarting transaction", err.Error())

	noCtxErr := NewLockWaitTimeoutNoCtx()
	require.Equal(t, ErrLockWaitTimeout, noCtxErr.ErrorCode())
	require.Equal(t, ER_LOCK_WAIT_TIMEOUT, noCtxErr.MySQLCode())
}

func TestMaxPreparedStmtCountReachedMySQLError(t *testing.T) {
	err := NewMaxPreparedStmtCountReached(context.Background(), 2)
	require.Equal(t, ErrMaxPreparedStmtCountReached, err.ErrorCode())
	require.Equal(t, ER_MAX_PREPARED_STMT_COUNT_REACHED, err.MySQLCode())
	require.Equal(t, "42000", err.SqlState())
	require.Equal(t,
		"Can't create more than max_prepared_stmt_count statements (current value: 2)",
		err.Error())
}

func TestIsMoErrCode(t *testing.T) {
	err := NewDivByZero(context.TODO())
	require.True(t, IsMoErrCode(err, ErrDivByZero))
	require.False(t, IsMoErrCode(err, ErrOOM))

	err2 := NewInternalError(context.TODO(), "what is this")
	require.False(t, IsMoErrCode(err2, ErrDivByZero))
	require.False(t, IsMoErrCode(err2, ErrOOM))
}

func TestEncoding(t *testing.T) {
	e := NewDivByZero(context.TODO())
	data, err := e.MarshalBinary()
	require.Nil(t, err)
	e2 := new(Error)
	err = e2.UnmarshalBinary(data)
	require.Nil(t, err)
	require.Equal(t, e, e2)
}

func TestResourceExhaustedWithDetailsEncoding(t *testing.T) {
	err := NewResourceExhaustedf(context.Background(), "requested=%d used=%d limit=%d", 3, 5, 7)
	require.Equal(t, ErrOOM, err.ErrorCode())
	require.Equal(t, ER_ENGINE_OUT_OF_MEMORY, err.MySQLCode())
	require.Equal(t,
		"error: resource exhausted: requested=3 used=5 limit=7",
		err.Error())

	data, marshalErr := err.MarshalBinary()
	require.NoError(t, marshalErr)
	decoded := new(Error)
	require.NoError(t, decoded.UnmarshalBinary(data))
	require.Equal(t, err, decoded)
}

func TestNoSuchTableWithFormattedMessage(t *testing.T) {
	err := NewNoSuchTablef(context.Background(), "SQL parser error: table %q does not exist", "missing")
	require.Equal(t, ErrNoSuchTable, err.ErrorCode())
	require.Equal(t, ER_NO_SUCH_TABLE, err.MySQLCode())
	require.Equal(t, `SQL parser error: table "missing" does not exist`, err.Error())
}

func TestBadFieldErrorWithFormattedMessage(t *testing.T) {
	err := NewBadFieldErrorf(context.Background(), "invalid input: column %s does not exist", "metric")
	require.Equal(t, ErrBadFieldError, err.ErrorCode())
	require.Equal(t, ER_BAD_FIELD_ERROR, err.MySQLCode())
	require.Equal(t, "42S22", err.SqlState())
	require.Equal(t, "invalid input: column metric does not exist", err.Error())
}

func TestMPoolCapacityEncoding(t *testing.T) {
	err := NewMPoolCapacityNoCtxf("alloc %d bytes, cap %d", 8, 4)
	require.Equal(t, ErrMPoolCapacity, err.ErrorCode())
	require.Equal(t, ER_ENGINE_OUT_OF_MEMORY, err.MySQLCode())
	require.Contains(t, err.Error(), "alloc 8 bytes, cap 4")

	data, marshalErr := err.MarshalBinary()
	require.NoError(t, marshalErr)
	decoded := new(Error)
	require.NoError(t, decoded.UnmarshalBinary(data))
	require.Equal(t, err, decoded)
}

func TestErrSubqueryNo1RowContract(t *testing.T) {
	err := NewErrSubqueryNo1Row(context.Background())
	require.Equal(t, ErrSubqueryNo1Row, err.ErrorCode())
	require.Equal(t, ER_SUBQUERY_NO_1_ROW, err.MySQLCode())
	require.Equal(t, "21000", err.SqlState())
	require.Equal(t, "Subquery returns more than 1 row", err.Error())

	data, marshalErr := err.MarshalBinary()
	require.NoError(t, marshalErr)

	decoded := new(Error)
	require.NoError(t, decoded.UnmarshalBinary(data))
	require.Equal(t, err, decoded)
}

func TestErrTooManyRowsContract(t *testing.T) {
	err := NewTooManyRows(context.Background())
	require.Equal(t, ErrTooManyRows, err.ErrorCode())
	require.Equal(t, ER_TOO_MANY_ROWS, err.MySQLCode())
	require.Equal(t, "42000", err.SqlState())
	require.Equal(t, "Result consisted of more than one row", err.Error())

	data, marshalErr := err.MarshalBinary()
	require.NoError(t, marshalErr)

	decoded := new(Error)
	require.NoError(t, decoded.UnmarshalBinary(data))
	require.Equal(t, err, decoded)
}

func TestErrCantChangeTxnCodeRemainsStable(t *testing.T) {
	// This code is part of the client-visible compatibility contract. New
	// MatrixOne errors must use a fresh code instead of renumbering it.
	require.Equal(t, uint16(20325), ErrCantChangeTxn)
}

func TestErrWrongNumberOfColumnsInSelectContract(t *testing.T) {
	err := NewWrongNumberOfColumnsInSelect(context.Background())
	require.Equal(t, ErrWrongNumberOfColumnsInSelect, err.ErrorCode())
	require.Equal(t, ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, err.MySQLCode())
	require.Equal(t, "21000", err.SqlState())
	require.Equal(t, "The used SELECT statements have a different number of columns", err.Error())

	data, marshalErr := err.MarshalBinary()
	require.NoError(t, marshalErr)

	decoded := new(Error)
	require.NoError(t, decoded.UnmarshalBinary(data))
	require.Equal(t, err, decoded)
}

type fakeErr struct {
}

func (f *fakeErr) Error() string {
	return "fake error"
}

func TestIsSameMoErr(t *testing.T) {
	var a, b error
	require.False(t, IsSameMoErr(a, b))

	_, ok := GetMoErrCode(a)
	require.False(t, ok)

	_, ok = GetMoErrCode(b)
	require.False(t, ok)

	a = &fakeErr{}
	require.False(t, IsSameMoErr(a, b))

	_, ok = GetMoErrCode(a)
	require.False(t, ok)

	b = &fakeErr{}
	require.False(t, IsSameMoErr(a, b))

	_, ok = GetMoErrCode(b)
	require.False(t, ok)

	a = GetOkExpectedEOB()
	require.False(t, IsSameMoErr(a, b))

	code, ok := GetMoErrCode(a)
	require.True(t, ok)
	require.Equal(t, OkExpectedEOB, code)

	b = GetOkExpectedDup()
	require.False(t, IsSameMoErr(a, b))

	code, ok = GetMoErrCode(b)
	require.True(t, ok)
	require.Equal(t, OkExpectedDup, code)

	b = nil
	require.False(t, IsSameMoErr(a, b))

	b = GetOkExpectedEOB()
	require.True(t, IsSameMoErr(a, b))
}

// TestNewErrTooBigPrecision tests the NewErrTooBigPrecision error constructor
func TestNewErrTooBigPrecision(t *testing.T) {
	ctx := context.TODO()

	// Test with function name "now"
	err := NewErrTooBigPrecision(ctx, 7, "now", 6)
	require.NotNil(t, err)
	require.Equal(t, ErrTooBigPrecision, err.ErrorCode())
	require.Equal(t, ER_TOO_BIG_PRECISION, err.MySQLCode())
	require.Contains(t, err.Error(), "Too-big precision 7 specified for 'now'")
	require.Contains(t, err.Error(), "Maximum is 6")

	// Test with function name "sysdate"
	err = NewErrTooBigPrecision(ctx, -1, "sysdate", 6)
	require.NotNil(t, err)
	require.Equal(t, ErrTooBigPrecision, err.ErrorCode())
	require.Contains(t, err.Error(), "Too-big precision -1 specified for 'sysdate'")
	require.Contains(t, err.Error(), "Maximum is 6")

	// Test with type name "TIMESTAMP"
	err = NewErrTooBigPrecision(ctx, 10, "TIMESTAMP", 6)
	require.NotNil(t, err)
	require.Equal(t, ErrTooBigPrecision, err.ErrorCode())
	require.Contains(t, err.Error(), "Too-big precision 10 specified for 'TIMESTAMP'")
	require.Contains(t, err.Error(), "Maximum is 6")

	// Verify error code
	code, ok := GetMoErrCode(err)
	require.True(t, ok)
	require.Equal(t, ErrTooBigPrecision, code)
}

func Test_ForCoverage(t *testing.T) {
	ctx := context.Background()
	err := NewDataTruncatedf(ctx, "test", "test")
	require.True(t, IsMoErrCode(err, ErrDataTruncated))

	err = NewConstraintViolationf(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrConstraintViolation))

	err = NewTxnWriteConflictf(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrTxnWriteConflict))

	err = NewTxnErrorf(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrTxnError))

	err = NewTAEErrorf(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrTAEError))

	err = NewDragonboatTimeoutf(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrDragonboatTimeout))

	err = NewDragonboatTimeoutTooSmallf(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrDragonboatTimeoutTooSmall))

	err = NewDragonboatInvalidDeadlinef(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrDragonboatInvalidDeadline))

	err = NewDragonboatRejectedf(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrDragonboatRejected))

	err = NewDragonboatInvalidPayloadSizef(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrDragonboatInvalidPayloadSize))

	err = NewDragonboatShardNotReadyf(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrDragonboatShardNotReady))

	err = NewDragonboatSystemClosedf(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrDragonboatSystemClosed))

	err = NewDragonboatInvalidRangef(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrDragonboatInvalidRange))

	err = NewDragonboatShardNotFoundf(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrDragonboatShardNotFound))

	err = NewDragonboatOtherSystemErrorf(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrDragonboatOtherSystemError))

	err = NewTAECommitf(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrTAECommit))

	err = NewTAERollbackf(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrTAERollback))

	err = NewTAEPreparef(ctx, "test")
	require.True(t, IsMoErrCode(err, ErrTAEPrepare))

	err = NewTxnStaleNoCtxf("test")
	require.True(t, IsMoErrCode(err, ErrTxnStale))
}

// TestNewErrCastWidthExceeded verifies the cast width-violation error carries the
// ErrCastWidthExceeded code and maps to MySQL ER_DATA_TOO_LONG (1406) — the
// correct protocol code for an over-length write. The message template is bare
// "%s" (no "internal error:" prefix); the JDBC driver then wraps it as
// java.sql.DataTruncation, which the BVT result files reflect.
func TestNewErrCastWidthExceeded(t *testing.T) {
	ctx := context.Background()
	err := NewErrCastWidthExceeded(ctx, "Can't cast 'abcd' to VARCHAR type. Src length 4 is larger than Dest length 3")

	require.Equal(t, ErrCastWidthExceeded, err.ErrorCode())
	require.Equal(t, uint16(ER_DATA_TOO_LONG), err.MySQLCode())
	require.Equal(t, "22001", err.SqlState())
	require.True(t, IsMoErrCode(err, ErrCastWidthExceeded))
	// Bare "%s" template: the diagnostic message is carried verbatim.
	require.Equal(t,
		"Can't cast 'abcd' to VARCHAR type. Src length 4 is larger than Dest length 3",
		err.Error())
}

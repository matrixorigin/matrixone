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

	err = NewOutOfRange(context.TODO(), "int8", "1111")
	require.Equal(t, ER_DATA_OUT_OF_RANGE, err.MySQLCode())
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

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

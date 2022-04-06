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
	"testing"
)

func pf1() {
	panic("foo")
}

func pf2(a, b int) int {
	return a / b
}

func pf3() {
	panic(NewInternalError("%s %s %s %d", "foo", "bar", "zoo", 2))
}

func PanicF(i int) (err *Error) {
	defer func() {
		if e := recover(); e != nil {
			err = NewPanicError(e)
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
			if err.Ok() {
				t.Errorf("Caught OK panic")
			}
		}
	}
}

// Copyright 2021 Matrix Origin
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

package common

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

func CompareInterface(a, b interface{}) int64 {
	if av, ok := a.(int8); ok {
		return int64(av - b.(int8))
	}
	if av, ok := a.(int16); ok {
		return int64(av - b.(int16))
	}
	if av, ok := a.(int32); ok {
		return int64(av - b.(int32))
	}
	if av, ok := a.(int64); ok {
		return av - b.(int64)
	}
	if av, ok := a.(uint8); ok {
		return int64(av - b.(uint8))
	}
	if av, ok := a.(uint16); ok {
		return int64(av - b.(uint16))
	}
	if av, ok := a.(uint32); ok {
		return int64(av - b.(uint32))
	}
	if av, ok := a.(uint64); ok {
		return int64(av - b.(uint64))
	}
	if av, ok := a.(float32); ok {
		return int64(av - b.(float32))
	}
	if av, ok := a.(float64); ok {
		return int64(av - b.(float64))
	}
	if av, ok := a.(types.Date); ok {
		return int64(av - b.(types.Date))
	}
	if av, ok := a.(types.Datetime); ok {
		return int64(av - b.(types.Datetime))
	}
	if av, ok := a.([]byte); ok {
		return int64(bytes.Compare(av, b.([]byte)))
	}
	logutil.Infof("%+v\n%+v\n", a, b)
	panic("invalid type")
}


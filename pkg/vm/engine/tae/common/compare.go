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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common/errors"
)

func CompareGeneric(a, b interface{}, t types.Type) int {
	switch t.Oid {
	case types.T_int8:
		if a.(int8) > b.(int8) {
			return 1
		} else if a.(int8) < b.(int8) {
			return -1
		} else {
			return 0
		}
	case types.T_int16:
		if a.(int16) > b.(int16) {
			return 1
		} else if a.(int16) < b.(int16) {
			return -1
		} else {
			return 0
		}
	case types.T_int32:
		if a.(int32) > b.(int32) {
			return 1
		} else if a.(int32) < b.(int32) {
			return -1
		} else {
			return 0
		}
	case types.T_int64:
		if a.(int64) > b.(int64) {
			return 1
		} else if a.(int64) < b.(int64) {
			return -1
		} else {
			return 0
		}
	case types.T_uint8:
		if a.(uint8) > b.(uint8) {
			return 1
		} else if a.(uint8) < b.(uint8) {
			return -1
		} else {
			return 0
		}
	case types.T_uint16:
		if a.(uint16) > b.(uint16) {
			return 1
		} else if a.(uint16) < b.(uint16) {
			return -1
		} else {
			return 0
		}
	case types.T_uint32:
		if a.(uint32) > b.(uint32) {
			return 1
		} else if a.(uint32) < b.(uint32) {
			return -1
		} else {
			return 0
		}
	case types.T_uint64:
		if a.(uint64) > b.(uint64) {
			return 1
		} else if a.(uint64) < b.(uint64) {
			return -1
		} else {
			return 0
		}
	case types.T_float32:
		if a.(float32) > b.(float32) {
			return 1
		} else if a.(float32) < b.(float32) {
			return -1
		} else {
			return 0
		}
	case types.T_float64:
		if a.(float64) > b.(float64) {
			return 1
		} else if a.(float64) < b.(float64) {
			return -1
		} else {
			return 0
		}
	case types.T_date:
		if a.(types.Date) > b.(types.Date) {
			return 1
		} else if a.(types.Date) < b.(types.Date) {
			return -1
		} else {
			return 0
		}
	case types.T_datetime:
		if a.(types.Datetime) > b.(types.Datetime) {
			return 1
		} else if a.(types.Datetime) < b.(types.Datetime) {
			return -1
		} else {
			return 0
		}
	case types.T_char, types.T_varchar:
		res := bytes.Compare(a.([]byte), b.([]byte))
		if res > 0 {
			return 1
		} else if res < 0 {
			return -1
		} else {
			return 0
		}
	default:
		panic(errors.ErrTypeNotSupported)
	}
}


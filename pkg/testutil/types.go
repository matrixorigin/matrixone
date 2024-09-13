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

package testutil

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	boolType      = types.T_bool.ToType()
	bitType       = types.T_bit.ToType()
	int8Type      = types.T_int8.ToType()
	int16Type     = types.T_int16.ToType()
	int32Type     = types.T_int32.ToType()
	int64Type     = types.T_int64.ToType()
	uint8Type     = types.T_uint8.ToType()
	uint16Type    = types.T_uint16.ToType()
	uint32Type    = types.T_uint32.ToType()
	uint64Type    = types.T_uint64.ToType()
	float32Type   = types.T_float32.ToType()
	float64Type   = types.T_float64.ToType()
	varcharType   = types.T_varchar.ToType()
	textType      = types.T_text.ToType()
	rowIdType     = types.T_Rowid.ToType()
	blockIdType     = types.T_Blockid.ToType()
	tsType        = types.T_TS.ToType()
	uuidType      = types.T_uuid.ToType()
	jsonType      = types.T_json.ToType()
	dateType      = types.T_date.ToType()
	timeType      = types.T_time.ToType()
	datetimeType  = types.T_datetime.ToType()
	timestampType = types.T_timestamp.ToType()
)

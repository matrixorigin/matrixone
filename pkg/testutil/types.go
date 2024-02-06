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
	int8Type    = types.T_int8.ToType()
	int16Type   = types.T_int16.ToType()
	int32Type   = types.T_int32.ToType()
	int64Type   = types.T_int64.ToType()
	uint16Type  = types.T_uint16.ToType()
	varcharType = types.T_varchar.ToType()
	textType    = types.T_text.ToType()
	rowIdType   = types.T_Rowid.ToType()
)

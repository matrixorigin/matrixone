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

package mock

import "github.com/matrixorigin/matrixone/pkg/container/types"

func MockColTypes(colCnt int) (ct []types.Type) {
	for i := 0; i < colCnt; i++ {
		var typ types.Type
		switch i {
		case 0:
			typ = types.Type{
				Oid:   types.T_int8,
				Size:  1,
				Width: 8,
			}
		case 1:
			typ = types.Type{
				Oid:   types.T_int16,
				Size:  2,
				Width: 16,
			}
		case 2:
			typ = types.Type{
				Oid:   types.T_int32,
				Size:  4,
				Width: 32,
			}
		case 3:
			typ = types.Type{
				Oid:   types.T_int64,
				Size:  8,
				Width: 64,
			}
		case 4:
			typ = types.Type{
				Oid:   types.T_uint8,
				Size:  1,
				Width: 8,
			}
		case 5:
			typ = types.Type{
				Oid:   types.T_uint16,
				Size:  2,
				Width: 16,
			}
		case 6:
			typ = types.Type{
				Oid:   types.T_uint32,
				Size:  4,
				Width: 32,
			}
		case 7:
			typ = types.Type{
				Oid:   types.T_uint64,
				Size:  8,
				Width: 64,
			}
		case 8:
			typ = types.Type{
				Oid:   types.T_float32,
				Size:  4,
				Width: 32,
			}
		case 9:
			typ = types.Type{
				Oid:   types.T_float64,
				Size:  8,
				Width: 64,
			}
		case 10:
			typ = types.Type{
				Oid:   types.T_date,
				Size:  4,
				Width: 32,
			}
		case 11:
			typ = types.Type{
				Oid:   types.T_datetime,
				Size:  8,
				Width: 64,
			}
		case 12:
			typ = types.Type{
				Oid:   types.T_varchar,
				Size:  24,
				Width: 100,
			}
		case 13:
			typ = types.Type{
				Oid:   types.T_char,
				Size:  24,
				Width: 100,
			}
		}
		ct = append(ct, typ)
	}
	return
}

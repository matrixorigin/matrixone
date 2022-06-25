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

package types

func MockColTypes(colCnt int) (ct []Type) {
	for i := 0; i < colCnt; i++ {
		var typ Type
		switch i {
		case 0:
			typ = Type{
				Oid:   Type_INT8,
				Size:  1,
				Width: 8,
			}
		case 1:
			typ = Type{
				Oid:   Type_INT16,
				Size:  2,
				Width: 16,
			}
		case 2:
			typ = Type{
				Oid:   Type_INT32,
				Size:  4,
				Width: 32,
			}
		case 3:
			typ = Type{
				Oid:   Type_INT64,
				Size:  8,
				Width: 64,
			}
		case 4:
			typ = Type{
				Oid:   Type_UINT8,
				Size:  1,
				Width: 8,
			}
		case 5:
			typ = Type{
				Oid:   Type_UINT16,
				Size:  2,
				Width: 16,
			}
		case 6:
			typ = Type{
				Oid:   Type_UINT32,
				Size:  4,
				Width: 32,
			}
		case 7:
			typ = Type{
				Oid:   Type_UINT64,
				Size:  8,
				Width: 64,
			}
		case 8:
			typ = Type{
				Oid:   Type_FLOAT32,
				Size:  4,
				Width: 32,
			}
		case 9:
			typ = Type{
				Oid:   Type_FLOAT64,
				Size:  8,
				Width: 64,
			}
		case 10:
			typ = Type{
				Oid:   Type_DATE,
				Size:  4,
				Width: 32,
			}
		case 11:
			typ = Type{
				Oid:   Type_DATETIME,
				Size:  8,
				Width: 64,
			}
		case 12:
			typ = Type{
				Oid:   Type_VARCHAR,
				Size:  24,
				Width: 100,
			}
		case 13:
			typ = Type{
				Oid:   Type_CHAR,
				Size:  24,
				Width: 100,
			}
		case 14:
			typ = Type_BOOL.ToType()
			typ.Width = 8
		case 15:
			typ = Type_TIMESTAMP.ToType()
			typ.Width = 64
		case 16:
			typ = Type_DECIMAL64.ToType()
			typ.Width = 64
		case 17:
			typ = Type_DECIMAL128.ToType()
			typ.Width = 128
		}
		ct = append(ct, typ)
	}
	return
}

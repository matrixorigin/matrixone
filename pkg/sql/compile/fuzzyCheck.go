// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package compile

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func newFuzzyInfo(bat *batch.Batch) *fuzzyCheckInfo {
	cnt := int32(len(bat.Attrs))
	// last two vectors contain the dbName and tblName
	// refer func generateRbat in fuzzyFilter operator for batch input
	return &fuzzyCheckInfo{
		db:    bat.GetVector(cnt - 2).GetStringAt(0),
		tbl:   bat.GetVector(cnt - 1).GetStringAt(0),
		attrs: bat.Attrs[:cnt-2],
		isCpk: cnt > 1+2,
	}
}

// generate condition for background SQL to check if duplicate constraint is satisfied
// for compound primary key, the condtion will look like : "attr_1=x1 and attr_2=y1 and ... attr_n = yn or attr1=x2 and attr=y2 ... "
// and its background SQL will look like : select pkAttrs, count(*) as cnt from db.tbl where **cont** having cnt > 1;
//
// for non compound primary key, cond will look like : "x1, x2, x3, ..."
// and its background SQL will look like :  select pkAttr, count(*) as cnt from db.tbl where pkAttr in (**cond**) having cnt > 1;
//
// for more info, refer func backgroundSQLCheck
func (f *fuzzyCheckInfo) genCondition(ctx context.Context, bat *batch.Batch) error {

	pkeys, err := f.genCollsionKeys(ctx, bat)
	if err != nil {
		return err
	}

	if err := f.firstCheck(ctx, pkeys); err != nil {
		return err // fail to pass duplicate constraint
	}

	if !f.isCpk {
		f.condition = strings.Join(pkeys[0], ", ")
	} else {
		var all bytes.Buffer
		var one bytes.Buffer
		var i int
		var j int

		var lastRow = len(pkeys[0]) - 1

		// one compound primary key has multiple conditions, use and to join them
		for i = 0; i < lastRow; i++ {
			one.Reset()
			for j = 0; j < len(f.attrs)-1; j++ {
				one.WriteString(fmt.Sprintf("%s = %s and ", f.attrs[j], pkeys[j][i]))
			}
			one.WriteString(fmt.Sprintf("%s = %s", f.attrs[j], pkeys[j][i]))
			one.WriteTo(&all)

			// use or join different compound primary keys
			all.WriteString(" or ")
		}

		for j = 0; j < len(f.attrs)-1; j++ {
			one.WriteString(fmt.Sprintf("%s = %s and ", f.attrs[j], pkeys[j][lastRow]))
		}
		one.WriteString(fmt.Sprintf("%s = %s", f.attrs[j], pkeys[j][lastRow]))
		one.WriteTo(&all)
		f.condition = all.String()
	}

	return nil
}

// firstCheck check if collision keys are duplicates with each other, if do dup, no need to run background SQL
func (f *fuzzyCheckInfo) firstCheck(ctx context.Context, collisionPkeys [][]string) error {
	cnt := make(map[string]int)
	if !f.isCpk {
		pkey := collisionPkeys[0]
		for _, k := range pkey {
			cnt[k]++
			if cnt[k] > 1 {
				// key that has been inserted before, fail to pass dup constraint
				return moerr.NewDuplicateEntry(ctx, k, f.attrs[0])
			}
		}
	} else {
		for i := 0; i < len(collisionPkeys[0]); i++ {
			var buf bytes.Buffer
			var j int
			buf.WriteString("(")
			for j = 0; j < len(collisionPkeys)-1; j++ {
				buf.WriteString(fmt.Sprintf("%s, ", collisionPkeys[j][i]))
			}
			buf.WriteString(fmt.Sprintf("%s)", collisionPkeys[j][i]))

			ck := buf.String()
			cnt[ck]++
			if cnt[ck] > 1 {
				// key that has been inserted before, fail to pass dup constraint
				return moerr.NewDuplicateEntry(ctx, ck, catalog.CPrimaryKeyColName)
			}
		}
	}
	return nil
}

// for table that like CREATE TABLE t1(a int, b CHAR(10), PRIMARY KEY(a, b));
// with : insert into t1 values(1, 'ab'), (1, 'ab');
// background SQL condition should be a=1 and b='ab' instead of a=1 and b=ab
// other time formats also require similar processing.
func (f *fuzzyCheckInfo) formatCol(col string, typ *types.Type) []string {
	s := strings.Trim(col, "[]")
	ss := strings.Split(s, " ")
	switch typ.Oid {
	case types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_TS:
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_text, types.T_uuid:
		for i, str := range ss {
			ss[i] = "'" + str + "'"
		}
		return ss

	default:
	}
	return ss
}

// genCollsionKeys return [][]string to store the string of collsion keys
func (f *fuzzyCheckInfo) genCollsionKeys(ctx context.Context, bat *batch.Batch) ([][]string, error) {
	if !f.isCpk {
		pkCol := bat.GetVector(0)
		pkeys := f.formatCol(pkCol.String(), pkCol.GetType())
		if len(pkeys) == 0 {
			return nil, moerr.NewInternalError(ctx, "fuzzyfilter failed to get collsion key to check duplicate constraints")
		}
		return [][]string{pkeys}, nil
	} else {
		kcnt := int32(len(bat.Attrs) - 2)
		cpkeys := make([][]string, kcnt)

		var i int32
		for i = 0; i < kcnt; i++ {
			partialCol := bat.GetVector(i)
			cpkeys[i] = f.formatCol(partialCol.String(), partialCol.GetType())
		}

		// check all partialCol has same length and greater than 0
		check := func(cPkeys [][]string) bool {
			if len(cPkeys) == 0 {
				return false
			}
			length := len(cPkeys[0])
			for i := 1; i < len(cPkeys); i++ {
				if len(cPkeys[i]) != length {
					return false
				}
			}
			return true
		}

		if !check(cpkeys) {
			return nil, moerr.NewInternalError(ctx, "fuzzyfilter failed to get collsion ckey to check duplicate constraints")
		}
		return cpkeys, nil
	}
}

// backgroundSQLCheck launches a background SQL to check if the true positive is true
func (f *fuzzyCheckInfo) backgroundSQLCheck(c *Compile) error {
	var duplicateCheckSql string
	if !f.isCpk {
		duplicateCheckSql = fmt.Sprintf(doubleCheckForFuzzyFilterWithoutCK, f.attrs[0], f.db, f.tbl, f.attrs[0], f.condition)
	} else {
		attrs := strings.Join(f.attrs, ", ")
		duplicateCheckSql = fmt.Sprintf(doubleCheckForFuzzyFilterWithCK, attrs, f.db, f.tbl, f.condition)
	}

	res, err := c.runSqlWithResult(duplicateCheckSql)
	defer res.Close()
	if err != nil {
		return err
	}
	if res.Batches != nil {
		v := res.Batches[0].Vecs
		if v != nil && v[0].Length() > 0 {
			if !f.isCpk {
				dupKey := fmt.Sprintf("%v", getNonNullValue(v[0], uint32(0)))
				return moerr.NewDuplicateEntry(c.ctx, dupKey, f.attrs[0])
			} else {
				return moerr.NewDuplicateEntry(c.ctx, "a", catalog.CPrimaryKeyColName)
			}
		} else {
			return nil
		}
	} else {
		panic(fmt.Sprintf("The execution flow caused by the %s should never enter this function", c.sql))
	}
}

func getNonNullValue(col *vector.Vector, row uint32) any {
	switch col.GetType().Oid {
	case types.T_bool:
		return vector.GetFixedAt[bool](col, int(row))
	case types.T_int8:
		return vector.GetFixedAt[int8](col, int(row))
	case types.T_int16:
		return vector.GetFixedAt[int16](col, int(row))
	case types.T_int32:
		return vector.GetFixedAt[int32](col, int(row))
	case types.T_int64:
		return vector.GetFixedAt[int64](col, int(row))
	case types.T_uint8:
		return vector.GetFixedAt[uint8](col, int(row))
	case types.T_uint16:
		return vector.GetFixedAt[uint16](col, int(row))
	case types.T_uint32:
		return vector.GetFixedAt[uint32](col, int(row))
	case types.T_uint64:
		return vector.GetFixedAt[uint64](col, int(row))
	case types.T_decimal64:
		return vector.GetFixedAt[types.Decimal64](col, int(row))
	case types.T_decimal128:
		return vector.GetFixedAt[types.Decimal128](col, int(row))
	case types.T_uuid:
		return vector.GetFixedAt[types.Uuid](col, int(row))
	case types.T_float32:
		return vector.GetFixedAt[float32](col, int(row))
	case types.T_float64:
		return vector.GetFixedAt[float64](col, int(row))
	case types.T_date:
		return vector.GetFixedAt[types.Date](col, int(row))
	case types.T_time:
		return vector.GetFixedAt[types.Time](col, int(row))
	case types.T_datetime:
		return vector.GetFixedAt[types.Datetime](col, int(row))
	case types.T_timestamp:
		return vector.GetFixedAt[types.Timestamp](col, int(row))
	case types.T_enum:
		return vector.GetFixedAt[types.Enum](col, int(row))
	case types.T_TS:
		return vector.GetFixedAt[types.TS](col, int(row))
	case types.T_Rowid:
		return vector.GetFixedAt[types.Rowid](col, int(row))
	case types.T_Blockid:
		return vector.GetFixedAt[types.Blockid](col, int(row))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text:
		return col.GetBytesAt(int(row))
	default:
		//return vector.ErrVecTypeNotSupport
		panic(any("No Support"))
	}
}

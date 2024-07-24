// Copyright 2023 Matrix Origin
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

package compile

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

/*
fuzzyCheck use to contains some info to run a background SQL when
fuzzy filter can not draw a definite conclusion for duplicate check
*/

func newFuzzyCheck(n *plan.Node) (*fuzzyCheck, error) {
	tblName := n.TableDef.GetName()
	dbName := n.ObjRef.GetSchemaName()

	if tblName == "" || dbName == "" {
		return nil, moerr.NewInternalErrorNoCtx("fuzzyfilter failed to get the db/tbl name")
	}

	f := reuse.Alloc[fuzzyCheck](nil)
	f.tbl = tblName
	f.db = dbName
	f.attr = n.TableDef.Pkey.PkeyColName

	for _, c := range n.TableDef.Cols {
		if c.Name == n.TableDef.Pkey.PkeyColName {
			f.col = c
		}
	}

	// compound key could be primary key(a, b, c...) or unique key(a, b, c ...)
	// for Decimal type, we need colDef to get the scale
	if n.TableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		f.isCompound = true
		f.compoundCols = f.sortColDef(n.TableDef.Pkey.Names, n.TableDef.Cols)
	}

	// for the case like create unique index for existed table,
	// We can only get the table definition of the hidden table.
	// How the original table defines this unique index (for example, which columns are used and whether it is composite) can NOT be confirmed,
	// that introduces some strange logic, and obscures the meaning of some fields, such as fuzzyCheck.isCompound
	if catalog.IsHiddenTable(tblName) && n.Fuzzymessage == nil {
		f.onlyInsertHidden = true
	}

	if n.Fuzzymessage != nil {
		if len(n.Fuzzymessage.ParentUniqueCols) > 1 {
			f.isCompound = true
			f.tbl = n.Fuzzymessage.ParentTableName
			f.compoundCols = n.Fuzzymessage.ParentUniqueCols
		} else {
			f.col = n.Fuzzymessage.ParentUniqueCols[0]
		}
	}

	return f, nil
}

func (f fuzzyCheck) TypeName() string {
	return "compile.fuzzyCheck"
}

func (f *fuzzyCheck) reset() {
	f.condition = ""
	f.cnt = 0
}

func (f *fuzzyCheck) clear() {
	f.db = ""
	f.tbl = ""
	f.attr = ""
	f.condition = ""
	f.isCompound = false
	f.onlyInsertHidden = false
	f.col = nil
	f.compoundCols = nil
	f.cnt = 0
}

func (f *fuzzyCheck) release() {
	reuse.Free[fuzzyCheck](f, nil)
}

// fill will generate condition for background SQL to check if duplicate constraint is satisfied
// for compound primary key, the condtion will look like : "attr_1=x1 and attr_2=y1 and ... attr_n = yn or attr1=x2 and attr=y2 ... "
// and its background SQL will look like : select pkAttrs, count(*) as cnt from db.tbl where **cont** having cnt > 1;
//
// for non compound primary key, cond will look like : "x1, x2, x3, ..."
// and its background SQL will look like :  select pkAttr, count(*) as cnt from db.tbl where pkAttr in (**cond**) having cnt > 1;
//
// for more info, refer func backgroundSQLCheck
func (f *fuzzyCheck) fill(ctx context.Context, bat *batch.Batch) error {
	var collision [][]string
	var err error

	toCheck := bat.GetVector(0)
	f.cnt = bat.RowCount()

	f.isCompound, err = f.recheckIfCompound(toCheck)
	if err != nil {
		return err
	}

	if !f.onlyInsertHidden {
		if err := f.firstlyCheck(ctx, toCheck); err != nil {
			return err // fail to pass duplicate constraint
		}
	}

	collision, err = f.genCollsionKeys(toCheck)
	if err != nil {
		return err
	}

	// generate codition used in background SQL
	if !f.onlyInsertHidden {
		if !f.isCompound {
			f.condition = strings.Join(collision[0], ", ")
		} else {
			// not using __mo_cpkey_col search directly because efficiency considerations,
			// HOWEVER This part of the code is still redundant, because currently plan can not support sql like
			//
			// SELECT pk1, pk2, COUNT(*) AS cnt
			// FROM tbl
			// WHERE (pk1, pk2) IN ((1, 1), (2, 1))
			// GROUP BY pk1, pk2
			// HAVING cnt > 1;
			//
			// otherwise, the code will be much more cleaner
			var all bytes.Buffer
			var one bytes.Buffer
			var i int
			var j int

			lastRow := len(collision[0]) - 1

			cAttrs := make([]string, len(f.compoundCols))
			for k, c := range f.compoundCols {
				if c == nil {
					panic("compoundCols should not have nil element")
				}
				cAttrs[k] = c.Name
			}

			for i = 0; i < lastRow; i++ {
				one.Reset()
				one.WriteByte('(')
				// one compound primary key has multiple conditions, use "and" to join them
				for j = 0; j < len(cAttrs)-1; j++ {
					one.WriteString(fmt.Sprintf("%s = %s and ", cAttrs[j], collision[j][i]))
				}

				// the last condition does not need to be followed by "and"
				one.WriteString(fmt.Sprintf("%s = %s", cAttrs[j], collision[j][i]))
				one.WriteByte(')')
				if _, err = one.WriteTo(&all); err != nil {
					return err
				}

				// use or join each compound primary keys
				all.WriteString(" or ")
			}

			// if only have one collision key, there will no "or", same as the last collision key
			for j = 0; j < len(cAttrs)-1; j++ {
				one.WriteString(fmt.Sprintf("%s = %s and ", cAttrs[j], collision[j][lastRow]))
			}

			one.WriteString(fmt.Sprintf("%s = %s", cAttrs[j], collision[j][lastRow]))
			if _, err = one.WriteTo(&all); err != nil {
				return err
			}
			f.condition = all.String()
		}
	} else {
		keys := collision[0]
		f.condition = strings.Join(keys, ", ")
	}

	return nil
}

func (f *fuzzyCheck) firstlyCheck(ctx context.Context, toCheck *vector.Vector) error {
	kcnt := make(map[string]int)

	if !f.isCompound {
		pkey, err := f.format(toCheck)
		if err != nil {
			return err
		}
		for _, k := range pkey {
			kcnt[k]++
		}
	} else {
		for i := 0; i < toCheck.Length(); i++ {
			b := toCheck.GetRawBytesAt(i)
			t, err := types.Unpack(b)
			if err != nil {
				return err
			}
			scales := make([]int32, len(f.compoundCols))
			for i, c := range f.compoundCols {
				if c == nil {
					panic("compoundCols should not have nil element")
				}
				scales[i] = c.Typ.Scale
			}
			es := t.ErrString(scales)
			kcnt[es]++
		}
	}

	// firstly check if contains duplicate
	for k, cnt := range kcnt {
		if cnt > 1 {
			ds, e := strconv.Unquote(k)
			if e != nil {
				return moerr.NewDuplicateEntry(ctx, k, f.attr)
			} else {
				return moerr.NewDuplicateEntry(ctx, ds, f.attr)
			}
		}
	}
	return nil
}

// genCollsionKeys return [][]string to store the string of collsion keys, it will check if
// collision keys are duplicates with each other, if do dup, no need to run background SQL
func (f *fuzzyCheck) genCollsionKeys(toCheck *vector.Vector) ([][]string, error) {
	var keys [][]string
	if !f.isCompound {
		keys = make([][]string, 1)
	} else {
		keys = make([][]string, len(f.compoundCols))
	}

	if !f.onlyInsertHidden {
		if !f.isCompound {
			pkey, err := f.format(toCheck)
			if err != nil {
				return nil, err
			}
			keys[0] = pkey
		} else {
			scales := make([]int32, len(f.compoundCols))
			for i, c := range f.compoundCols {
				scales[i] = c.Typ.Scale
			}
			for i := 0; i < toCheck.Length(); i++ {
				b := toCheck.GetRawBytesAt(i)
				t, err := types.Unpack(b)
				if err != nil {
					return nil, err
				}
				s := t.SQLStrings(scales)
				for j := 0; j < len(s); j++ {
					keys[j] = append(keys[j], s[j])
				}
			}
		}
	} else {
		pkey, err := f.format(toCheck)
		if err != nil {
			return nil, err
		}
		keys[0] = pkey
	}

	return keys, nil
}

// backgroundSQLCheck launches a background SQL to check if there are any duplicates
func (f *fuzzyCheck) backgroundSQLCheck(c *Compile) error {
	var duplicateCheckSql string

	if !f.onlyInsertHidden {
		if !f.isCompound {
			duplicateCheckSql = fmt.Sprintf(fuzzyNonCompoundCheck, f.attr, f.db, f.tbl, f.attr, f.condition, f.attr)
		} else {
			cAttrs := make([]string, len(f.compoundCols))
			for k, c := range f.compoundCols {
				cAttrs[k] = c.Name
			}
			attrs := strings.Join(cAttrs, ", ")
			duplicateCheckSql = fmt.Sprintf(fuzzyCompoundCheck, attrs, f.db, f.tbl, f.condition, attrs)
		}
	} else {
		duplicateCheckSql = fmt.Sprintf(fuzzyNonCompoundCheck, f.attr, f.db, f.tbl, f.attr, f.condition, f.attr)
	}

	res, err := c.runSqlWithResult(duplicateCheckSql)
	if err != nil {
		c.proc.Errorf(c.proc.Ctx, "The sql that caused the fuzzy check background SQL failed is %s, and generated background sql is %s", c.sql, duplicateCheckSql)
		return err
	}
	defer res.Close()

	if res.Batches != nil {
		vs := res.Batches[0].Vecs
		if vs != nil && vs[0].Length() > 0 { // do dup
			toCheck := vs[0]
			if !f.isCompound {
				f.adjustDecimalScale(toCheck)
				if dupKey, e := f.format(toCheck); e != nil {
					err = e
				} else {
					ds, e := strconv.Unquote(dupKey[0])
					if e != nil {
						err = moerr.NewDuplicateEntry(c.proc.Ctx, dupKey[0], f.attr)
					} else {
						err = moerr.NewDuplicateEntry(c.proc.Ctx, ds, f.attr)
					}
				}
			} else {
				if t, e := types.Unpack(toCheck.GetBytesAt(0)); e != nil {
					err = e
				} else {
					scales := make([]int32, len(f.compoundCols))
					for i, col := range f.compoundCols {
						if col != nil {
							scales[i] = col.Typ.Scale
						} else {
							scales[i] = 0
						}
					}
					err = moerr.NewDuplicateEntry(c.proc.Ctx, t.ErrString(scales), f.attr)
				}
			}
		}
	}

	return err
}

// -----------------------------utils-----------------------------------

// make sure that the attr sort by define way
func (f *fuzzyCheck) sortColDef(toSelect []string, cols []*plan.ColDef) []*plan.ColDef {
	ccols := make([]*plan.ColDef, len(toSelect))
	nmap := make(map[string]int)
	for i, n := range toSelect {
		nmap[n] = i
	}
	for _, c := range cols {
		if i, ok := nmap[c.Name]; ok {
			ccols[i] = c
		}
	}
	return ccols
}

// format format strings from vector
func (f *fuzzyCheck) format(toCheck *vector.Vector) ([]string, error) {
	var ss []string
	typ := toCheck.GetType()

	for i := 0; i < toCheck.Length(); i++ {
		s, err := vectorToString(toCheck, i)
		if err != nil {
			return nil, err
		}
		ss = append(ss, s)
	}

	if f.isCompound {
		for i, s := range ss {
			ss[i] = "unhex('" + hex.EncodeToString([]byte(s)) + "')"
		}
		return ss, nil
	}

	// for table that like CREATE TABLE t1( b CHAR(10), PRIMARY KEY);
	// with : insert into t1 values('ab'), ('ab');
	// background SQL condition should be b='ab' instead of b=ab, as well as time types
	switch typ.Oid {
	// date and time
	case types.T_date, types.T_time, types.T_datetime, types.T_timestamp:
		for i, str := range ss {
			ss[i] = strconv.Quote(str)
		}
		return ss, nil

	// string family but not include binary
	case types.T_char, types.T_varchar, types.T_varbinary, types.T_text, types.T_uuid, types.T_binary, types.T_datalink:
		for i, str := range ss {
			ss[i] = strconv.Quote(str)
		}
		return ss, nil
	default:
		return ss, nil
	}
}

func vectorToString(vec *vector.Vector, rowIndex int) (string, error) {
	if nulls.Any(vec.GetNulls()) {
		return "", nil
	}
	switch vec.GetType().Oid {
	case types.T_bool:
		flag := vector.GetFixedAt[bool](vec, rowIndex)
		if flag {
			return "true", nil
		}
		return "false", nil
	case types.T_bit:
		return fmt.Sprintf("%v", vector.GetFixedAt[uint64](vec, rowIndex)), nil
	case types.T_int8:
		return fmt.Sprintf("%v", vector.GetFixedAt[int8](vec, rowIndex)), nil
	case types.T_int16:
		return fmt.Sprintf("%v", vector.GetFixedAt[int16](vec, rowIndex)), nil
	case types.T_int32:
		return fmt.Sprintf("%v", vector.GetFixedAt[int32](vec, rowIndex)), nil
	case types.T_int64:
		return fmt.Sprintf("%v", vector.GetFixedAt[int64](vec, rowIndex)), nil
	case types.T_uint8:
		return fmt.Sprintf("%v", vector.GetFixedAt[uint8](vec, rowIndex)), nil
	case types.T_uint16:
		return fmt.Sprintf("%v", vector.GetFixedAt[uint16](vec, rowIndex)), nil
	case types.T_uint32:
		return fmt.Sprintf("%v", vector.GetFixedAt[uint32](vec, rowIndex)), nil
	case types.T_uint64:
		return fmt.Sprintf("%v", vector.GetFixedAt[uint64](vec, rowIndex)), nil
	case types.T_float32:
		return fmt.Sprintf("%v", vector.GetFixedAt[float32](vec, rowIndex)), nil
	case types.T_float64:
		return fmt.Sprintf("%v", vector.GetFixedAt[float64](vec, rowIndex)), nil
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_text, types.T_blob, types.T_datalink:
		return vec.GetStringAt(rowIndex), nil
	case types.T_array_float32:
		return types.ArrayToString[float32](vector.GetArrayAt[float32](vec, rowIndex)), nil
	case types.T_array_float64:
		return types.ArrayToString[float64](vector.GetArrayAt[float64](vec, rowIndex)), nil
	case types.T_decimal64:
		val := vector.GetFixedAt[types.Decimal64](vec, rowIndex)
		return val.Format(vec.GetType().Scale), nil
	case types.T_decimal128:
		val := vector.GetFixedAt[types.Decimal128](vec, rowIndex)
		return val.Format(vec.GetType().Scale), nil
	case types.T_json:
		val := vec.GetBytesAt(rowIndex)
		byteJson := types.DecodeJson(val)
		return byteJson.String(), nil
	case types.T_uuid:
		val := vector.GetFixedAt[types.Uuid](vec, rowIndex)
		return val.String(), nil
	case types.T_date:
		val := vector.GetFixedAt[types.Date](vec, rowIndex)
		return val.String(), nil
	case types.T_time:
		val := vector.GetFixedAt[types.Time](vec, rowIndex)
		return val.String(), nil
	case types.T_timestamp:
		loc := time.Local
		val := vector.GetFixedAt[types.Timestamp](vec, rowIndex)
		return val.String2(loc, vec.GetType().Scale), nil
	case types.T_datetime:
		val := vector.GetFixedAt[types.Datetime](vec, rowIndex)
		return val.String2(vec.GetType().Scale), nil
	case types.T_enum:
		return fmt.Sprintf("%v", vector.GetFixedAt[uint16](vec, rowIndex)), nil
	default:
		return "", moerr.NewInternalErrorNoCtx("fuzzy filter can not parse correct string for type id : %d", vec.GetType().Oid)
	}
}

// for decimal type in batch.vector that read from pipeline, its scale is empty
// so we have to fill it with tableDef from plan
func (f *fuzzyCheck) adjustDecimalScale(toCheck *vector.Vector) {
	typ := toCheck.GetType()
	switch typ.Oid {
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
		if typ.Scale == 0 {
			typ.Scale = f.col.Typ.Scale
		}
	default:
	}
}

func (f *fuzzyCheck) recheckIfCompound(toCheck *vector.Vector) (bool, error) {

	if f.onlyInsertHidden {
		// for the case that create compound unique index for existed table
		// can only detact if it is compound by the schema of the vector

		foo, err := vectorToString(toCheck, 0)
		if err != nil {
			return false, err
		}

		if _, _, schema, err := types.DecodeTuple([]byte(foo)); err == nil {
			f.compoundCols = make([]*plan.ColDef, len(schema))
			return len(schema) > 1, nil
		}
	}

	return f.isCompound, nil
}

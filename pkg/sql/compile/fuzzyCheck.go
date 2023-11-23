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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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

	f := new(fuzzyCheck)
	f.tbl = f.wrapup(tblName)
	f.db = f.wrapup(dbName)
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
	} else if n.TableDef.ParentUniqueCols != nil {
		if len(n.TableDef.ParentUniqueCols) > 1 {
			f.isCompound = true
			f.tbl = n.TableDef.ParentTblName // search for data table but not index table
			f.compoundCols = n.TableDef.ParentUniqueCols
		} else {
			f.col = n.TableDef.ParentUniqueCols[0]
		}
	}

	return f, nil
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
	toCheck := bat.GetVector(0)
	f.cnt = bat.RowCount()

	if err := f.firstlyCheck(ctx, toCheck); err != nil {
		return err // fail to pass duplicate constraint
	}

	pkeys, err := f.genCollsionKeys(toCheck)
	if err != nil {
		return err
	}

	// generate codition used in background SQL
	if !f.isCompound {
		f.condition = strings.Join(pkeys[0], ", ")
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

		var lastRow = len(pkeys[0]) - 1

		cAttrs := make([]string, len(f.compoundCols))
		for k, c := range f.compoundCols {
			cAttrs[k] = c.Name
		}

		for i = 0; i < lastRow; i++ {
			one.Reset()
			one.WriteByte('(')
			// one compound primary key has multiple conditions, use "and" to join them
			for j = 0; j < len(cAttrs)-1; j++ {
				one.WriteString(fmt.Sprintf("%s = %s and ", cAttrs[j], pkeys[j][i]))
			}

			// the last condition does not need to be followed by "and"
			one.WriteString(fmt.Sprintf("%s = %s", cAttrs[j], pkeys[j][i]))
			one.WriteByte(')')
			one.WriteTo(&all)

			// use or join each compound primary keys
			all.WriteString(" or ")
		}

		// if only have one collision key, there will no "or", same as the last collision key
		for j = 0; j < len(cAttrs)-1; j++ {
			one.WriteString(fmt.Sprintf("%s = %s and ", cAttrs[j], pkeys[j][lastRow]))
		}

		one.WriteString(fmt.Sprintf("%s = %s", cAttrs[j], pkeys[j][lastRow]))
		one.WriteTo(&all)
		f.condition = all.String()
	}

	return nil
}

func (f *fuzzyCheck) firstlyCheck(ctx context.Context, toCheck *vector.Vector) error {
	kcnt := make(map[string]int)

	if !f.isCompound {
		pkey, err := f.formatNonCompound(toCheck, true)
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
			es := t.ErrString()
			kcnt[es]++
		}
	}

	// firstly check if contains duplicate
	for k, cnt := range kcnt {
		if cnt > 1 {
			return moerr.NewDuplicateEntry(ctx, k, f.attr)
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

	if !f.isCompound {
		pkey, err := f.formatNonCompound(toCheck, false)
		if err != nil {
			return nil, err
		}
		keys[0] = pkey
	} else {
		for i := 0; i < toCheck.Length(); i++ {
			b := toCheck.GetRawBytesAt(i)
			t, err := types.Unpack(b)
			if err != nil {
				return nil, err
			}
			s := t.SQLStrings()
			for j := 0; j < len(s); j++ {
				keys[j] = append(keys[j], s[j])
			}
		}
	}

	return keys, nil
}

// backgroundSQLCheck launches a background SQL to check if there are any duplicates
func (f *fuzzyCheck) backgroundSQLCheck(c *Compile) error {
	var duplicateCheckSql string
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

	res, err := c.runSqlWithResult(duplicateCheckSql)
	if err != nil {
		logutil.Errorf("The sql that caused the fuzzy check background SQL failed is %s, and generated background sql is %s", c.sql, duplicateCheckSql)
		return err
	}
	defer res.Close()

	if res.Batches != nil {
		vs := res.Batches[0].Vecs
		if vs != nil && vs[0].Length() > 0 { // do dup
			toCheck := vs[0]
			if !f.isCompound {
				f.adjustDecimalScale(toCheck)
				if dupKey, e := f.formatNonCompound(toCheck, true); e != nil {
					err = e
				} else {
					err = moerr.NewDuplicateEntry(c.ctx, dupKey[0], f.attr)
				}
			} else {
				if t, e := types.Unpack(toCheck.GetBytesAt(0)); e != nil {
					err = e
				} else {
					err = moerr.NewDuplicateEntry(c.ctx, t.ErrString(), f.attr)
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

func (f *fuzzyCheck) wrapup(name string) string {
	return "`" + name + "`"
}

// for table that like CREATE TABLE t1( b CHAR(10), PRIMARY KEY);
// with : insert into t1 values('ab'), ('ab');
// background SQL condition should be b='ab' instead of b=ab
// other time format such as bool, float, decimal
func (f *fuzzyCheck) formatNonCompound(toCheck *vector.Vector, useInErr bool) ([]string, error) {
	var ss []string
	s := strings.Trim(toCheck.String(), "[]")
	typ := toCheck.GetType()

	switch typ.Oid {
	case types.T_datetime, types.T_timestamp:
		ss = f.handletimesType(toCheck)
	default:
		ss = strings.Split(s, " ")
	}

	if useInErr {
		switch typ.Oid {
		// decimal
		case types.T_decimal64:
			ds := make([]string, 0)
			for i := 0; i < toCheck.Length(); i++ {
				val := vector.GetFixedAt[types.Decimal64](toCheck, i)
				ds = append(ds, val.Format(typ.Scale))
			}
			return ds, nil
		case types.T_decimal128:
			ds := make([]string, 0)
			for i := 0; i < toCheck.Length(); i++ {
				val := vector.GetFixedAt[types.Decimal128](toCheck, i)
				ds = append(ds, val.Format(typ.Scale))
			}
			return ds, nil
		case types.T_decimal256:
			ds := make([]string, 0)
			for i := 0; i < toCheck.Length(); i++ {
				val := vector.GetFixedAt[types.Decimal256](toCheck, i)
				ds = append(ds, val.Format(typ.Scale))
			}
			return ds, nil
		}
		return ss, nil
	} else {
		switch typ.Oid {
		// date and time
		case types.T_date, types.T_time, types.T_datetime, types.T_timestamp:
			for i, str := range ss {
				ss[i] = "'" + str + "'"
			}
			return ss, nil

		// string family but not include binary
		case types.T_char, types.T_varchar, types.T_varbinary, types.T_text, types.T_uuid, types.T_binary:
			for i, str := range ss {
				ss[i] = "'" + str + "'"
			}
			return ss, nil

		// case types.T_enum:
		// 	enumValues := strings.Split(f.col.Typ.Enumvalues, ",")
		// 	f.isEnum = true
		// 	for i, str := range ss {
		// 		num, err := strconv.Atoi(str)
		// 		if err != nil {
		// 			return nil, err
		// 		}
		// 		ss[i] = fmt.Sprintf("'%s'", enumValues[num-1])
		// 	}
		// 	return ss, nil
		// decimal
		case types.T_decimal64, types.T_decimal128, types.T_decimal256:
			return ss, nil

		// bool, int, float
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_int128,
			types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_uint128,
			types.T_float32, types.T_float64, types.T_bool:
			return ss, nil
		default:
			return nil, moerr.NewInternalErrorNoCtx("fuzzy filter can not parse correct string for type id : %d", typ.Oid)
		}
	}
}

// datime time and timestamp type can not split by space directly
func (f *fuzzyCheck) handletimesType(toCheck *vector.Vector) []string {
	result := []string{}
	typ := toCheck.GetType()

	if typ.Oid == types.T_timestamp {
		loc := time.Local
		for i := 0; i < toCheck.Length(); i++ {
			ts := vector.GetFixedAt[types.Timestamp](toCheck, i)
			result = append(result, ts.String2(loc, typ.Scale))
		}
	} else {
		for i := 0; i < toCheck.Length(); i++ {
			ts := vector.GetFixedAt[types.Datetime](toCheck, i)
			result = append(result, ts.String2(typ.Scale))
		}
	}
	return result
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

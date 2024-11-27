// Copyright 2024 Matrix Origin
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
package postdml

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "postdml"

var (
	fulltextInsertSqlFmt    = "INSERT INTO %s SELECT f.* FROM %s as %s CROSS APPLY fulltext_index_tokenize('%s', %s, %s) as f WHERE %s IN (%s)"
	fulltextDeleteSqlFmt    = "DELETE FROM %s WHERE doc_id IN (%s)"
	fulltextDeleteAllSqlFmt = "DELETE FROM %s"
)

func (postdml *PostDml) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
}

func (postdml *PostDml) OpType() vm.OpType {
	return vm.PostDml
}

func (postdml *PostDml) Prepare(proc *process.Process) error {
	if postdml.OpAnalyzer == nil {
		postdml.OpAnalyzer = process.NewAnalyzer(postdml.GetIdx(), postdml.IsFirst, postdml.IsLast, "postdml")
	} else {
		postdml.OpAnalyzer.Reset()
	}

	postdml.ctr.affectedRows = 0
	return nil
}

func (postdml *PostDml) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := postdml.OpAnalyzer

	result, err := vm.ChildrenCall(postdml.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}
	if result.Batch == nil || result.Batch.IsEmpty() {
		return result, nil
	}

	if err = postdml.runPostDml(proc, result); err != nil {
		return vm.CancelResult, err
	}

	return result, nil
}

func (postdml *PostDml) runPostDml(proc *process.Process, result vm.CallResult) error {

	var in_list []string
	bat := result.Batch
	pkvec := bat.Vecs[postdml.PostDmlCtx.PrimaryKeyIdx]
	pkTyp := pkvec.GetType()

	var values string
	if !postdml.PostDmlCtx.IsDeleteWithoutFilters {
		in_list = make([]string, 0, bat.RowCount())
		for i := 0; i < bat.RowCount(); i++ {
			pkey, err := GetAnyAsString(pkvec, i)
			if err != nil {
				return err
			}

			switch pkTyp.Oid {
			case types.T_date, types.T_datetime, types.T_timestamp, types.T_time, types.T_uuid,
				types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json,
				types.T_blob, types.T_text, types.T_datalink:
				pkey = "'" + pkey + "'"
			case types.T_array_float32, types.T_array_float64:
				return moerr.NewInternalError(proc.Ctx, "array cannot be primary key")
			}

			in_list = append(in_list, pkey)
		}
		values = strings.Join(in_list, ",")
	}

	// you may add new context to generate post dml SQL
	if postdml.PostDmlCtx.FullText != nil {
		ftctx := postdml.PostDmlCtx.FullText

		dbname := postdml.PostDmlCtx.Ref.GetSchemaName()

		alias := "src"
		sourcetbl := fmt.Sprintf("`%s`.`%s`", dbname, ftctx.SourceTableName)
		indextbl := fmt.Sprintf("`%s`.`%s`", dbname, ftctx.IndexTableName)
		pkcolname := fmt.Sprintf("%s.%s", alias, postdml.PostDmlCtx.PrimaryKeyName)

		var parts []string
		for _, p := range ftctx.Parts {
			parts = append(parts, fmt.Sprintf("%s.%s", alias, p))
		}

		if postdml.PostDmlCtx.IsDelete {
			var sql string
			// append Delete SQL
			if postdml.PostDmlCtx.IsDeleteWithoutFilters {
				// delete all
				sql = fmt.Sprintf(fulltextDeleteAllSqlFmt, indextbl)
			} else {
				sql = fmt.Sprintf(fulltextDeleteSqlFmt, indextbl, values)
			}

			//logutil.Infof("POST DELETE SQL : %s", sql)
			proc.Base.PostDmlSqlList.Append(sql)
		}

		if postdml.PostDmlCtx.IsInsert {
			sql := fmt.Sprintf(fulltextInsertSqlFmt, indextbl, sourcetbl, alias,
				ftctx.AlgoParams, pkcolname, strings.Join(parts, ", "),
				pkcolname, values)
			//logutil.Infof("POST INSERT SQL : %s", sql)
			proc.Base.PostDmlSqlList.Append(sql)
		}
	}

	return nil
}

func GetAnyAsString(vec *vector.Vector, i int) (string, error) {
	switch vec.GetType().Oid {
	case types.T_bool:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[bool](vec, i)), nil
	case types.T_bit:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[uint64](vec, i)), nil
	case types.T_int8:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[int8](vec, i)), nil
	case types.T_int16:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[int16](vec, i)), nil
	case types.T_int32:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[int32](vec, i)), nil
	case types.T_int64:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[int64](vec, i)), nil
	case types.T_uint8:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[uint8](vec, i)), nil
	case types.T_uint16:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[uint16](vec, i)), nil
	case types.T_uint32:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[uint32](vec, i)), nil
	case types.T_uint64:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[uint64](vec, i)), nil
	case types.T_float32:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[float32](vec, i)), nil
	case types.T_float64:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[float64](vec, i)), nil
	case types.T_date:
		return vector.GetFixedAtNoTypeCheck[types.Date](vec, i).String(), nil
	case types.T_datetime:
		return vector.GetFixedAtNoTypeCheck[types.Datetime](vec, i).String(), nil
	case types.T_time:
		return vector.GetFixedAtNoTypeCheck[types.Time](vec, i).String(), nil
	case types.T_timestamp:
		return vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, i).String(), nil
	case types.T_enum:
		return vector.GetFixedAtNoTypeCheck[types.Enum](vec, i).String(), nil
	case types.T_decimal64:
		return vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, i).Format(vec.GetType().Scale), nil
	case types.T_decimal128:
		return vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, i).Format(vec.GetType().Scale), nil
	case types.T_uuid:
		return vector.GetFixedAtNoTypeCheck[types.Uuid](vec, i).String(), nil
	case types.T_TS:
		return vector.GetFixedAtNoTypeCheck[types.TS](vec, i).ToString(), nil
	case types.T_Rowid:
		return vector.GetFixedAtNoTypeCheck[types.Rowid](vec, i).String(), nil
	case types.T_Blockid:
		return "", moerr.NewInternalErrorNoCtx("GetAnyAsString: block_id not supported") // vector.GetFixedAtNoTypeCheck[types.Blockid](vec, i)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		return string(vec.GetBytesAt(i)), nil
	}
	return "", moerr.NewInternalErrorNoCtx("GetAnyAsString: invalid type")
}

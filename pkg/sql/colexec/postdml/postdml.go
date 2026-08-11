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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
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
	postdml.cycleCheck = nil
	if postdml.PostDmlCtx.ReplaceCycleCheck != "" {
		postdml.cycleCheck = new(replaceCycleCheckConfig)
		if err := json.Unmarshal([]byte(postdml.PostDmlCtx.ReplaceCycleCheck), postdml.cycleCheck); err != nil {
			return moerr.NewInternalErrorf(proc.Ctx, "invalid REPLACE cycle check: %v", err)
		}
	}
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
		var noBackslashEscapes bool
		var sqlModeResolved bool
		for i := 0; i < bat.RowCount(); i++ {
			pkey, err := GetAnyAsString(pkvec, i)
			if err != nil {
				return err
			}

			switch pkTyp.Oid {
			case types.T_date, types.T_datetime, types.T_timestamp, types.T_time, types.T_uuid,
				types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json,
				types.T_blob, types.T_text, types.T_datalink:
				if !sqlModeResolved {
					noBackslashEscapes = postDmlNoBackslashEscapes(proc)
					sqlModeResolved = true
				}
				if noBackslashEscapes {
					pkey = "'" + strings.ReplaceAll(pkey, "'", "''") + "'"
				} else {
					pkey = sqlquote.String(pkey)
				}
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
	if postdml.cycleCheck != nil {
		if err := postdml.appendReplaceCycleChecks(proc, bat); err != nil {
			return err
		}
	}

	return nil
}

func (postdml *PostDml) appendReplaceCycleChecks(proc *process.Process, bat *batch.Batch) error {
	config := postdml.cycleCheck
	rowPredicates := make([]string, 0, bat.RowCount())
	for row := 0; row < bat.RowCount(); row++ {
		parts := make([]string, 0, len(config.PrimaryKey))
		skip := false
		for _, pk := range config.PrimaryKey {
			if pk.Pos < 0 || int(pk.Pos) >= len(bat.Vecs) {
				return moerr.NewInternalError(proc.Ctx, "REPLACE cycle check primary key is out of range")
			}
			vec := bat.Vecs[pk.Pos]
			if vec.IsNull(uint64(row)) {
				skip = true
				break
			}
			literal, err := postDmlSQLLiteral(proc, vec, row)
			if err != nil {
				return err
			}
			parts = append(parts, fmt.Sprintf("%s.%s = %s",
				quotePostDmlIdentifier(config.ChildTable), quotePostDmlIdentifier(pk.Name), literal))
		}
		if !skip {
			rowPredicates = append(rowPredicates, "("+strings.Join(parts, " and ")+")")
		}
	}
	if len(rowPredicates) == 0 {
		return nil
	}
	childTable := quotePostDmlIdentifier(config.ChildSchema) + "." + quotePostDmlIdentifier(config.ChildTable)
	for _, fk := range config.ForeignKeys {
		if len(fk.ChildCols) == 0 || len(fk.ChildCols) != len(fk.ParentCols) {
			return moerr.NewInternalError(proc.Ctx, "REPLACE cycle check foreign key is incomplete")
		}
		childCols := make([]string, len(fk.ChildCols))
		parentCols := make([]string, len(fk.ParentCols))
		nonNull := make([]string, len(fk.ChildCols))
		for i := range fk.ChildCols {
			childCols[i] = quotePostDmlIdentifier(config.ChildTable) + "." + quotePostDmlIdentifier(fk.ChildCols[i])
			parentCols[i] = quotePostDmlIdentifier(fk.ParentTable) + "." + quotePostDmlIdentifier(fk.ParentCols[i])
			nonNull[i] = childCols[i] + " is not null"
		}
		parentTable := quotePostDmlIdentifier(fk.ParentSchema) + "." + quotePostDmlIdentifier(fk.ParentTable)
		sql := fmt.Sprintf(
			"select count(*) = 0 from (select distinct %s from %s where (%s) and %s except select distinct %s from %s) as __mo_fk_check_source",
			strings.Join(childCols, ","), childTable, strings.Join(rowPredicates, " or "), strings.Join(nonNull, " and "),
			strings.Join(parentCols, ","), parentTable)
		proc.Base.PostDmlSqlList.Append("REPLACE_CYCLE_CHECK:" + sql)
	}
	return nil
}

func quotePostDmlIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func postDmlSQLLiteral(proc *process.Process, vec *vector.Vector, row int) (string, error) {
	value, err := GetAnyAsString(vec, row)
	if err != nil {
		return "", err
	}
	switch vec.GetType().Oid {
	case types.T_date, types.T_datetime, types.T_timestamp, types.T_time, types.T_uuid,
		types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json,
		types.T_blob, types.T_text, types.T_datalink:
		if postDmlNoBackslashEscapes(proc) {
			return "'" + strings.ReplaceAll(value, "'", "''") + "'", nil
		}
		return sqlquote.String(value), nil
	case types.T_array_float32, types.T_array_float64:
		return "", moerr.NewInternalError(proc.Ctx, "array cannot be primary key")
	default:
		return value, nil
	}
}

func postDmlNoBackslashEscapes(proc *process.Process) bool {
	if proc == nil {
		return false
	}

	mode := proc.GetSessionInfo().SqlMode
	if resolver := proc.GetResolveVariableFunc(); resolver != nil {
		if value, err := resolver("sql_mode", true, false); err == nil {
			if sessionMode, ok := value.(string); ok {
				mode = sessionMode
			}
		}
	}
	return mysql.HasSQLMode(mode, "NO_BACKSLASH_ESCAPES")
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

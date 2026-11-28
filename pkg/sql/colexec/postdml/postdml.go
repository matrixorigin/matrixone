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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
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
		rowCount := bat.RowCount()
		in_list = make([]string, 0, rowCount)
		// The key type is the same on every row: decide how to quote it once.
		// A timestamp is written in the session time zone: the generated SQL
		// runs in this session, which casts the literal back with that zone.
		loc := postDmlTimeZone(proc)
		var quote, noBackslashEscapes bool
		switch pkTyp.Oid {
		case types.T_date, types.T_datetime, types.T_timestamp, types.T_time, types.T_uuid,
			types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json,
			types.T_blob, types.T_text, types.T_datalink:
			quote = true
			if rowCount > 0 {
				noBackslashEscapes = postDmlNoBackslashEscapes(proc)
			}
		case types.T_array_float32, types.T_array_float64:
			if rowCount > 0 {
				return moerr.NewInternalError(proc.Ctx, "array cannot be primary key")
			}
		}
		for i := 0; i < rowCount; i++ {
			pkey, err := GetAnyAsString(pkvec, i, loc)
			if err != nil {
				return err
			}
			if quote {
				if noBackslashEscapes {
					pkey = "'" + strings.ReplaceAll(pkey, "'", "''") + "'"
				} else {
					pkey = sqlquote.String(pkey)
				}
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
		sourcetbl := sqlquote.QualifiedIdent(dbname, ftctx.SourceTableName)
		indextbl := sqlquote.QualifiedIdent(dbname, ftctx.IndexTableName)
		pkcolname := sqlquote.QualifiedIdent(alias, postdml.PostDmlCtx.PrimaryKeyName)

		var parts []string
		for _, p := range ftctx.Parts {
			parts = append(parts, sqlquote.QualifiedIdent(alias, p))
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

// postDmlTimeZone is the zone the session casts a timestamp literal with,
// the same fallback the cast uses when the session sets none.
func postDmlTimeZone(proc *process.Process) *time.Location {
	if proc != nil && proc.GetSessionInfo().TimeZone != nil {
		return proc.GetSessionInfo().TimeZone
	}
	return time.Local
}

// GetAnyAsString returns the text of row i; a timestamp is written in loc.
func GetAnyAsString(vec *vector.Vector, i int, loc *time.Location) (string, error) {
	if !vector.SupportsAny(vec.GetType().Oid) {
		return "", moerr.NewInternalErrorNoCtx("GetAnyAsString: invalid type")
	}
	return string(types.AppendValueText(nil, *vec.GetType(), vector.GetAny(vec, i, false), loc)), nil
}

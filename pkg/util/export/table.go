// Copyright 2022 Matrix Origin
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

package export

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
)

type CsvOptions struct {
	FieldTerminator rune // like: ','
	EncloseRune     rune // like: '"'
	Terminator      rune // like: '\n'
}

var CommonCsvOptions = &CsvOptions{
	FieldTerminator: ',',
	EncloseRune:     '"',
	Terminator:      '\n',
}

type Column struct {
	Name    string
	Type    string
	Default string
	Comment string
}

func (col *Column) ToCreateSql() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("`%s` %s ", col.Name, col.Type))
	if len(col.Default) > 0 {
		sb.WriteString(fmt.Sprintf("DEFAULT %q ", col.Default))
	}
	sb.WriteString(fmt.Sprintf("COMMENT %q", col.Comment))
	return sb.String()
}

var _ batchpipe.HasName = (*Table)(nil)

var NormalTableEngine = "TABLE"
var ExternalTableEngine = "EXTERNAL"

type Table struct {
	Database         string
	Table            string
	Columns          []Column
	PrimaryKeyColumn []Column
	Engine           string
	Comment          string
	TableOptions     TableOptions
	PathBuilder      PathBuilder

	AccountColumn *Column
}

func (tbl *Table) GetName() string {
	return tbl.Table
}

type TableOptions interface {
	FormatDdl(ddl string) string
	// GetCreateOptions return option for `create {option}table`, which should end with ' '
	GetCreateOptions() string
	GetTableOptions(PathBuilder) string
}

func (tbl *Table) ToCreateSql(ifNotExists bool) string {

	const newLineCharacter = ",\n"
	sb := strings.Builder{}
	// create table
	sb.WriteString("CREATE ")
	switch strings.ToUpper(tbl.Engine) {
	case ExternalTableEngine:
		sb.WriteString(tbl.TableOptions.GetCreateOptions())
	default:
		panic(moerr.NewInternalError("NOT support engine: %s", tbl.Engine))
	}
	sb.WriteString("TABLE ")
	if ifNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	// table name
	sb.WriteString(fmt.Sprintf("`%s`.`%s`(", tbl.Database, tbl.Table))
	// columns
	for idx, col := range tbl.Columns {
		if idx > 0 {
			sb.WriteString(newLineCharacter)
		} else {
			sb.WriteRune('\n')
		}
		sb.WriteString(col.ToCreateSql())
	}
	// primary key
	if len(tbl.PrimaryKeyColumn) > 0 {
		sb.WriteString(newLineCharacter)
		sb.WriteString("PRIMARY KEY (`")
		for idx, col := range tbl.PrimaryKeyColumn {
			if idx > 0 {
				sb.WriteString(`, `)
			}
			sb.WriteString(fmt.Sprintf("`%s`", col.Name))
		}
		sb.WriteString(`)`)
	}
	sb.WriteString("\n)")
	sb.WriteString(tbl.TableOptions.GetTableOptions(tbl.PathBuilder))

	return sb.String()
}

type ViewOption func(view *View)

func (opt ViewOption) Apply(view *View) {
	opt(view)
}

type WhereCondition interface {
	String() string
}

type View struct {
	Database    string
	Table       string
	OriginTable *Table
	Columns     []Column
	Condition   WhereCondition
}

func WithColumn(c Column) ViewOption {
	return ViewOption(func(v *View) {
		v.Columns = append(v.Columns, c)
	})
}

func (tbl *View) ToCreateSql(ifNotExists bool) string {
	sb := strings.Builder{}
	// create table
	sb.WriteString("CREATE VIEW ")
	if ifNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	// table name
	sb.WriteString(fmt.Sprintf("`%s`.`%s` as ", tbl.Database, tbl.Table))
	sb.WriteString("select ")
	// columns
	for idx, col := range tbl.Columns {
		if idx > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("`%s`", col.Name))
	}
	sb.WriteString(fmt.Sprintf(" from `%s`.`%s` where ", tbl.OriginTable.Database, tbl.OriginTable.Table))
	sb.WriteString(tbl.Condition.String())

	return sb.String()
}

type Row struct {
	Table          *Table
	AccountIdx     int
	Columns        []string
	Name2ColumnIdx map[string]int
}

func (tbl *Table) GetRow() *Row {
	row := &Row{
		Table:          tbl,
		AccountIdx:     -1,
		Columns:        make([]string, len(tbl.Columns)),
		Name2ColumnIdx: make(map[string]int),
	}
	for idx, col := range tbl.Columns {
		row.Name2ColumnIdx[col.Name] = idx
	}
	if tbl.AccountColumn != nil {
		idx, exist := row.Name2ColumnIdx[tbl.AccountColumn.Name]
		if !exist {
			panic(moerr.NewInternalError("%s table missing %s column", tbl.GetName(), tbl.AccountColumn.Name))
		}
		row.AccountIdx = idx
	}
	return row
}

func (r *Row) Reset() {
	for idx := 0; idx < len(r.Columns); idx++ {
		r.Columns[idx] = ""
	}
}

// GetAccount return r.Columns[r.AccountIdx] if r.AccountIdx >= 0, else return "sys"
func (r *Row) GetAccount() string {
	if r.AccountIdx >= 0 {
		return r.Columns[r.AccountIdx]
	}
	return "sys"
}

func (r *Row) SetVal(col string, val string) {
	if idx, exist := r.Name2ColumnIdx[col]; !exist {
		logutil.Fatalf("column(%s) not exist in table(%s)", col, r.Table.Table)
	} else {
		r.Columns[idx] = val
	}
}

func (r *Row) SetFloat64(col string, val float64) {
	r.SetVal(col, fmt.Sprintf("%f", val))
}

func (r *Row) SetInt64(col string, val int64) {
	r.SetVal(col, fmt.Sprintf("%d", val))
}

// ToStrings output all column as string
func (r *Row) ToStrings() []string {
	for idx, col := range r.Columns {
		if len(col) == 0 {
			r.Columns[idx] = r.Table.Columns[idx].Default
		}
	}
	return r.Columns
}

var _ TableOptions = (*NoopTableOptions)(nil)

type NoopTableOptions struct{}

func (o NoopTableOptions) FormatDdl(ddl string) string        { return ddl }
func (o NoopTableOptions) GetCreateOptions() string           { return "" }
func (o NoopTableOptions) GetTableOptions(PathBuilder) string { return "" }

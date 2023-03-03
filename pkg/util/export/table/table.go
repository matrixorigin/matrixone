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

package table

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
)

const ExternalFilePath = "__mo_filepath"

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

type ColType int

const (
	TDatetime ColType = iota
	TUint64
	TInt64
	TFloat64
	TJson
	TText
	TVarchar
)

func (c *ColType) ToType() types.Type {
	switch *c {
	case TDatetime:
		typ := types.T_datetime.ToType()
		typ.Scale = 6
		return typ
	case TUint64:
		return types.T_uint64.ToType()
	case TInt64:
		return types.T_int64.ToType()
	case TFloat64:
		return types.T_float64.ToType()
	case TJson:
		return types.T_json.ToType()
	case TText:
		return types.T_text.ToType()
	case TVarchar:
		return types.T_varchar.ToType()
	default:
		panic("not support ColType")
	}
}

func (c *ColType) String(scale int) string {
	switch *c {
	case TDatetime:
		return "Datetime(6)"
	case TUint64:
		return "BIGINT UNSIGNED"
	case TInt64:
		return "BIGINT"
	case TFloat64:
		return "DOUBLE"
	case TJson:
		return "JSON"
	case TText:
		return "TEXT"
	case TVarchar:
		if scale == 0 {
			scale = 1024
		}
		return fmt.Sprintf("VARCHAR(%d)", scale)
	default:
		panic("not support ColType")
	}
}

func StringColumn(name, comment string) Column {
	return Column{
		Name:    name,
		ColType: TVarchar,
		Scale:   1024,
		Default: "",
		Comment: comment,
	}
}
func StringDefaultColumn(name, defaultVal, comment string) Column {
	return Column{
		Name:    name,
		ColType: TVarchar,
		Scale:   1024,
		Default: defaultVal,
		Comment: comment,
	}
}
func StringWithScale(name string, scale int, comment string) Column {
	return Column{
		Name:    name,
		ColType: TVarchar,
		Scale:   scale,
		Default: "",
		Comment: comment,
	}
}

func UuidStringColumn(name, comment string) Column {
	col := StringColumn(name, comment)
	col.Scale = 36
	return col
}

func SpanIDStringColumn(name, comment string) Column {
	col := StringDefaultColumn(name, "0", comment)
	col.Scale = 16
	return col
}

func TextColumn(name, comment string) Column {
	return Column{
		Name:    name,
		ColType: TText,
		Default: "",
		Comment: comment,
	}
}

func DatetimeColumn(name, comment string) Column {
	return Column{
		Name:    name,
		ColType: TDatetime,
		Default: "",
		Comment: comment,
	}
}

func JsonColumn(name, comment string) Column {
	return Column{
		Name:    name,
		ColType: TJson,
		Default: "{}",
		Comment: comment,
	}
}

func ValueColumn(name, comment string) Column {
	return Column{
		Name:    name,
		ColType: TFloat64,
		Default: "0.0",
		Comment: comment,
	}
}

func Int64Column(name, comment string) Column {
	return Column{
		Name:    name,
		ColType: TInt64,
		Default: "0",
		Comment: comment,
	}
}

func UInt64Column(name, comment string) Column {
	return Column{
		Name:    name,
		ColType: TUint64,
		Default: "0",
		Comment: comment,
	}
}

type Column struct {
	Name    string
	ColType ColType
	// Scale default 0, usually for varchar
	Scale   int
	Default string
	Comment string
	Alias   string // only use in view
}

// ToCreateSql return column scheme in create sql
//   - case 1: `column_name` varchar(36) DEFAULT "def_val" COMMENT "what am I, with default."
//   - case 2: `column_name` varchar(36) NOT NULL COMMENT "what am I. Without default, SO NOT NULL."
func (col *Column) ToCreateSql(ctx context.Context) string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("`%s` %s ", col.Name, col.ColType.String(col.Scale)))
	if col.ColType == TJson {
		sb.WriteString("NOT NULL ")
		if len(col.Default) == 0 {
			panic(moerr.NewNotSupported(ctx, "json column need default in csv, but not in schema"))
		}
	} else if len(col.Default) > 0 {
		sb.WriteString(fmt.Sprintf("DEFAULT %q ", col.Default))
	} else {
		sb.WriteString("NOT NULL ")
	}
	sb.WriteString(fmt.Sprintf("COMMENT %q", col.Comment))
	return sb.String()
}

var _ batchpipe.HasName = (*Table)(nil)

var NormalTableEngine = "TABLE"
var ExternalTableEngine = "EXTERNAL"

type Table struct {
	Account          string
	Database         string
	Table            string
	Columns          []Column
	PrimaryKeyColumn []Column
	Engine           string
	Comment          string
	// PathBuilder help to desc param 'infile'
	PathBuilder PathBuilder
	// AccountColumn help to split data in account's filepath
	AccountColumn *Column
	// TableOptions default is nil, see GetTableOptions
	TableOptions TableOptions
	// SupportUserAccess default false. if true, user account can access.
	SupportUserAccess bool
	// SupportConstAccess default false. if true, use Table.Account
	SupportConstAccess bool

	// name2ColumnIdx used in Row
	name2ColumnIdx map[string]int
	// accessIdx used in Row
	accountIdx int
}

func (tbl *Table) Clone() *Table {
	t := &Table{}
	*t = *tbl
	return t
}

func (tbl *Table) GetName() string {
	return tbl.Table
}
func (tbl *Table) GetDatabase() string {
	return tbl.Database
}

// GetIdentify return identify like database.table
func (tbl *Table) GetIdentify() string {
	return fmt.Sprintf("%s.%s", tbl.Database, tbl.Table)
}

type TableOptions interface {
	FormatDdl(ddl string) string
	// GetCreateOptions return option for `create {option}table`, which should end with ' '
	GetCreateOptions() string
	GetTableOptions(PathBuilder) string
}

func (tbl *Table) ToCreateSql(ctx context.Context, ifNotExists bool) string {

	TableOptions := tbl.GetTableOptions(ctx)

	const newLineCharacter = ",\n"
	sb := strings.Builder{}
	// create table
	sb.WriteString("CREATE ")
	switch strings.ToUpper(tbl.Engine) {
	case ExternalTableEngine:
		sb.WriteString(TableOptions.GetCreateOptions())
	default:
		panic(moerr.NewInternalError(ctx, "NOT support engine: %s", tbl.Engine))
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
		sb.WriteString(col.ToCreateSql(ctx))
	}
	// primary key
	if len(tbl.PrimaryKeyColumn) > 0 && tbl.Engine != ExternalTableEngine {
		sb.WriteString(newLineCharacter)
		sb.WriteString("PRIMARY KEY (")
		for idx, col := range tbl.PrimaryKeyColumn {
			if idx > 0 {
				sb.WriteString(`, `)
			}
			sb.WriteString(fmt.Sprintf("`%s`", col.Name))
		}
		sb.WriteString(`)`)
	}
	sb.WriteString("\n)")
	sb.WriteString(TableOptions.GetTableOptions(tbl.PathBuilder))

	return sb.String()
}

func (tbl *Table) GetTableOptions(ctx context.Context) TableOptions {
	if tbl.TableOptions != nil {
		return tbl.TableOptions
	}
	return GetOptionFactory(ctx, tbl.Engine)(tbl.Database, tbl.Table, tbl.Account)
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
	// SupportUserAccess default false. if true, user account can access.
	SupportUserAccess bool
}

func WithColumn(c Column) ViewOption {
	return ViewOption(func(v *View) {
		v.Columns = append(v.Columns, c)
	})
}

func SupportUserAccess(support bool) ViewOption {
	return ViewOption(func(v *View) {
		v.SupportUserAccess = support
	})
}

func (tbl *View) ToCreateSql(ctx context.Context, ifNotExists bool) string {
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
		if len(col.Alias) > 0 {
			sb.WriteString(fmt.Sprintf(" as `%s`", col.Alias))
		}
	}
	if tbl.OriginTable.Engine == ExternalTableEngine {
		sb.WriteString(fmt.Sprintf(", mo_log_date(`%s`) as `log_date`", ExternalFilePath))
		sb.WriteString(fmt.Sprintf(", `%s`", ExternalFilePath))
	}
	sb.WriteString(fmt.Sprintf(" from `%s`.`%s` where ", tbl.OriginTable.Database, tbl.OriginTable.Table))
	sb.WriteString(tbl.Condition.String())

	return sb.String()
}

type ViewSingleCondition struct {
	Column Column
	Table  string
}

func (tbl *ViewSingleCondition) String() string {
	return fmt.Sprintf("`%s` = %q", tbl.Column.Name, tbl.Table)
}

type Row struct {
	Table *Table

	Columns    []any
	CsvColumns []string
}

func (tbl *Table) GetRow(ctx context.Context) *Row {
	row := NewRow()
	row.Table = tbl
	row.Columns = make([]any, len(tbl.Columns))

	if len(tbl.name2ColumnIdx) == 0 {
		tbl.name2ColumnIdx = make(map[string]int, len(tbl.Columns))
		for idx, col := range tbl.Columns {
			if _, exist := tbl.name2ColumnIdx[col.Name]; exist {
				panic(moerr.NewInternalError(ctx, "%s table has duplicate column name: %s", tbl.GetIdentify(), col.Name))
			}
			tbl.name2ColumnIdx[col.Name] = idx
		}
		if tbl.AccountColumn != nil {
			idx, exist := tbl.name2ColumnIdx[tbl.AccountColumn.Name]
			if !exist {
				panic(moerr.NewInternalError(ctx, "%s table missing %s column", tbl.GetName(), tbl.AccountColumn.Name))
			}
			tbl.accountIdx = idx
		} else {
			tbl.accountIdx = -1
		}
	}
	return row
}

func NewRow() *Row {
	return gRowPool.Get().(*Row)
}

var gRowPool = sync.Pool{New: func() any {
	return &Row{
		Table:      nil,
		Columns:    nil,
		CsvColumns: nil,
	}
}}

func (r *Row) Free() {
	r.clean()
	gRowPool.Put(r)
}

func (r *Row) clean() {
	r.Table = nil
	r.Columns = nil
	r.CsvColumns = nil
}

func (r *Row) Clone() *Row {
	n := NewRow()
	n.Table = r.Table
	if len(r.Columns) > 0 {
		n.Columns = make([]any, len(r.Columns))
		copy(n.Columns[:], r.Columns[:])
	}
	if len(r.CsvColumns) > 0 {
		n.CsvColumns = make([]string, len(r.CsvColumns))
		copy(n.CsvColumns[:], r.CsvColumns[:])
	}
	return n
}

func (r *Row) Reset() {
	for idx, typ := range r.Table.Columns {
		switch typ.ColType.ToType().Oid {
		case types.T_int64:
			r.Columns[idx] = int64(0)
		case types.T_uint64:
			r.Columns[idx] = uint64(0)
		case types.T_float64:
			r.Columns[idx] = float64(0)
		case types.T_char, types.T_varchar,
			types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
			r.Columns[idx] = typ.Default
		case types.T_json:
			r.Columns[idx] = typ.Default
		case types.T_datetime:
			r.Columns[idx] = time.Time{}
		default:
			logutil.Errorf("the value type %v is not SUPPORT", typ.ColType.ToType().String())
			panic("the value type is not support now")
		}
	}
}

// GetAccount return r.Columns[r.AccountIdx] if r.AccountIdx >= 0 and r.Table.PathBuilder.SupportAccountStrategy,
// else return "sys"
func (r *Row) GetAccount() string {
	if r.Table.PathBuilder.SupportAccountStrategy() && r.Table.accountIdx >= 0 {
		return r.Columns[r.Table.accountIdx].(string)
	}
	if r.Table.SupportConstAccess && len(r.Table.Account) > 0 {
		return r.Table.Account
	}
	return "sys"
}

func (r *Row) SetVal(col string, val any) {
	if idx, exist := r.Table.name2ColumnIdx[col]; !exist {
		logutil.Fatalf("column(%s) not exist in table(%s)", col, r.Table.Table)
	} else {
		r.Columns[idx] = val
	}
}

func (r *Row) SetColumnVal(col Column, val any) {
	r.SetVal(col.Name, val)
}

// ToStrings output all column as string
func (r *Row) ToStrings() []string {
	col := make([]string, len(r.Table.Columns))
	for idx, typ := range r.Table.Columns {
		switch typ.ColType.ToType().Oid {
		case types.T_int64:
			col[idx] = fmt.Sprintf("%d", r.Columns[idx].(int64))
		case types.T_uint64:
			col[idx] = fmt.Sprintf("%d", r.Columns[idx].(uint64))
		case types.T_float64:
			col[idx] = fmt.Sprintf("%.1f", r.Columns[idx].(float64))
		case types.T_char, types.T_varchar,
			types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
			col[idx] = r.Columns[idx].(string) // default val can see Row.Reset
		case types.T_json:
			val := r.Columns[idx].(string)
			if len(val) == 0 {
				val = typ.Default
			}
			col[idx] = val
		case types.T_datetime:
			col[idx] = Time2DatetimeString(r.Columns[idx].(time.Time))
		default:
			logutil.Errorf("the value type %v is not SUPPORT", typ.ColType.ToType().String())
			panic("the value type is not support now")
		}
	}
	return col
}

func (r *Row) GetRawColumn() []any {
	return r.Columns
}

// GetCsvStrings not format
func (r *Row) GetCsvStrings() []string {
	return r.CsvColumns
}

func (r *Row) ParseRow(cols []string) error {
	// fixme: check len(r.Name2ColumnIdx) != len(cols)
	r.CsvColumns = cols
	return nil
}

func (r *Row) CsvPrimaryKey() string {
	if len(r.Table.PrimaryKeyColumn) == 0 {
		return ""
	}
	if len(r.Table.PrimaryKeyColumn) == 1 {
		return r.CsvColumns[r.Table.name2ColumnIdx[r.Table.PrimaryKeyColumn[0].Name]]
	}
	sb := strings.Builder{}
	for _, col := range r.Table.PrimaryKeyColumn {
		sb.WriteString(r.CsvColumns[r.Table.name2ColumnIdx[col.Name]])
		sb.WriteRune('-')
	}
	return sb.String()
}

func (r *Row) Size() (size int64) {
	if len(r.CsvColumns) > 0 {
		for _, v := range r.CsvColumns {
			size += int64(len(v))
		}
		return size
	}
	for idx, typ := range r.Table.Columns {
		switch typ.ColType.ToType().Oid {
		case types.T_int64:
			size += 8
		case types.T_uint64:
			size += 8
		case types.T_float64:
			size += 8
		case types.T_char, types.T_varchar,
			types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
			size += int64(len(r.Columns[idx].(string)))
		case types.T_json:
			size += int64(len(r.Columns[idx].(string)))
		case types.T_datetime:
			size += 16
		}
	}
	return
}

var _ TableOptions = (*NoopTableOptions)(nil)

type NoopTableOptions struct{}

func (o NoopTableOptions) FormatDdl(ddl string) string        { return ddl }
func (o NoopTableOptions) GetCreateOptions() string           { return "" }
func (o NoopTableOptions) GetTableOptions(PathBuilder) string { return "" }

var _ TableOptions = (*CsvTableOptions)(nil)

type CsvTableOptions struct {
	Formatter string
	DbName    string
	TblName   string
	Account   string
}

func getExternalTableDDLPrefix(sql string) string {
	return strings.Replace(sql, "CREATE TABLE", "CREATE EXTERNAL TABLE", 1)
}

func (o *CsvTableOptions) FormatDdl(ddl string) string {
	return getExternalTableDDLPrefix(ddl)
}

func (o *CsvTableOptions) GetCreateOptions() string {
	return "EXTERNAL "
}

func (o *CsvTableOptions) GetTableOptions(builder PathBuilder) string {
	if builder == nil {
		builder = NewDBTablePathBuilder()
	}
	if len(o.Formatter) > 0 {
		return fmt.Sprintf(o.Formatter, builder.BuildETLPath(o.DbName, o.TblName, o.Account))
	}
	return ""
}

func GetOptionFactory(ctx context.Context, engine string) func(db string, tbl string, account string) TableOptions {
	var infileFormatter = ` infile{"filepath"="etl:%s","compression"="none"}` +
		` FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 lines`
	switch engine {
	case NormalTableEngine:
		return func(_, _, _ string) TableOptions { return NoopTableOptions{} }
	case ExternalTableEngine:
		return func(db, tbl, account string) TableOptions {
			return &CsvTableOptions{Formatter: infileFormatter, DbName: db, TblName: tbl, Account: account}
		}
	default:
		panic(moerr.NewInternalError(ctx, "unknown engine: %s", engine))
	}
}

var gTable map[string]*Table
var mux sync.Mutex

// RegisterTableDefine return old one, if already registered
func RegisterTableDefine(table *Table) *Table {
	mux.Lock()
	defer mux.Unlock()
	if len(gTable) == 0 {
		gTable = make(map[string]*Table)
	}
	id := table.GetIdentify()
	old := gTable[id]
	gTable[id] = table
	return old
}

func GetAllTable() []*Table {
	mux.Lock()
	defer mux.Unlock()
	tables := make([]*Table, 0, len(gTable))
	for _, tbl := range gTable {
		tables = append(tables, tbl)
	}
	return tables
}

func GetTable(b string) (*Table, bool) {
	mux.Lock()
	defer mux.Unlock()
	tbl, exist := gTable[b]
	return tbl, exist
}

func SetPathBuilder(ctx context.Context, pathBuilder string) error {
	tables := GetAllTable()
	bp := PathBuilderFactory(pathBuilder)
	if bp == nil {
		return moerr.NewNotSupported(ctx, "not support PathBuilder: %s", pathBuilder)
	}
	for _, tbl := range tables {
		tbl.PathBuilder = bp
	}
	return nil
}

const timestampFormatter = "2006-01-02 15:04:05.000000"

func Time2DatetimeString(t time.Time) string {
	return t.Format(timestampFormatter)
}

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
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	time.Local = time.FixedZone("CST", 0) // set time-zone +0000
}

func TestNoopTableOptions_FormatDdl(t *testing.T) {
	type args struct {
		ddl string
	}
	tests := []struct {
		name          string
		args          args
		wantDdl       string
		wantCreateOpt string
		wantTableOpt  string
	}{
		{
			name: "normal",
			args: args{
				ddl: "create table ...",
			},
			wantDdl:       "create table ...",
			wantCreateOpt: "",
			wantTableOpt:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := NoopTableOptions{}
			assert.Equalf(t, tt.wantDdl, o.FormatDdl(tt.args.ddl), "FormatDdl(%v)", tt.args.ddl)
			assert.Equalf(t, tt.wantCreateOpt, o.GetCreateOptions(), "GetCreateOptions()")
			assert.Equalf(t, tt.wantTableOpt, o.GetTableOptions(nil), "GetTableOptions()")
		})
	}
}

var dummyStrColumn = Column{Name: "str", ColType: TVarchar, Scale: 32, Default: "", Comment: "str column"}
var dummyStrCreateSql = "`str` VARCHAR(32) NOT NULL COMMENT \"str column\""
var dummyInt64Column = Column{Name: "int64", ColType: TInt64, Default: "0", Comment: "int64 column"}
var dummyInt64CreateSql = "`int64` BIGINT DEFAULT \"0\" COMMENT \"int64 column\""
var dummyFloat64Column = Column{Name: "float64", ColType: TFloat64, Default: "0.0", Comment: "float64 column"}
var dummyFloat64CreateSql = "`float64` DOUBLE DEFAULT \"0.0\" COMMENT \"float64 column\""

var dummyTable = &Table{
	Account:          "test",
	Database:         "db_dummy",
	Table:            "tbl_dummy",
	Columns:          []Column{dummyStrColumn, dummyInt64Column, dummyFloat64Column},
	PrimaryKeyColumn: []Column{dummyStrColumn, dummyInt64Column},
	Engine:           ExternalTableEngine,
	Comment:          "dummy table",
	PathBuilder:      NewAccountDatePathBuilder(),
	TableOptions:     nil,
}

var dummyTableCreateExistsSql = "CREATE EXTERNAL TABLE IF NOT EXISTS `db_dummy`.`tbl_dummy`(" +
	"\n" + dummyStrCreateSql +
	",\n" + dummyInt64CreateSql +
	",\n" + dummyFloat64CreateSql +
	"\n) " + `infile{"filepath"="etl:/test/*/*/*/*/tbl_dummy/*","compression"="none"} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 lines`
var dummyTableCreateSql = "CREATE EXTERNAL TABLE `db_dummy`.`tbl_dummy`(" +
	"\n" + dummyStrCreateSql +
	",\n" + dummyInt64CreateSql +
	",\n" + dummyFloat64CreateSql +
	"\n) " + `infile{"filepath"="etl:/test/*/*/*/*/tbl_dummy/*","compression"="none"} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 lines`

type dummyCondition struct{}

func (c dummyCondition) String() string {
	return "`str` = \"NIL\""
}

var dummyView = &View{
	Database:    dummyTable.Database,
	Table:       "view",
	OriginTable: dummyTable,
	Columns:     []Column{},
	Condition:   dummyCondition{},
}
var dummyViewCreateSql = "CREATE VIEW IF NOT EXISTS `db_dummy`.`view` as select `str`, mo_log_date(`__mo_filepath`) as `log_date`, `__mo_filepath` from `db_dummy`.`tbl_dummy` where `str` = \"NIL\""

func TestRow_SetFloat64(t *testing.T) {
	type fields struct {
		Table *Table
	}
	type args struct {
		col Column
		val float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "normal",
			fields: fields{
				Table: dummyTable,
			},
			args: args{
				col: dummyFloat64Column,
				val: 1.1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.fields.Table.GetRow(context.TODO())
			defer r.Free()
			r.SetColumnVal(tt.args.col, Float64Field(tt.args.val))
		})
	}
}

func TestRow_ToStrings(t *testing.T) {
	type fields struct {
		Table   *Table
		prepare func(*Row)
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			name:   "nil",
			fields: fields{Table: dummyTable, prepare: func(r *Row) { r.Reset() }},
			want:   []string{"", "0", "0"},
		},
		{
			name: "nil",
			fields: fields{Table: dummyTable,
				prepare: func(r *Row) {
					r.SetColumnVal(dummyStrColumn, StringField("0"))
					r.SetColumnVal(dummyFloat64Column, Float64Field(1.1234567))
					r.SetColumnVal(dummyInt64Column, Int64Field(1))
				}},
			want: []string{"0", "1", "1.1234567"},
		},
		{
			name: "json",
			fields: fields{
				Table: &Table{
					Columns: []Column{
						JsonColumn("json1", ""),
						JsonColumn("json2", ""),
						JsonColumn("json3", ""),
					},
				},
				prepare: func(r *Row) {
					r.SetColumnVal(JsonColumn("json1", ""), StringField(`{"key":"str"}`))
					r.SetColumnVal(JsonColumn("json2", ""), JsonField(`{"key":"json"}`))
					r.SetColumnVal(JsonColumn("json3", ""), BytesField([]byte(`{"key":"byte"}`)))
				}},
			want: []string{`{"key":"str"}`, `{"key":"json"}`, `{"key":"byte"}`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.fields.Table.GetRow(context.TODO())
			defer r.Free()
			tt.fields.prepare(r)
			assert.Equalf(t, tt.want, r.ToStrings(), "ToStrings()")
		})
	}
}

func TestTable_GetName(t *testing.T) {
	type fields struct {
		Table *Table
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "dummy",
			fields: fields{Table: dummyTable},
			want:   dummyTable.Table,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.fields.Table.GetName(), "GetName()")
		})
	}
}

func TestTable_ToCreateSql(t *testing.T) {
	type fields struct {
		Table *Table
	}
	type args struct {
		ifNotExists bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name:   "dummy",
			fields: fields{Table: dummyTable},
			args:   args{ifNotExists: false},
			want:   dummyTableCreateSql,
		},
		{
			name:   "dummy_if_not_exist",
			fields: fields{Table: dummyTable},
			args:   args{ifNotExists: true},
			want:   dummyTableCreateExistsSql,
		},
	}
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbl := tt.fields.Table
			got := tbl.ToCreateSql(ctx, tt.args.ifNotExists)
			t.Logf("create sql: %s", got)
			require.Equalf(t, tt.want, got, "ToCreateSql(%v)", tt.args.ifNotExists)
		})
	}
}
func TestViewOption_Apply(t *testing.T) {
	type args struct {
		view *View
	}
	tests := []struct {
		name       string
		opt        ViewOption
		args       args
		wantCreate string
	}{
		{
			name:       "normal",
			opt:        WithColumn(dummyStrColumn),
			args:       args{view: dummyView},
			wantCreate: dummyViewCreateSql,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.opt.Apply(tt.args.view)
			got := tt.args.view.ToCreateSql(context.TODO(), true)
			assert.Equalf(t, tt.wantCreate, got, "ToCreateSql(%v)", true)
		})
	}
}

func TestTable_GetTableOptions(t *testing.T) {
	type fields struct {
		Account           string
		Database          string
		Table             string
		Columns           []Column
		PrimaryKeyColumn  []Column
		Engine            string
		Comment           string
		PathBuilder       PathBuilder
		AccountColumn     *Column
		TableOptions      TableOptions
		SupportUserAccess bool
	}
	tests := []struct {
		name   string
		fields fields
		want   TableOptions
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbl := &Table{
				Account:           tt.fields.Account,
				Database:          tt.fields.Database,
				Table:             tt.fields.Table,
				Columns:           tt.fields.Columns,
				PrimaryKeyColumn:  tt.fields.PrimaryKeyColumn,
				Engine:            tt.fields.Engine,
				Comment:           tt.fields.Comment,
				PathBuilder:       tt.fields.PathBuilder,
				AccountColumn:     tt.fields.AccountColumn,
				TableOptions:      tt.fields.TableOptions,
				SupportUserAccess: tt.fields.SupportUserAccess,
			}
			assert.Equalf(t, tt.want, tbl.GetTableOptions(context.TODO()), "GetTableOptions()")
		})
	}
}

func TestSetPathBuilder(t *testing.T) {
	var err error
	ctx := context.Background()
	err = SetPathBuilder(ctx, (*DBTablePathBuilder)(nil).GetName())
	require.Nil(t, err)
	err = SetPathBuilder(ctx, "AccountDate")
	require.Nil(t, err)
	err = SetPathBuilder(ctx, "unknown")
	var moErr *moerr.Error
	if errors.As(err, &moErr) && moerr.IsMoErrCode(moErr, moerr.ErrNotSupported) {
		t.Logf("got ErrNotSupported normally")
	} else {
		t.Errorf("unexpect err: %v", err)
	}
}

func TestColumnField_EncodedDatetime(t *testing.T) {
	type fields struct {
		cf ColumnField
	}
	tests := []struct {
		name   string
		fields fields
		wantT  time.Time
		want   string
	}{
		{
			name: "zero",
			fields: fields{
				cf: TimeField(ZeroTime),
			},
			wantT: ZeroTime,
			want:  "0001-01-01 00:00:00.000000",
		},
		{
			name: "empty",
			fields: fields{
				cf: TimeField(time.Time{}),
			},
			wantT: time.Time{},
			want:  "0001-01-01 00:00:00.000000",
		},
		{
			name: "Unix_Zero",
			fields: fields{
				cf: TimeField(time.Unix(0, 0)),
			},
			wantT: time.Unix(0, 0),
			want:  "1970-01-01 00:00:00.000000",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.fields.cf.GetTime()
			require.Equal(t, tt.wantT, buf)
			var bbuf [64]byte
			dst := tt.fields.cf.EncodedDatetime(bbuf[:0])
			require.Equal(t, tt.want, string(dst))
		})
	}
}

func TestTimeField(t *testing.T) {
	type args struct {
		val time.Time
	}
	tests := []struct {
		name string
		args args
		want time.Time
	}{
		{
			name: "empty",
			args: args{},
			want: time.Time{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := TimeField(tt.args.val)
			assert.Equalf(t, tt.want, f.GetTime(), "TimeField(%v)", tt.args.val)
		})
	}
}

// BenchmarkTimeField
//
// goos: darwin
// goarch: arm64
// pkg: github.com/matrixorigin/matrixone/pkg/util/export/table
// BenchmarkTimeField
// BenchmarkTimeField/uuid.string
// BenchmarkTimeField/uuid.string-10         	31779484	        39.32 ns/op
// BenchmarkTimeField/EncodeUUIDHex
// BenchmarkTimeField/EncodeUUIDHex-10       	33151078	        37.44 ns/op
// PASS
func BenchmarkTimeField(b *testing.B) {
	type args struct {
		val [16]byte
	}
	benchmarks := []struct {
		name string
		args args
		op   func(val [16]byte)
	}{
		{
			name: "uuid.string",
			args: args{
				val: uuid.New(),
			},
			op: func(val [16]byte) {
				_ = uuid.UUID(val).String()
			},
		},
		{
			name: "EncodeUUIDHex",
			args: args{
				val: uuid.New(),
			},
			op: func(val [16]byte) {
				var bytes [36]byte
				util.EncodeUUIDHex(bytes[:], val[:])
				_ = string(bytes[:])
			},
		},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bm.op(bm.args.val)
			}
		})
	}
}

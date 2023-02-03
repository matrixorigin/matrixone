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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

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

var dummyStrColumn = Column{Name: "str", ColType: TVarchar, Precision: 32, Default: "", Comment: "str column"}
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
var dummyViewCreateSql = "CREATE VIEW IF NOT EXISTS `db_dummy`.`view` as select `str`, `__mo_filepath` from `db_dummy`.`tbl_dummy` where `str` = \"NIL\""

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
			r.SetColumnVal(tt.args.col, tt.args.val)
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
			want:   []string{"", "0", "0.0"},
		},
		{
			name: "nil",
			fields: fields{Table: dummyTable,
				prepare: func(r *Row) {
					r.SetColumnVal(dummyStrColumn, "0")
					r.SetColumnVal(dummyFloat64Column, 1.1)
					r.SetColumnVal(dummyInt64Column, int64(1))
				}},
			want: []string{"0", "1", "1.1"},
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

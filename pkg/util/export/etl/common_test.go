// Copyright 2024 Matrix Origin
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

package etl

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

var dummyUInt64Column = table.Column{Name: "uint64", ColType: table.TUint64, Default: "0", Comment: "uint64 column"}
var dummyStrColumn = table.Column{Name: "str", ColType: table.TVarchar, Scale: 32, Default: "", Comment: "str column"}
var dummyInt64Column = table.Column{Name: "int64", ColType: table.TInt64, Default: "0", Comment: "int64 column"}
var dummyFloat64Column = table.Column{Name: "float64", ColType: table.TFloat64, Default: "0.0", Comment: "float64 column"}
var dummyDatetimeColumn = table.Column{Name: "datetime_6", ColType: table.TDatetime, Default: "", Comment: "datetime.6 column"}
var dummyJsonColumn = table.Column{Name: "json_col", ColType: table.TJson, Default: "{}", Comment: "json column"}

var dummyAllTypeTable = &table.Table{
	Account:          "test",
	Database:         "db_dummy",
	Table:            "tbl_all_type_dummy",
	Columns:          []table.Column{dummyStrColumn, dummyInt64Column, dummyFloat64Column, dummyUInt64Column, dummyDatetimeColumn, dummyJsonColumn},
	PrimaryKeyColumn: []table.Column{dummyStrColumn, dummyInt64Column},
	Engine:           table.ExternalTableEngine,
	Comment:          "dummy table",
	PathBuilder:      table.NewAccountDatePathBuilder(),
	TableOptions:     nil,
}

var _ table.RowField = (*dummyItem)(nil)

type dummyItem struct {
	strVal      string
	int64Val    int64
	float64Val  float64
	uint64Val   uint64
	datetimeVal time.Time
	jsonVal     string
}

func (d *dummyItem) GetTable() *table.Table {
	return dummyAllTypeTable
}

func (d *dummyItem) FillRow(ctx context.Context, row *table.Row) {
	row.SetColumnVal(dummyStrColumn, table.StringField(d.strVal))
	row.SetColumnVal(dummyInt64Column, table.Int64Field(d.int64Val))
	row.SetColumnVal(dummyFloat64Column, table.Float64Field(d.float64Val))
	row.SetColumnVal(dummyUInt64Column, table.Uint64Field(d.uint64Val))
	row.SetColumnVal(dummyDatetimeColumn, table.TimeField(d.datetimeVal))
	row.SetColumnVal(dummyJsonColumn, table.StringField(d.jsonVal))
}

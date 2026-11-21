// Copyright 2026 Matrix Origin
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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestGetTableAutoIncrCol(t *testing.T) {
	auto := plan.Type{AutoIncr: true}
	fakePK := &plan.ColDef{Name: catalog.FakePrimaryKeyColName, Typ: auto, Hidden: true}
	userColumn := &plan.ColDef{Name: "id", OriginName: "ID", Typ: auto}
	for _, tc := range []struct {
		name string
		def  *plan.TableDef
		want string
	}{
		{
			name: "hidden fake primary key only",
			def:  &plan.TableDef{Cols: []*plan.ColDef{{Name: "payload"}, fakePK}},
		},
		{
			name: "first user allocator",
			def: &plan.TableDef{Cols: []*plan.ColDef{
				nil,
				{Name: catalog.Row_ID, Typ: auto, Hidden: true},
				fakePK,
				userColumn,
				{Name: "another", Typ: auto},
			}},
			want: "id",
		},
		{
			name: "visible fake primary key name",
			def:  &plan.TableDef{Cols: []*plan.ColDef{{Name: catalog.FakePrimaryKeyColName, Typ: auto}}},
			want: catalog.FakePrimaryKeyColName,
		},
		{
			name: "view over an auto increment column",
			def:  &plan.TableDef{ViewSql: &plan.ViewDef{View: "select ID from t"}, Cols: []*plan.ColDef{userColumn}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, getTableAutoIncrCol(tc.def))
		})
	}
}

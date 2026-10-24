// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type temporaryAccessTestSession struct {
	physical string
}

func (s *temporaryAccessTestSession) GetTempTable(string, string) (string, bool) { return "", false }
func (s *temporaryAccessTestSession) AddTempTable(string, string, string)        {}
func (s *temporaryAccessTestSession) RemoveTempTable(string, string)             {}
func (s *temporaryAccessTestSession) RemoveTempTableByRealName(string)           {}
func (s *temporaryAccessTestSession) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }
func (s *temporaryAccessTestSession) OwnsTemporaryTable(_, physical string) bool {
	return s.physical == physical
}
func (s *temporaryAccessTestSession) CheckTemporaryTableCapacity(context.Context) error {
	return nil
}
func (s *temporaryAccessTestSession) PublishTemporaryTable(string, string, string)          {}
func (s *temporaryAccessTestSession) RetireTemporaryTable(string, string, string, []string) {}

var _ process.TemporaryTableDDL = (*temporaryAccessTestSession)(nil)

func TestTemporaryTableAccessAllowed(t *testing.T) {
	physical := defines.GenTempTableName(uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), "db", "t")
	foreignPhysical := defines.GenTempTableName(uuid.MustParse("ffeeddcc-bbaa-9988-7766-554433221100"), "db", "t")
	session := &temporaryAccessTestSession{physical: physical}
	tests := []struct {
		name          string
		ctx           context.Context
		tableDef      *TableDef
		physical      string
		extraInfo     *api.SchemaExtra
		aliasResolved bool
		want          bool
	}{
		{name: "owner root", tableDef: &TableDef{TableType: catalog.SystemTemporaryTable}, physical: physical, want: true},
		{name: "owner hidden index", tableDef: &TableDef{TableType: catalog.SystemTemporaryTable}, physical: physical + "___mo_index_unique_00112233-4455-6677-8899-aabbccddeeff", want: true},
		{name: "owner generated index relation", tableDef: &TableDef{TableType: catalog.SystemIndexRel}, physical: physical + "___mo_index_secondary_00112233-4455-6677-8899-aabbccddeeff", want: true},
		{name: "foreign root", tableDef: &TableDef{TableType: catalog.SystemTemporaryTable}, physical: physical + "_other", want: false},
		{name: "foreign hidden index", tableDef: &TableDef{TableType: catalog.SystemTemporaryTable}, physical: foreignPhysical + "___mo_index_unique_ffeeddcc-bbaa-9988-7766-554433221100", want: false},
		{name: "foreign generated index relation", tableDef: &TableDef{TableType: catalog.SystemIndexRel}, physical: foreignPhysical + "___mo_index_secondary_ffeeddcc-bbaa-9988-7766-554433221100", want: false},
		{name: "ordinary prefix remains visible", tableDef: &TableDef{TableType: catalog.SystemOrdinaryRel, Createsql: "CREATE TABLE __mo_tmp_00112233445566778899aabbccddeeff_db_ordinary(id int)"}, physical: defines.GenTempTableName(uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), "db", "ordinary"), want: true},
		{name: "legacy ordinary temporary root", tableDef: &TableDef{TableType: catalog.SystemOrdinaryRel, Createsql: "CREATE TEMPORARY TABLE t(id int)"}, physical: foreignPhysical, want: false},
		{name: "legacy renamed permanent root", tableDef: &TableDef{TableType: catalog.SystemOrdinaryRel, Createsql: "CREATE TEMPORARY TABLE t(id int)"}, physical: foreignPhysical, extraInfo: &api.SchemaExtra{OldName: "permanent_t"}, want: true},
		{name: "legacy derived relation", tableDef: &TableDef{TableType: catalog.SystemIndexRel}, physical: foreignPhysical + "_index", want: false},
		{name: "staging alias", tableDef: &TableDef{TableType: catalog.SystemTemporaryTable}, physical: physical + "_staged", aliasResolved: true, want: true},
		{name: "trusted cleanup", ctx: defines.AttachTemporaryTableAccess(context.Background()), tableDef: &TableDef{TableType: catalog.SystemTemporaryTable}, physical: physical + "_other", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, TemporaryTableAccessAllowed(test.ctx, session, "db", test.physical, test.tableDef, test.extraInfo, test.aliasResolved))
		})
	}
}

// Copyright 2023 Matrix Origin
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

package upgrader

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

// conditionalUpgradeV1SQLs is adding 3 columns
// (catalog.IndexAlgoName,catalog.IndexAlgoTableType,catalog.IndexAlgoParams) to mo_indexes table.
func conditionalUpgradeV1SQLs(tenantId int) ([]string, [][]string) {
	var ifEmpty = make([]string, 3)
	var then = make([][]string, 3)

	ifEmpty[0] = fmt.Sprintf(`SELECT attname FROM mo_catalog.mo_columns WHERE att_database = "%s" AND att_relname = "%s" AND attname = "%s" AND account_id=%d;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoName, tenantId)
	ifEmpty[1] = fmt.Sprintf(`SELECT attname FROM mo_catalog.mo_columns WHERE att_database = "%s" AND att_relname = "%s" AND attname = "%s" AND account_id=%d;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoTableType, tenantId)
	ifEmpty[2] = fmt.Sprintf(`SELECT attname FROM mo_catalog.mo_columns WHERE att_database = "%s" AND att_relname = "%s" AND attname = "%s" AND account_id=%d;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoParams, tenantId)

	then[0] = []string{fmt.Sprintf(`alter table %s.%s add column %s varchar(11) after type;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoName)}
	then[1] = []string{fmt.Sprintf(`alter table %s.%s add column %s varchar(11) after %s;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoTableType, catalog.IndexAlgoName)}
	then[2] = []string{fmt.Sprintf(`alter table %s.%s add column %s varchar(2048) after %s;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoParams, catalog.IndexAlgoTableType)}
	return ifEmpty, then
}

var (
	MoPubsTable = &table.Table{
		Account:  table.AccountAll,
		Database: catalog.MO_CATALOG,
		Table:    catalog.MO_PUBS,
		Columns: []table.Column{
			// only add new column
			table.TimestampDefaultColumn("update_time", "NULL", ""),
		},
	}
)

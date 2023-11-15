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
)

var (
	// conditionalUpgradeV1SQLs is adding 3 columns
	// (catalog.IndexAlgoName,catalog.IndexAlgoTableType,catalog.IndexAlgoParams) to mo_indexes table.
	conditionalUpgradeV1SQLs = []struct {
		ifEmpty string
		then    []string
	}{
		{
			ifEmpty: fmt.Sprintf(`SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = "%s" AND TABLE_NAME = "%s" AND COLUMN_NAME = "%s";`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoName),
			then: []string{
				fmt.Sprintf(`alter table %s.%s add column %s varchar(11) after type;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoName),
			},
		},
		{
			ifEmpty: fmt.Sprintf(`SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = "%s" AND TABLE_NAME = "%s" AND COLUMN_NAME = "%s";`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoTableType),
			then: []string{
				fmt.Sprintf(`alter table %s.%s add column %s varchar(11) after %s;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoTableType, catalog.IndexAlgoName),
			},
		},
		{
			ifEmpty: fmt.Sprintf(`SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = "%s" AND TABLE_NAME = "%s" AND COLUMN_NAME = "%s";`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoParams),
			then: []string{
				fmt.Sprintf(`alter table %s.%s add column %s varchar(2048) after %s;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoParams, catalog.IndexAlgoTableType),
			},
		},
	}
)

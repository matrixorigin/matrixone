// Copyright 2025 Matrix Origin
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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestGenInsertIndexTableSql_QuotesReservedKeywordColumn(t *testing.T) {
	originTableDef := &planpb.TableDef{
		Name: "files_related_mph",
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
	}
	indexDef := &planpb.IndexDef{
		IndexTableName: "__mo_index_secondary_test",
		Parts:          []string{"order", catalog.CreateAlias("id")},
		Unique:         false,
	}

	sql := genInsertIndexTableSql(originTableDef, indexDef, "test", false)

	require.Contains(t, sql, "serial_full(`order`,`id`)")
	require.Contains(t, sql, "select serial_full(`order`,`id`), `id` from `test`.`files_related_mph`;")
}

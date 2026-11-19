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

package catalog

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestValidateTableChangesSourcePriority(t *testing.T) {
	subscription := &plan.ObjectRef{PubInfo: &plan.PubInfo{}}
	require.EqualError(t, ValidateTableChangesSource(subscription, nil),
		"invalid input: table_changes source table does not exist")

	conflicting := &plan.TableDef{
		IsTemporary: true,
		TableType:   SystemViewRel,
		Cols:        []*plan.ColDef{nil, {Name: TableChangesAttrChangeType}},
	}
	require.EqualError(t, ValidateTableChangesSource(subscription, conflicting),
		"not supported: table_changes does not support subscription tables")
	require.EqualError(t, ValidateTableChangesSource(nil, conflicting),
		"not supported: table_changes does not support temporary tables")
	conflicting.IsTemporary = false
	require.EqualError(t, ValidateTableChangesSource(nil, conflicting),
		"invalid input: table_changes source column \"change_type\" conflicts with reserved metadata column")

	valid := &plan.TableDef{
		TableType: SystemClusterRel,
		Cols:      []*plan.ColDef{nil, {Name: "id"}},
		Pkey:      &plan.PrimaryKeyDef{Names: []string{"ID", "AcCoUnT_Id"}, PkeyColName: "id"},
	}
	require.NoError(t, ValidateTableChangesSource(nil, valid))
	valid.Pkey.Names = []string{"ID", "other"}
	require.EqualError(t, ValidateTableChangesSource(nil, valid),
		"not supported: table_changes requires cluster table primary keys to include account_id")
}

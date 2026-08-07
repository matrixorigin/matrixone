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

package table_function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestAppendEncodedCheckConstraintRowsDecodesSchemaExtra(t *testing.T) {
	rows := make([]checkConstraintRow, 0)
	data := api.MustMarshalTblExtra(&api.SchemaExtra{
		Checks: []*planpb.CheckDef{{
			Name:      "amount_positive",
			OriginSql: "`amount` > 0",
		}},
	})

	require.NoError(t, appendEncodedCheckConstraintRows(&rows, "app", data))
	require.Equal(t, []checkConstraintRow{
		{schema: "app", name: "amount_positive", clause: "`amount` > 0"},
	}, rows)
}

func TestAppendEncodedCheckConstraintRowsRejectsMalformedMetadata(t *testing.T) {
	rows := make([]checkConstraintRow, 0)
	require.Error(t, appendEncodedCheckConstraintRows(&rows, "app", []byte{0xff}))
	require.Empty(t, rows)
}

func TestAppendCheckConstraintRowsDecodesCheckDef(t *testing.T) {
	rows := make([]checkConstraintRow, 0)
	appendCheckConstraintRows(&rows, "app", []*planpb.CheckDef{
		{Name: "amount_positive", OriginSql: "`amount` > 0"},
		{Name: "status_valid", OriginSql: "`status` in ('new','done')"},
	})

	require.Equal(t, []checkConstraintRow{
		{schema: "app", name: "amount_positive", clause: "`amount` > 0"},
		{schema: "app", name: "status_valid", clause: "`status` in ('new','done')"},
	}, rows)
}

func TestAppendCheckConstraintRowsHandlesEmptyChecks(t *testing.T) {
	rows := make([]checkConstraintRow, 0)
	// Temporary and internal relation filtering is performed by the catalog
	// predicate in collectCheckConstraintRows; this helper only decodes rows.
	appendCheckConstraintRows(&rows, "app", nil)
	require.Empty(t, rows)
}

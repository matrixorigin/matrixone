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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

func TestCheckConstraintOutputPositionsAllowPrunedColumns(t *testing.T) {
	require.Equal(t,
		[4]int{checkConstraintCatalogColumn, checkConstraintSchemaColumn, checkConstraintNameColumn, -1},
		checkConstraintOutputPositions([]string{
			"constraint_catalog",
			"constraint_schema",
			"constraint_name",
		}))
	require.Equal(t,
		[4]int{-1, 0, 1, 2},
		checkConstraintOutputPositions([]string{
			"CONSTRAINT_SCHEMA",
			"CONSTRAINT_NAME",
			"CHECK_CLAUSE",
		}))
}

func TestAppendCheckConstraintRowAllowsPrunedColumns(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer proc.Free()

	vectors := []*vector.Vector{
		vector.NewVec(types.T_varchar.ToType()),
		vector.NewVec(types.T_varchar.ToType()),
		vector.NewVec(types.T_varchar.ToType()),
	}
	for _, vec := range vectors {
		defer vec.Free(mp)
	}

	positions := checkConstraintOutputPositions([]string{
		"constraint_catalog",
		"constraint_schema",
		"constraint_name",
	})
	require.NoError(t, appendCheckConstraintRow(vectors, positions, checkConstraintRow{
		schema: "app",
		name:   "amount_positive",
		clause: "`amount` > 0",
	}, proc))
	require.Equal(t, "def", vectors[0].GetStringAt(0))
	require.Equal(t, "app", vectors[1].GetStringAt(0))
	require.Equal(t, "amount_positive", vectors[2].GetStringAt(0))
}

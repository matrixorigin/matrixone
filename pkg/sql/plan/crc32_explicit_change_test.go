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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSameLegacyCRC32Expression(t *testing.T) {
	for _, tc := range []struct {
		name, old, next string
		same, known     bool
	}{
		{"format_and_case", "crc32(j)", "CRC32( `J` )", true, true},
		{"nested_and_literal", "crc32(json_extract(j, '$.A'))", "CRC32(JSON_EXTRACT(`J`, '$.A'))", true, true},
		{"literal_case", "crc32(json_extract(j, '$.A'))", "crc32(json_extract(j, '$.a'))", false, true},
		{"changed_operator", "crc32(j)", "crc32(j) + 1", false, true},
		{"malformed_old", "crc32(j) + 'unfinished", "crc32(j) + 1", false, false},
		{"missing_old", "", "crc32(j)", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			same, known := sameLegacyCRC32Expression(tc.old, tc.next)
			require.Equal(t, tc.same, same)
			require.Equal(t, tc.known, known)
		})
	}
}

// An explicit replacement is a semantic change: COPY must bind the new
// expression and rebuild the generated-column index, while leaving the
// original catalog definition untouched until publication.
func TestCRC32LegacyGeneratedExplicitChangeRebuildsIndex(t *testing.T) {
	mock := newGeneratedIndexAlterMock(t)
	base := mock.ctxt.tables["t_on_update_gen"]
	base.Name2ColIndex = make(map[string]int32, len(base.Cols))
	for i, col := range base.Cols {
		base.Name2ColIndex[col.Name] = int32(i)
	}
	inputPos := mockTableColPos(t, base, "val")
	input := base.Cols[inputPos]
	input.Typ = planpb.Type{Id: int32(types.T_json)}
	input.Default = &planpb.Default{NullAbility: true}
	shape, err := mysql.ParseOne(context.Background(), "create table shape(g bigint unsigned)", 1)
	require.NoError(t, err)
	defer shape.Free()
	generated := FindColumn(base.Cols, "g")
	generated.Typ, err = getTypeFromAst(context.Background(), shape.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef).Type)
	require.NoError(t, err)
	legacy := legacyCRC32Expr()
	legacy.GetF().Args[0].GetCol().ColPos = inputPos
	legacy.GetF().Args[0].GetCol().Name = "val"
	generated.GeneratedCol = &planpb.GeneratedCol{Expr: legacy, OriginString: "crc32(val)", IsStored: true}
	generated.Default = &planpb.Default{NullAbility: true}
	before := proto.Clone(base).(*planpb.TableDef)

	built, err := runOneStmt(mock, t, "alter table constraint_test.t_on_update_gen modify column g bigint unsigned generated always as (crc32(val) + 1) stored, algorithm=copy")
	require.NoError(t, err)
	alter := built.GetDdl().GetAlterTable()
	require.NotNil(t, alter)
	require.Equal(t, planpb.AlterTable_COPY, alter.AlgorithmType)
	require.False(t, alter.Options.SkipIndexesCopy["idx_generated_g"])
	require.True(t, proto.Equal(before, base), "planning must not modify source catalog")
	result := FindColumn(alter.CopyTableDef.Cols, "g")
	require.NotNil(t, result.GeneratedCol)
	require.False(t, containsLegacyCRC32(result.GeneratedCol.Expr))
	containsText, err := containsCRC32JSONTextCall(result.GeneratedCol)
	require.NoError(t, err)
	require.True(t, containsText, "the explicitly changed expression must use current JSON text semantics")
}

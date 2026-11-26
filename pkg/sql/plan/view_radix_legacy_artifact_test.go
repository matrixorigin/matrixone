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
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// glm37ReleasedViewDefRadix holds the exact serialized catalog bytes written
// by release v4.2.8-hotfix-20261119 for
// CREATE VIEW ... AS SELECT CAST(X'41' AS DECIMAL(4,4)) AS vcol. The released
// instance returned 0.0065 for that view, and a same-data upgrade instance of
// the current build re-verified both the byte-identical storage (sha256 of
// these exact bytes) and the same value; the raw receipts live in the batch
// evidence trail rather than in-repo paths.
const glm37ReleasedViewDefRadix = `{"Stmt":"create view glm36legacy.v as select cast(x'41' as decimal(4,4)) as vcol","DefaultDatabase":"mysql","sql_mode":"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES","security_type":"DEFINER"}`

// glm37LegacyViewCompiler resolves every table as the stored legacy view so
// the normal production view reader consumes the original released catalog
// bytes instead of a candidate-authored definition.
type glm37LegacyViewCompiler struct {
	*MockCompilerContext
	def *TableDef
}

func (c *glm37LegacyViewCompiler) Resolve(
	dbName string, tableName string, snapshot *Snapshot, borrowOutput *TableResolution,
) (*ObjectRef, *TableDef, error) {
	borrowOutput.Reset()
	return &ObjectRef{SchemaName: dbName, ObjName: tableName, Obj: 99}, c.def, nil
}

func (c *glm37LegacyViewCompiler) ResolveWithoutTemporaryAlias(
	dbName string, tableName string, snapshot *Snapshot, borrowOutput *TableResolution,
) (*ObjectRef, *TableDef, error) {
	return c.Resolve(dbName, tableName, snapshot, borrowOutput)
}

// The stored view names databases that exist in the release instance's
// catalog; the fixture catalog admits them without their full metadata.
func (c *glm37LegacyViewCompiler) DatabaseExists(name string, snapshot *Snapshot) bool {
	return true
}

func (c *glm37LegacyViewCompiler) GetDatabaseId(dbName string, snapshot *Snapshot) (uint64, error) {
	return 9, nil
}

// TestReleasedViewDefRadixValueAtPlanConsumer binds the released catalog
// bytes through the normal production view reader and asserts the byte
// round-trip plus the semantic value of the view column at the actual plan
// consumer, so a magnitude rebind of a legacy stored view cannot escape CI
// once the ephemeral upgrade instance is gone.
func TestReleasedViewDefRadixValueAtPlanConsumer(t *testing.T) {
	var original ViewData
	require.NoError(t, json.Unmarshal([]byte(glm37ReleasedViewDefRadix), &original))
	require.Equal(t,
		"create view glm36legacy.v as select cast(x'41' as decimal(4,4)) as vcol",
		original.Stmt, "the released catalog statement bytes must round-trip")
	require.Equal(t, "mysql", original.DefaultDatabase)
	require.NotNil(t, original.SQLMode)
	// The legacy payload predates every later field; the current reader must
	// tolerate their absence.
	require.Empty(t, original.Definition)
	require.Empty(t, original.CheckOption)
	require.Nil(t, original.LowerCaseTableNames)

	mock := NewMockCompilerContext(true, newPlanTestProcess(t))
	catalog := &glm37LegacyViewCompiler{MockCompilerContext: mock,
		def: &TableDef{
			Name: "v", DbName: "glm36legacy", TblId: 99, Version: 5,
			TableType: catalog.SystemViewRel,
			ViewSql:   &planpb.ViewDef{View: glm37ReleasedViewDefRadix},
			// The outer reader resolves the view's output name through the
			// catalog column metadata, exactly as the real catalog rows do.
			Cols: []*ColDef{{Name: "vcol", Typ: planpb.Type{
				Id: int32(types.T_decimal64), Width: 4, Scale: 4}}},
		}}

	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"select vcol from glm36legacy.v", 1)
	require.NoError(t, err)
	sel, ok := stmt.(*tree.Select)
	require.True(t, ok, "the view reader query must parse to a select")
	p, err := bindAndOptimizeSelectQueryWithValidator(
		planpb.Query_SELECT, catalog, sel, false, false, nil)
	require.NoError(t, err, "the normal current bind must accept the released bytes")

	q := p.GetQuery()
	require.NotEmpty(t, q.Steps)
	node := q.Nodes[q.Steps[q.MainOutputStep()]]
	require.NotEmpty(t, node.ProjectList)
	proj := node.ProjectList[0]
	require.Equal(t, int32(types.T_decimal64), proj.Typ.Id,
		"the view column must keep its stored DECIMAL type")
	require.Equal(t, int32(4), proj.Typ.Width)
	require.Equal(t, int32(4), proj.Typ.Scale)
	// The shape (runtime cast vs folded literal) is not the contract; the
	// value is. A magnitude rebind of this width-4 view cannot even bind
	// (typed out-of-range), and any folded decimal64 constant must carry the
	// historical coefficient: raw 65 at scale 4 formats as 0.0065, while a
	// magnitude fold would be raw 650000 at scale 4.
	lit := proj.GetLit()
	if lit != nil {
		require.NotNil(t, lit.GetDecimal64Val(),
			"a folded constant projection must be decimal64")
		require.Equal(t, int64(65), lit.GetDecimal64Val().A,
			"the stored view must fold the historical coefficient 65 at scale 4 (0.0065)")
	} else {
		require.NotNil(t, proj.GetF(),
			"a non-folded projection must keep the runtime cast so the consumer still computes 0.0065")
	}
}

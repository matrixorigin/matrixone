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

package plan

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestAlterCopyAddColumnCertificate(t *testing.T) {
	for _, sql := range []string{
		"alter table t1 add column d int default 7",
		"alter table t1 add column `D` int first, add column e int first, add column f int after a",
		"alter table t1 add column d int, algorithm=copy, lock=exclusive",
	} {
		t.Run(sql, func(t *testing.T) {
			p, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, sql)
			require.NoError(t, err)
			q := p.GetDdl().GetAlterTable()
			require.Equal(t, plan.AlterTable_COPY, q.AlgorithmType)
			check := func(q *plan.AlterTable) {
				i := 0
				for pos, col := range q.CopyTableDef.Cols {
					if FindColumn(q.TableDef.Cols, col.Name) != nil {
						continue
					}
					require.Less(t, i, len(q.Actions))
					add := q.Actions[i].GetAddColumn()
					require.NotNil(t, add)
					require.Equal(t, col.Name, add.Name)
					require.EqualValues(t, pos, add.Pos)
					require.True(t, proto.Equal(&col.Typ, &add.Type))
					previous := ""
					if pos > 0 {
						previous = q.CopyTableDef.Cols[pos-1].Name
					}
					require.Equal(t, previous, add.PreName)
					i++
				}
				require.Positive(t, i)
				require.Len(t, q.Actions, i)
			}
			check(q)
			check(DeepCopyPlan(p).GetDdl().GetAlterTable())
			encoded, err := p.Marshal()
			require.NoError(t, err)
			var decoded plan.Plan
			require.NoError(t, decoded.Unmarshal(encoded))
			check(decoded.GetDdl().GetAlterTable())
			original := q.Actions[0].GetAddColumn()
			original.Type.XXX_unrecognized = []byte{0xa0, 0x06, 1}
			copied := DeepCopyPlan(p).GetDdl().GetAlterTable().Actions[0].GetAddColumn()
			require.Equal(t, original, copied)
			copied.Name = "changed"
			copied.Type.XXX_unrecognized[2] = 2
			require.NotEqual(t, copied.Name, original.Name)
			require.EqualValues(t, 1, original.Type.XXX_unrecognized[2])
		})
	}
}

func TestAlterCopyMixedOptionsHaveNoAddCertificate(t *testing.T) {
	for _, suffix := range []string{
		"modify column b char(20)", "drop column b", "drop column b, add column b char(10)", "rename column b to c", "order by a",
		"default character set utf8mb4", "add index idx_d(d)",
		"add constraint ck_d check(d>0)", "engine=innodb",
	} {
		t.Run(suffix, func(t *testing.T) {
			p, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
				"alter table t1 add column d int, "+suffix)
			require.NoError(t, err)
			for _, action := range p.GetDdl().GetAlterTable().Actions {
				require.Nil(t, action.GetAddColumn(), "mixed operations cannot get a partial certificate")
			}
		})
	}
	for _, options := range [][]tree.AlterTableOption{
		nil, {nil}, {(*tree.AlterAddCol)(nil)}, {&tree.AlterAddCol{}},
		{&tree.AlterOptionAlgorithm{}}, {&tree.AlterOptionLock{}},
		{&tree.AlterAddCol{Column: &tree.ColumnTableDef{}}, &tree.TableOptionAutoIncrement{Value: 20}},
	} {
		require.False(t, pureAlterAddColumns(options))
	}
	for _, hint := range []string{"algorithm=unknown", "lock=unknown"} {
		_, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "alter table t1 add column d int, "+hint)
		require.Error(t, err)
	}
}

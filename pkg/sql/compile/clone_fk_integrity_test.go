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

package compile

import (
	"context"
	"errors"
	"testing"

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func colDefById(name string, id uint64) *plan.ColDef {
	return &plan.ColDef{Name: name, ColId: id}
}

func TestFkColumnNames(t *testing.T) {
	def := &plan.TableDef{
		Name: "c",
		Cols: []*plan.ColDef{colDefById("id", 1), colDefById("pid", 2)},
	}
	names, err := fkColumnNames(def, []uint64{2, 1})
	require.NoError(t, err)
	require.Equal(t, []string{"pid", "id"}, names)

	_, err = fkColumnNames(def, []uint64{99})
	require.ErrorContains(t, err, "missing column id 99")

	_, err = fkColumnNames(&plan.TableDef{Name: "empty"}, []uint64{1})
	require.ErrorContains(t, err, "missing table definition")

	_, err = fkColumnNames(nil, []uint64{1})
	require.ErrorContains(t, err, "missing table definition")
}

func TestBuildCloneFKOrphanProbeSQL(t *testing.T) {
	t.Run("single column", func(t *testing.T) {
		sql := buildCloneFKOrphanProbeSQL("db", "child", []string{"pid"}, "db", "parent", []string{"id"})
		require.Equal(t,
			"select concat_ws(',', `db`.`child`.`pid`) from `db`.`child` where `db`.`child`.`pid` is not null "+
				"and not exists (select 1 from `db`.`parent` where `db`.`parent`.`id` = `db`.`child`.`pid`) limit 1",
			sql)
	})
	t.Run("composite foreign key", func(t *testing.T) {
		sql := buildCloneFKOrphanProbeSQL("db", "child", []string{"c1", "c2"}, "pdb", "parent", []string{"p1", "p2"})
		require.Contains(t, sql, "`db`.`child`.`c1` is not null and `db`.`child`.`c2` is not null")
		require.Contains(t, sql, "`pdb`.`parent`.`p1` = `db`.`child`.`c1` and `pdb`.`parent`.`p2` = `db`.`child`.`c2`")
		require.Contains(t, sql, "limit 1")
	})
}

func TestShouldValidateClonedTableFKIntegrity(t *testing.T) {
	t.Run("historical clone is validated", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		c := &Compile{proc: proc}
		require.True(t, shouldValidateClonedTableFKIntegrity(c, &plan.CloneTable{
			ScanSnapshot: &plan.Snapshot{},
		}))
	})
	t.Run("restore session clone is validated", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo = process.SessionInfo{IsRestore: true}
		c := &Compile{proc: proc}
		require.True(t, shouldValidateClonedTableFKIntegrity(c, &plan.CloneTable{}))
	})
	t.Run("ordinary clone is not validated", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		c := &Compile{proc: proc}
		require.False(t, shouldValidateClonedTableFKIntegrity(c, &plan.CloneTable{}))
	})
}

type orphanProbeCall struct {
	sql string
}

func stubOrphanProbe(stubs *gostub.Stubs, calls *[]orphanProbeCall, found bool, sample string, err error) {
	stubs.Stub(&probeCloneFKOrphan, func(c *Compile, sql string) (bool, string, error) {
		*calls = append(*calls, orphanProbeCall{sql: sql})
		return found, sample, err
	})
}

// fkCloneTestSetup builds the engine-visible state the validator consumes:
// the freshly created destination relation "db.child" (engine-id columns
// id=1, pid=2) plus a parent relation id=300 named pdb.parent.
func fkCloneTestSetup(t *testing.T, restoreSession bool) (*Compile, *plan.CloneTable, *stubRelation) {
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo = process.SessionInfo{IsRestore: restoreSession}
	eng := newStubEngine()
	eng.relsById = make(map[uint64]*stubRelation)
	eng.relDbNames = make(map[uint64]string)
	child := newStubRelation("child")
	child.tableDef = &plan.TableDef{
		Name: "child",
		Cols: []*plan.ColDef{colDefById("id", 1), colDefById("pid", 2)},
	}
	db := newStubDatabase("db")
	db.rels["child"] = child
	eng.dbs["db"] = db
	c := &Compile{proc: proc, e: eng}
	clonePlan := &plan.CloneTable{}
	parent := newStubRelation("parent")
	parent.copyDef = &plan.TableDef{Name: "parent", Cols: []*plan.ColDef{colDefById("id", 10)}}
	eng.relsById[300] = parent
	eng.relDbNames[300] = "pdb"
	return c, clonePlan, child
}

func fk1Def() *plan.ForeignKeyDef {
	return &plan.ForeignKeyDef{Name: "fk1", Cols: []uint64{2}, ForeignTbl: 300, ForeignCols: []uint64{10}}
}

// stubChildConstraint installs the freshly created destination constraint:
// the FK the relation actually stores after creation refreshed it.
func stubChildConstraint(stubs *gostub.Stubs, child *stubRelation, fks ...*plan.ForeignKeyDef) {
	stubs.Stub(&GetConstraintDef, func(_ context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
		if rel == child {
			return &engine.ConstraintDef{Cts: []engine.Constraint{
				&engine.ForeignKeyDef{Fkeys: fks},
			}}, nil
		}
		return &engine.ConstraintDef{}, nil
	})
}

func TestValidateClonedTableFKIntegrity(t *testing.T) {
	t.Run("no foreign keys is a no-op", func(t *testing.T) {
		c, clonePlan, child := fkCloneTestSetup(t, true)
		var calls []orphanProbeCall
		stubs := gostub.New()
		defer stubs.Reset()
		stubChildConstraint(stubs, child)
		stubOrphanProbe(stubs, &calls, true, "x", nil)
		require.NoError(t, validateClonedTableFKIntegrity(c, clonePlan, "db", "child"))
		require.Empty(t, calls)
	})

	t.Run("ordinary clone skips validation", func(t *testing.T) {
		c, clonePlan, child := fkCloneTestSetup(t, false)
		var calls []orphanProbeCall
		stubs := gostub.New()
		defer stubs.Reset()
		stubChildConstraint(stubs, child, fk1Def())
		stubOrphanProbe(stubs, &calls, true, "x", nil)
		require.NoError(t, validateClonedTableFKIntegrity(c, clonePlan, "db", "child"))
		require.Empty(t, calls)
	})

	t.Run("historical clone runs validation on an ordinary session", func(t *testing.T) {
		c, clonePlan, child := fkCloneTestSetup(t, false)
		clonePlan.ScanSnapshot = &plan.Snapshot{}
		var calls []orphanProbeCall
		stubs := gostub.New()
		defer stubs.Reset()
		stubChildConstraint(stubs, child, fk1Def())
		stubOrphanProbe(stubs, &calls, false, "", nil)
		require.NoError(t, validateClonedTableFKIntegrity(c, clonePlan, "db", "child"))
		require.Len(t, calls, 1)
		require.Contains(t, calls[0].sql, "from `db`.`child`")
		require.Contains(t, calls[0].sql, "from `pdb`.`parent`")
	})

	t.Run("self reference is skipped", func(t *testing.T) {
		c, clonePlan, child := fkCloneTestSetup(t, true)
		selfFk := fk1Def()
		selfFk.ForeignTbl = 0
		var calls []orphanProbeCall
		stubs := gostub.New()
		defer stubs.Reset()
		stubChildConstraint(stubs, child, selfFk)
		stubOrphanProbe(stubs, &calls, true, "x", nil)
		require.NoError(t, validateClonedTableFKIntegrity(c, clonePlan, "db", "child"))
		require.Empty(t, calls)
	})

	t.Run("missing parent relation keeps the historical dangling behavior", func(t *testing.T) {
		c, clonePlan, child := fkCloneTestSetup(t, true)
		eng := c.e.(*stubEngine)
		eng.relsById = nil
		var calls []orphanProbeCall
		stubs := gostub.New()
		defer stubs.Reset()
		stubChildConstraint(stubs, child, fk1Def())
		stubOrphanProbe(stubs, &calls, true, "x", nil)
		require.NoError(t, validateClonedTableFKIntegrity(c, clonePlan, "db", "child"))
		require.Empty(t, calls)
	})

	t.Run("parent lookup error propagates", func(t *testing.T) {
		c, clonePlan, child := fkCloneTestSetup(t, true)
		eng := c.e.(*stubEngine)
		eng.relByIdErr = errors.New("relation lookup failed")
		stubs := gostub.New()
		defer stubs.Reset()
		stubChildConstraint(stubs, child, fk1Def())
		require.ErrorContains(t,
			validateClonedTableFKIntegrity(c, clonePlan, "db", "child"),
			"relation lookup failed")
	})

	t.Run("destination database lookup error propagates", func(t *testing.T) {
		c, clonePlan, child := fkCloneTestSetup(t, true)
		eng := c.e.(*stubEngine)
		eng.dbErr = errors.New("database lookup failed")
		stubs := gostub.New()
		defer stubs.Reset()
		stubChildConstraint(stubs, child, fk1Def())
		require.ErrorContains(t,
			validateClonedTableFKIntegrity(c, clonePlan, "db", "child"),
			"database lookup failed")
	})

	t.Run("destination relation lookup error propagates", func(t *testing.T) {
		c, clonePlan, child := fkCloneTestSetup(t, true)
		eng := c.e.(*stubEngine)
		delete(eng.dbs["db"].rels, "child")
		stubs := gostub.New()
		defer stubs.Reset()
		stubChildConstraint(stubs, child, fk1Def())
		require.Error(t, validateClonedTableFKIntegrity(c, clonePlan, "db", "child"))
	})

	t.Run("constraint read error propagates", func(t *testing.T) {
		c, clonePlan, _ := fkCloneTestSetup(t, true)
		stubs := gostub.New()
		defer stubs.Reset()
		stubs.Stub(&GetConstraintDef, func(context.Context, engine.Relation) (*engine.ConstraintDef, error) {
			return nil, errors.New("constraint read failed")
		})
		require.ErrorContains(t,
			validateClonedTableFKIntegrity(c, clonePlan, "db", "child"),
			"constraint read failed")
	})

	t.Run("missing child column is an error", func(t *testing.T) {
		c, clonePlan, child := fkCloneTestSetup(t, true)
		badFk := fk1Def()
		badFk.Cols = []uint64{77}
		stubs := gostub.New()
		defer stubs.Reset()
		stubChildConstraint(stubs, child, badFk)
		require.ErrorContains(t,
			validateClonedTableFKIntegrity(c, clonePlan, "db", "child"),
			"missing column id 77")
	})

	t.Run("missing parent column is an error", func(t *testing.T) {
		c, clonePlan, child := fkCloneTestSetup(t, true)
		badFk := fk1Def()
		badFk.ForeignCols = []uint64{78}
		stubs := gostub.New()
		defer stubs.Reset()
		stubChildConstraint(stubs, child, badFk)
		require.ErrorContains(t,
			validateClonedTableFKIntegrity(c, clonePlan, "db", "child"),
			"missing column id 78")
	})

	t.Run("probe failure propagates", func(t *testing.T) {
		c, clonePlan, child := fkCloneTestSetup(t, true)
		var calls []orphanProbeCall
		stubs := gostub.New()
		defer stubs.Reset()
		stubChildConstraint(stubs, child, fk1Def())
		stubOrphanProbe(stubs, &calls, false, "", errors.New("probe failed"))
		require.ErrorContains(t,
			validateClonedTableFKIntegrity(c, clonePlan, "db", "child"),
			"probe failed")
	})

	t.Run("orphan rows reject the restore with the constraint and sample", func(t *testing.T) {
		c, clonePlan, child := fkCloneTestSetup(t, true)
		var calls []orphanProbeCall
		stubs := gostub.New()
		defer stubs.Reset()
		stubChildConstraint(stubs, child, fk1Def())
		stubOrphanProbe(stubs, &calls, true, "777", nil)
		err := validateClonedTableFKIntegrity(c, clonePlan, "db", "child")
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot restore table db.child")
		require.Contains(t, err.Error(), "`fk1`")
		require.Contains(t, err.Error(), "777")
		require.Contains(t, err.Error(), "pdb.parent")
	})

	t.Run("clean state validates every foreign key", func(t *testing.T) {
		c, clonePlan, child := fkCloneTestSetup(t, true)
		secondParent := newStubRelation("parent2")
		secondParent.copyDef = &plan.TableDef{Name: "parent2", Cols: []*plan.ColDef{colDefById("k", 20)}}
		eng := c.e.(*stubEngine)
		eng.relsById[400] = secondParent
		eng.relDbNames[400] = "pdb"
		child.tableDef.Cols = append(child.tableDef.Cols, colDefById("pid2", 3))
		fk2 := &plan.ForeignKeyDef{Name: "fk2", Cols: []uint64{3}, ForeignTbl: 400, ForeignCols: []uint64{20}}
		var calls []orphanProbeCall
		stubs := gostub.New()
		defer stubs.Reset()
		stubChildConstraint(stubs, child, fk1Def(), fk2)
		stubOrphanProbe(stubs, &calls, false, "", nil)
		require.NoError(t, validateClonedTableFKIntegrity(c, clonePlan, "db", "child"))
		require.Len(t, calls, 2)
	})
}

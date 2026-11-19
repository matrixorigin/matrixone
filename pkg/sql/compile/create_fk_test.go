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
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type recordedConstraintWrite struct {
	relation   *stubRelation
	constraint *engine.ConstraintDef
}

func newConstraintAwareRelation(name string, id uint64, defs []engine.TableDef, copyDef *plan.TableDef) *stubRelation {
	rel := newStubRelation(name)
	rel.tableID = id
	rel.defs = defs
	rel.copyDef = copyDef
	return rel
}

func childColumnDefs(names ...string) []engine.TableDef {
	defs := make([]engine.TableDef, 0, len(names))
	for i, name := range names {
		defs = append(defs, &engine.AttributeDef{
			Attr: engine.Attribute{Name: name, ID: uint64(i + 11)},
		})
	}
	return defs
}

func constraintByType(def *engine.ConstraintDef, refChild bool) (fks *engine.ForeignKeyDef, refs *engine.RefChildTableDef) {
	for _, ct := range def.Cts {
		switch c := ct.(type) {
		case *engine.ForeignKeyDef:
			fks = c
		case *engine.RefChildTableDef:
			refs = c
		}
	}
	return
}

// The forward-FK publisher must merge freshly bound child FK definitions into
// the child's CURRENT constraint. createTable registers the new child on the
// direct-FK parents' RefChildTableDef between binding and publication; the
// pre-fix publisher rewrote the child from the stale bind-time snapshot and
// lost that registration, silently disabling parent-side FK enforcement.
func TestPublishCreateForwardForeignKeysPreservesCurrentConstraint(t *testing.T) {
	proc := testutil.NewProcess(t)
	c := &Compile{proc: proc}

	child := newConstraintAwareRelation("b", 200, nil, nil)
	parent := newConstraintAwareRelation("a", 100, childColumnDefs("id"), nil)

	existingChildFk := &plan.ForeignKeyDef{Name: "fk_unrelated", ForeignTbl: 55}
	// Constraint state committed by the same transaction AFTER binding: the
	// RefChildTableDef entry written by addChildTableIDToDistinctParents.
	childCurrent := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.ForeignKeyDef{Fkeys: []*plan.ForeignKeyDef{existingChildFk}},
		&engine.RefChildTableDef{Tables: []uint64{42}},
	}}
	parentCurrent := &engine.ConstraintDef{}

	var writes []recordedConstraintWrite
	stubs := gostub.New()
	defer stubs.Reset()
	stubs.Stub(&GetConstraintDef, func(_ context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
		switch rel {
		case child:
			return childCurrent, nil
		case parent:
			return parentCurrent, nil
		}
		return nil, errors.New("unexpected relation")
	})

	stubUpdate := func(rel *stubRelation) {
		rel.updateConstraint = func(_ context.Context, ct *engine.ConstraintDef) error {
			writes = append(writes, recordedConstraintWrite{relation: rel, constraint: ct})
			return nil
		}
	}
	stubUpdate(child)
	stubUpdate(parent)

	boundFk := &plan.ForeignKeyDef{
		Name:        "fk_b",
		ForeignCols: []uint64{0},
	}
	children := []forwardForeignKeyChild{{
		relation: child,
		bindings: []*plan2.FkData{{
			Def:          boundFk,
			ColsReferred: &plan.FkColName{Cols: []string{"id"}},
		}},
	}}

	require.NoError(t, c.publishCreateForwardForeignKeys(parent, children))

	require.Len(t, writes, 2)
	var childWrite, parentWrite *engine.ConstraintDef
	for _, w := range writes {
		if w.relation == child {
			childWrite = w.constraint
		} else {
			parentWrite = w.constraint
		}
	}
	require.NotNil(t, childWrite)
	require.NotNil(t, parentWrite)

	childFks, childRefs := constraintByType(childWrite, true)
	require.NotNil(t, childRefs, "same-transaction RefChildTableDef registration must survive publication")
	require.Equal(t, []uint64{42}, childRefs.Tables)
	require.NotNil(t, childFks)
	require.Len(t, childFks.Fkeys, 2)
	byName := make(map[string]*plan.ForeignKeyDef, 2)
	for _, fk := range childFks.Fkeys {
		byName[fk.Name] = fk
	}
	require.Same(t, existingChildFk, byName["fk_unrelated"], "unrelated FK must be preserved")
	published := byName["fk_b"]
	require.NotNil(t, published, "freshly bound FK must be published")
	require.Equal(t, uint64(100), published.ForeignTbl, "freshly bound FK must point at the parent relation")
	require.Equal(t, []uint64{uint64(11)}, published.ForeignCols, "referenced columns must map to the parent's column ids")

	parentFks, parentRefs := constraintByType(parentWrite, true)
	require.Nil(t, parentFks)
	require.NotNil(t, parentRefs)
	require.Contains(t, parentRefs.Tables, uint64(200), "parent must register the forward-ref child")
}

// A second publication of the same binding (e.g. statement retry after the
// first attempt wrote constraints in-txn) must not duplicate entries.
func TestPublishCreateForwardForeignKeysIsIdempotent(t *testing.T) {
	proc := testutil.NewProcess(t)
	c := &Compile{proc: proc}

	child := newConstraintAwareRelation("b", 200, nil, nil)
	parent := newConstraintAwareRelation("a", 100, childColumnDefs("id"), nil)

	alreadyBound := &plan.ForeignKeyDef{Name: "fk_b", ForeignTbl: 100, ForeignCols: []uint64{11}}
	childCurrent := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.ForeignKeyDef{Fkeys: []*plan.ForeignKeyDef{alreadyBound}},
		&engine.RefChildTableDef{Tables: []uint64{42}},
	}}
	parentCurrent := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.RefChildTableDef{Tables: []uint64{200}},
	}}

	var childWrite, parentWrite *engine.ConstraintDef
	stubs := gostub.New()
	defer stubs.Reset()
	stubs.Stub(&GetConstraintDef, func(_ context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
		if rel == child {
			return childCurrent, nil
		}
		return parentCurrent, nil
	})
	child.updateConstraint = func(_ context.Context, ct *engine.ConstraintDef) error {
		childWrite = ct
		return nil
	}
	parent.updateConstraint = func(_ context.Context, ct *engine.ConstraintDef) error {
		parentWrite = ct
		return nil
	}

	children := []forwardForeignKeyChild{{
		relation: child,
		bindings: []*plan2.FkData{{
			Def:          &plan.ForeignKeyDef{Name: "fk_b", ForeignCols: []uint64{0}},
			ColsReferred: &plan.FkColName{Cols: []string{"id"}},
		}},
	}}
	require.NoError(t, c.publishCreateForwardForeignKeys(parent, children))

	childFks, childRefs := constraintByType(childWrite, true)
	require.NotNil(t, childFks)
	require.Len(t, childFks.Fkeys, 1, "re-publication must not duplicate the same FK name")
	require.Equal(t, []uint64{42}, childRefs.Tables)
	_, parentRefs := constraintByType(parentWrite, true)
	require.Len(t, parentRefs.Tables, 1, "re-publication must not duplicate the parent's child registration")
}

func TestPublishCreateForwardForeignKeysNoChildren(t *testing.T) {
	proc := testutil.NewProcess(t)
	c := &Compile{proc: proc}
	require.NoError(t, c.publishCreateForwardForeignKeys(newStubRelation("a"), nil))
}

func TestPublishCreateForwardForeignKeysErrors(t *testing.T) {
	newBindingChildren := func(child *stubRelation) []forwardForeignKeyChild {
		return []forwardForeignKeyChild{{
			relation: child,
			bindings: []*plan2.FkData{{
				Def:          &plan.ForeignKeyDef{Name: "fk_b", ForeignCols: []uint64{0}},
				ColsReferred: &plan.FkColName{Cols: []string{"id"}},
			}},
		}}
	}

	t.Run("missing referenced column on parent", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		c := &Compile{proc: proc}
		child := newConstraintAwareRelation("b", 200, nil, nil)
		parent := newConstraintAwareRelation("a", 100, childColumnDefs("other"), nil)
		err := c.publishCreateForwardForeignKeys(parent, newBindingChildren(child))
		require.ErrorContains(t, err, "missing referenced column")
	})

	t.Run("current constraint read failure propagates", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		c := &Compile{proc: proc}
		child := newConstraintAwareRelation("b", 200, nil, nil)
		parent := newConstraintAwareRelation("a", 100, childColumnDefs("id"), nil)
		stubs := gostub.New()
		defer stubs.Reset()
		stubs.Stub(&GetConstraintDef, func(context.Context, engine.Relation) (*engine.ConstraintDef, error) {
			return nil, errors.New("constraint read failed")
		})
		require.ErrorContains(t, c.publishCreateForwardForeignKeys(parent, newBindingChildren(child)), "constraint read failed")
	})

	t.Run("child constraint update failure propagates", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		c := &Compile{proc: proc}
		child := newConstraintAwareRelation("b", 200, nil, nil)
		parent := newConstraintAwareRelation("a", 100, childColumnDefs("id"), nil)
		stubs := gostub.New()
		defer stubs.Reset()
		stubs.Stub(&GetConstraintDef, func(context.Context, engine.Relation) (*engine.ConstraintDef, error) {
			return &engine.ConstraintDef{}, nil
		})
		child.updateConstraint = func(context.Context, *engine.ConstraintDef) error {
			return errors.New("child update failed")
		}
		require.ErrorContains(t, c.publishCreateForwardForeignKeys(parent, newBindingChildren(child)), "child update failed")
	})

	t.Run("parent constraint update failure propagates", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		c := &Compile{proc: proc}
		child := newConstraintAwareRelation("b", 200, nil, nil)
		parent := newConstraintAwareRelation("a", 100, childColumnDefs("id"), nil)
		stubs := gostub.New()
		defer stubs.Reset()
		stubs.Stub(&GetConstraintDef, func(context.Context, engine.Relation) (*engine.ConstraintDef, error) {
			return &engine.ConstraintDef{}, nil
		})
		child.updateConstraint = func(context.Context, *engine.ConstraintDef) error { return nil }
		parent.updateConstraint = func(context.Context, *engine.ConstraintDef) error {
			return errors.New("parent update failed")
		}
		require.ErrorContains(t, c.publishCreateForwardForeignKeys(parent, newBindingChildren(child)), "parent update failed")
	})
}

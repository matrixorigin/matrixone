// Copyright 2021 Matrix Origin
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

package moengine

import (
	"context"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

var (
	_ engine.Relation = (*baseRelation)(nil)
)

const ADDR = "localhost:20000"

func (*baseRelation) Size(context.Context, string) (int64, error) {
	return 0, nil
}

func (*baseRelation) CardinalNumber(string) int64 {
	return 0
}

func (*baseRelation) Ranges(_ context.Context, _ *plan.Expr) ([][]byte, error) {
	return nil, nil
}

func (*baseRelation) AddTableDef(_ context.Context, def engine.TableDef) error {
	panic(any("implement me"))
}

func (*baseRelation) DelTableDef(_ context.Context, def engine.TableDef) error {
	panic(any("implement me"))
}

func (rel *baseRelation) TableDefs(_ context.Context) ([]engine.TableDef, error) {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	defs, _ := SchemaToDefs(schema)
	return defs, nil
}

func (rel *baseRelation) UpdateConstraint(_ context.Context, def *engine.ConstraintDef) error {
	bin, err := def.MarshalBinary()
	if err != nil {
		return err
	}
	return rel.handle.UpdateConstraint(bin)
}

func (rel *baseRelation) UpdateConstraintWithBin(_ context.Context, bin []byte) error {
	return rel.handle.UpdateConstraint(bin)
}

func (rel *baseRelation) TableColumns(_ context.Context) ([]*engine.Attribute, error) {
	colDefs := rel.handle.GetMeta().(*catalog.TableEntry).GetColDefs()
	cols, _ := ColDefsToAttrs(colDefs)
	return cols, nil
}

func (rel *baseRelation) Stats(context.Context, *plan2.Expr) (int32, int64, int64, error) {
	//for tae, return 0 blocks. it does not matter and will be deleted in the future
	rows := rel.handle.Rows()
	return 0, rows, rows, nil
}

func (rel *baseRelation) Rows(c context.Context) (int64, error) {
	_, rows, _, err := rel.Stats(c, nil)
	return rows, err
}

func (rel *baseRelation) GetSchema(_ context.Context) *catalog.Schema {
	return rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
}

func (rel *baseRelation) GetPrimaryKeys(_ context.Context) ([]*engine.Attribute, error) {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	if !schema.HasPK() {
		return nil, nil
	}
	attrs := make([]*engine.Attribute, 0, len(schema.SortKey.Defs))
	for _, def := range schema.SortKey.Defs {
		attr := new(engine.Attribute)
		attr.Name = def.Name
		attr.Type = def.Type
		attrs = append(attrs, attr)
	}
	logutil.Debugf("GetPrimaryKeys: %v", attrs[0])
	return attrs, nil
}

// The hidden column in tae has been renamed to PhyAddr, while GetHideKeys method remains untouched.
// As @nnsgmsone suggests, it is better to only retain TableDefs and discard other column-info-related methods.
// Might that can be done in the future

func (rel *baseRelation) GetHideKeys(_ context.Context) ([]*engine.Attribute, error) {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	if schema.PhyAddrKey == nil {
		return nil, moerr.NewNotSupportedNoCtx("system table has no rowid")
	}
	key := new(engine.Attribute)
	key.Name = schema.PhyAddrKey.Name
	key.Type = schema.PhyAddrKey.Type
	key.IsRowId = true
	// key.IsHidden = true
	logutil.Debugf("GetHideKey: %v", key)
	return []*engine.Attribute{key}, nil
}

func (rel *baseRelation) Write(_ context.Context, _ *batch.Batch) error {
	return nil
}

func (rel *baseRelation) Update(_ context.Context, _ *batch.Batch) error {
	return nil
}

func (rel *baseRelation) Delete(_ context.Context, _ *batch.Batch, _ string) error {
	return nil
}

func (rel *baseRelation) NewReader(_ context.Context, num int, _ *plan.Expr, _ [][]byte) ([]engine.Reader, error) {
	var rds []engine.Reader

	it := rel.handle.MakeBlockIt()
	for i := 0; i < num; i++ {
		reader := newReader(rel.handle, it)
		rds = append(rds, reader)
	}
	return rds, nil
}

func (rel *baseRelation) GetTableID(_ context.Context) uint64 {
	return rel.handle.ID()
}

func (rel *baseRelation) GetRelationID(_ context.Context) uint64 {
	return rel.handle.ID()
}

func (rel *baseRelation) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	return nil, nil, nil
}

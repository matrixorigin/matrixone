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

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var (
	_ engine.Database = (*txnDatabase)(nil)
)

func newDatabase(h handle.Database) *txnDatabase {
	return &txnDatabase{
		handle: h,
	}
}

func (db *txnDatabase) Relations(_ context.Context) ([]string, error) {
	var names []string

	it := db.handle.MakeRelationIt()
	for it.Valid() {
		names = append(names, it.GetRelation().GetMeta().(*catalog.TableEntry).GetSchema().Name)
		it.Next()
	}
	return names, nil
}

func (db *txnDatabase) Relation(_ context.Context, name string) (engine.Relation, error) {
	var err error
	var rel engine.Relation

	h, err := db.handle.GetRelationByName(name)
	if err != nil {
		return nil, err
	}
	if isSysRelation(name) {
		rel = newSysRelation(h)
		return rel, nil
	}
	rel = newRelation(h)
	return rel, nil
}

func (db *txnDatabase) Create(_ context.Context, name string, defs []engine.TableDef) error {
	schema, err := DefsToSchema(name, defs)
	if err != nil {
		return err
	}
	schema.BlockMaxRows = 40000
	schema.SegmentMaxBlocks = 20
	_, err = db.handle.CreateRelation(schema)
	return err
}

func (db *txnDatabase) Delete(_ context.Context, name string) error {
	_, err := db.handle.DropRelationByName(name)
	return err
}

// Copyright 2023 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type stubEngine struct {
	engine.Engine
	dbs map[string]*stubDatabase
}

func newStubEngine() *stubEngine {
	e := &stubEngine{dbs: make(map[string]*stubDatabase)}
	// Setup MO_CATALOG
	cat := newStubDatabase(catalog.MO_CATALOG)
	cat.rels[catalog.MO_DATABASE] = newStubRelation(catalog.MO_DATABASE)
	cat.rels[catalog.MO_TABLES] = newStubRelation(catalog.MO_TABLES)
	e.dbs[catalog.MO_CATALOG] = cat
	return e
}

func (e *stubEngine) Database(ctx context.Context, name string, op client.TxnOperator) (engine.Database, error) {
	if db, ok := e.dbs[name]; ok {
		return db, nil
	}
	return nil, moerr.NewBadDB(ctx, name)
}

func (e *stubEngine) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return 1, nil
}

type stubDatabase struct {
	engine.Database
	name         string
	rels         map[string]*stubRelation
	createErr    error
	relExistsErr error
}

func newStubDatabase(name string) *stubDatabase {
	return &stubDatabase{name: name, rels: make(map[string]*stubRelation)}
}

func (db *stubDatabase) Relation(ctx context.Context, name string, op any) (engine.Relation, error) {
	if rel, ok := db.rels[name]; ok {
		return rel, nil
	}
	return nil, moerr.NewNoSuchTable(ctx, db.name, name)
}

func (db *stubDatabase) RelationExists(ctx context.Context, name string, op any) (bool, error) {
	if db.relExistsErr != nil {
		return false, db.relExistsErr
	}
	_, ok := db.rels[name]
	return ok, nil
}

func (db *stubDatabase) Create(ctx context.Context, name string, defs []engine.TableDef) error {
	if db.createErr != nil {
		return db.createErr
	}
	db.rels[name] = newStubRelation(name)
	return nil
}

func (db *stubDatabase) GetDatabaseId(ctx context.Context) string {
	return "1"
}

type stubRelation struct {
	engine.Relation
	name string
}

func newStubRelation(name string) *stubRelation {
	return &stubRelation{name: name}
}

func (r *stubRelation) GetTableID(ctx context.Context) uint64 {
	return 1
}

func (r *stubRelation) GetPrimaryKeys(ctx context.Context) ([]*engine.Attribute, error) {
	return nil, nil
}

func (r *stubRelation) TableDefs(ctx context.Context) ([]engine.TableDef, error) {
	return nil, nil
}

func (r *stubRelation) GetTableDef(ctx context.Context) *plan.TableDef {
	return &plan.TableDef{}
}

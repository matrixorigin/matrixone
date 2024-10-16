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

package catalog

// XXX this API is broken.  In case of inplementing a cursor like interface
// we cannot use error.  moerr is a very heavy mechanism.
//
// Return a int code.
type Processor interface {
	OnDatabase(database *DBEntry) error
	OnPostDatabase(database *DBEntry) error
	OnTable(table *TableEntry) error
	OnPostTable(table *TableEntry) error
	OnPostObject(object *ObjectEntry) error
	OnObject(object *ObjectEntry) error
	OnTombstone(tombstone *ObjectEntry) error
}

type LoopProcessor struct {
	DatabaseFn     func(*DBEntry) error
	TableFn        func(*TableEntry) error
	ObjectFn       func(*ObjectEntry) error
	PostDatabaseFn func(*DBEntry) error
	PostTableFn    func(*TableEntry) error
	PostObjectFn   func(*ObjectEntry) error
	TombstoneFn    func(*ObjectEntry) error
}

func (p *LoopProcessor) OnDatabase(database *DBEntry) error {
	if p.DatabaseFn != nil {
		return p.DatabaseFn(database)
	}
	return nil
}

func (p *LoopProcessor) OnPostDatabase(database *DBEntry) error {
	if p.PostDatabaseFn != nil {
		return p.PostDatabaseFn(database)
	}
	return nil
}

func (p *LoopProcessor) OnTable(table *TableEntry) error {
	if p.TableFn != nil {
		return p.TableFn(table)
	}
	return nil
}

func (p *LoopProcessor) OnPostTable(table *TableEntry) error {
	if p.PostTableFn != nil {
		return p.PostTableFn(table)
	}
	return nil
}

func (p *LoopProcessor) OnPostObject(Object *ObjectEntry) error {
	if p.PostObjectFn != nil {
		return p.PostObjectFn(Object)
	}
	return nil
}

func (p *LoopProcessor) OnObject(Object *ObjectEntry) error {
	if p.ObjectFn != nil {
		return p.ObjectFn(Object)
	}
	return nil
}
func (p *LoopProcessor) OnTombstone(tombstone *ObjectEntry) error {
	if p.TombstoneFn != nil {
		return p.TombstoneFn(tombstone)
	}
	return nil
}

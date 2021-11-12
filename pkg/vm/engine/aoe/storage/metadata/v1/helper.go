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

package metadata

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type Cleaner struct {
	Catalog *Catalog
}

func NewCleaner(catalog *Catalog) *Cleaner {
	return &Cleaner{
		Catalog: catalog,
	}
}

func (cleaner *Cleaner) TryCompactDB(db *Database, dropTableFn func(*Table) error) error {
	if !db.IsDeleted() {
		return errors.New("Cannot compact active database")
	}
	tables := make([]*Table, 0)
	processor := new(LoopProcessor)
	processor.TableFn = func(t *Table) error {
		var err error
		if t.IsHardDeleted() {
			return err
		}
		tables = append(tables, t)
		return err
	}

	db.RLock()
	db.LoopLocked(processor)
	db.RUnlock()

	if len(tables) == 0 {
		return db.SimpleHardDelete()
	}
	var err error
	for _, t := range tables {
		if t.IsDeleted() {
			continue
		}
		if err = dropTableFn(t); err != nil {
			logutil.Warn(err.Error())
			break
		}
	}
	return err
}

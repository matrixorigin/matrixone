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

package local

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
)

type localRoDatabase struct {
	dbimpl *db.DB
}

func NewLocalRoDatabase(dbimpl *db.DB) *localRoDatabase {
	return &localRoDatabase{
		dbimpl: dbimpl,
	}
}

func (d *localRoDatabase) Type() int {
	panic("not supported")
}

func (d *localRoDatabase) Relations() []string {
	return d.dbimpl.TableNames()
}

func (d *localRoDatabase) Relation(name string) (engine.Relation, error) {
	impl, err := d.dbimpl.Relation(name)
	if err != nil {
		return nil, err
	}
	return NewLocalRoRelation(impl), nil
}

func (d *localRoDatabase) Delete(uint64, string) error {
	panic("not supported")
}

func (d *localRoDatabase) Create(uint64, string, []engine.TableDef, *engine.PartitionBy, *engine.DistributionBy, string) error {
	panic("not supported")
}

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
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/db"
)

type localRoEngine struct {
	dbimpl *db.DB
}

func NewLocalRoEngine(dbimpl *db.DB) *localRoEngine {
	return &localRoEngine{
		dbimpl: dbimpl,
	}
}

func (e *localRoEngine) Node(_ string) *engine.NodeInfo {
	panic("not supported")
}

func (e *localRoEngine) Delete(name string) error {
	panic("not supported")
}

func (e *localRoEngine) Create(_ string, _ int) error {
	panic("not supported")
}

func (e *localRoEngine) Databases() []string {
	panic("not supported")
}

func (e *localRoEngine) Database(_ string) (engine.Database, error) {
	return NewLocalRoDatabase(e.dbimpl), nil
}

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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var (
	_ engine.Relation = (*sysRelation)(nil)
)

func newSysRelation(h handle.Relation) *sysRelation {
	r := &sysRelation{}
	r.handle = h
	r.nodes = append(r.nodes, engine.Node{
		Addr: ADDR,
	})
	return r
}

func isSysRelation(name string) bool {
	if name == catalog.SystemTable_DB_Name ||
		name == catalog.SystemTable_Table_Name ||
		name == catalog.SystemTable_Columns_Name {
		return true
	}
	return false
}

func (s *sysRelation) Write(_ uint64, _ *batch.Batch, _ engine.Snapshot) error {
	return ErrReadOnly
}

func (s *sysRelation) Update(_ uint64, _ *batch.Batch, _ engine.Snapshot) error {
	return ErrReadOnly
}

func (s *sysRelation) Delete(_ uint64, _ *vector.Vector, _ string, _ engine.Snapshot) error {
	return ErrReadOnly
}

func (s *sysRelation) Truncate(_ engine.Snapshot) (uint64, error) {
	return 0, ErrReadOnly
}

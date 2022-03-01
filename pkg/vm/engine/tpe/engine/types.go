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

package engine

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/computation"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
)

var _ engine.Engine = &TpeEngine{}
var _ engine.Database = &TpeDatabase{}
var _ engine.Relation = &TpeRelation{}
var _ engine.Reader = &TpeReader{}

type TpeConfig struct {

}

type TpeEngine struct {
	computeHandler computation.ComputationHandler
}

type TpeDatabase struct {
	id uint64
	desc *descriptor.DatabaseDesc
	computeHandler computation.ComputationHandler
}

type TpeRelation struct {
	id uint64
	dbDesc *descriptor.DatabaseDesc
	desc *descriptor.RelationDesc
	computeHandler computation.ComputationHandler
}

type TpeReader struct {
	dbDesc         *descriptor.DatabaseDesc
	tableDesc      *descriptor.RelationDesc
	computeHandler computation.ComputationHandler
	prefix []byte
	prefixLen int
	//for test
	isDumpReader bool
}
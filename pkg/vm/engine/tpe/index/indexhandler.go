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

package index

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
)

type IndexHandler interface {

	ReadFromIndex(db *descriptor.DatabaseDesc, table *descriptor.RelationDesc, index *descriptor.IndexDesc, attrs []*descriptor.AttributeDesc, prefix []byte, prefixLen int) (*batch.Batch, int, []byte, int, error)

	WriteIntoTable(table *descriptor.RelationDesc,bat *batch.Batch) error

	WriteIntoIndex(db *descriptor.DatabaseDesc, table *descriptor.RelationDesc, index *descriptor.IndexDesc, attrs []descriptor.AttributeDesc, writeCtx interface{}, bat *batch.Batch) error

	DeleteFromTable(table *descriptor.RelationDesc,bat *batch.Batch) error

	DeleteFromIndex(index *descriptor.IndexDesc,attrs []descriptor.AttributeDesc,bat *batch.Batch) error
}
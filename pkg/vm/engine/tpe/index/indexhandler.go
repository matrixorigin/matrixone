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

	ReadFromIndex(readCtx interface{}) (*batch.Batch, int, error)

	WriteIntoTable(table *descriptor.RelationDesc, writeCtx interface{}, bat *batch.Batch) error

	WriteIntoIndex(writeCtx interface{}, bat *batch.Batch) error

	UpdateIntoIndex(writeCtx interface{}, bat *batch.Batch) error

	DeleteFromTable(writeCtx interface{}, bat *batch.Batch) error

	DeleteFromIndex(writeCtx interface{}, bat *batch.Batch) error
}
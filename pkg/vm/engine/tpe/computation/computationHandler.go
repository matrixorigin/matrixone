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

package computation

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"

type ComputationHandler interface {
	CreateDatabase(epoch uint64, dbName string, typ int) (uint64, error)

	DropDatabase(epoch uint64, dbName string) error

	GetDatabase(dbName string) (*descriptor.DatabaseDesc, error)

	ListDatabases() ([]*descriptor.DatabaseDesc, error)

	CreateTable(epoch, dbId uint64, tableDesc *descriptor.RelationDesc) (uint64, error)

	DropTable(epoch, dbId uint64, tableName string) (uint64, error)

	DropTableByDesc(epoch, dbId uint64, tableDesc *descriptor.RelationDesc) (uint64, error)

	ListTables(dbId uint64) ([]*descriptor.RelationDesc, error)

	GetTable(name string) (*descriptor.RelationDesc, error)
}
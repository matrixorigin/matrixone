// Copyright 2024 Matrix Origin
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

package testutil

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

type TxnOperation interface {
	CreateDatabase(ctx context.Context, databaseName string, e engine.Engine,
		op client.TxnOperator) (response *txn.TxnResponse, dbId uint64)

	CreateTable(ctx context.Context, db engine.Database,
		schema *catalog.Schema, ts timestamp.Timestamp) (response *txn.TxnResponse, tblId uint64)

	//AlterTable()
	//DropTable()
	//DropDatabase()

	//Select()
	//Insert()
	//Delete()
	//Update()
}

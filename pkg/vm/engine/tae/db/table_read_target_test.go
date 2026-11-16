// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	taecatalog "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

func TestTableReadTargetForSharedCatalogTenant(t *testing.T) {
	catalogState := taecatalog.MockCatalog(nil)
	catalogDB, err := catalogState.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	if err != nil {
		t.Fatal(err)
	}
	entry := taecatalog.NewSystemTableEntry(catalogDB, 100, taecatalog.NewEmptySchema("mo_mysql_compatibility_mode"))
	if err := catalogDB.AddEntryLocked(entry, nil, true); err != nil {
		t.Fatal(err)
	}

	manager := &txnbase.TxnManager{}
	recovered := types.BuildTS(10, 0)
	manager.MaxCommittedTS.Store(&recovered)
	manager.InitializeTableReadTargets()

	tae := &DB{Catalog: catalogState, TxnMgr: manager}
	tae.TxnMode.Store(uint32(DBTxnMode_Write))
	target, ok := tae.tableReadTarget(api.TableID{
		DbId:   pkgcatalog.MO_CATALOG_ID,
		TbId:   100,
		AccId:  42,
		DbName: pkgcatalog.MO_CATALOG,
		TbName: "mo_mysql_compatibility_mode",
	})
	if !ok {
		t.Fatal("shared catalog target was rejected for a tenant request")
	}
	if !target.Equal(recovered.ToTimestamp()) {
		t.Fatalf("target %v, want recovery floor %v", target, recovered.ToTimestamp())
	}
}

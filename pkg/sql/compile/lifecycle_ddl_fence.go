// Copyright 2026 Matrix Origin
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

package compile

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type lifecycleDDLQuery func(string, int32) (executor.Result, error)

// ignoreMissingLifecycleCatalog keeps ordinary management DDL usable while a
// tenant's asynchronous version upgrade has not created the Lifecycle tables
// yet. Table absence proves that no Lifecycle Binding can exist; every other
// Catalog error remains fail-closed.
func ignoreMissingLifecycleCatalog(err error) error {
	if err == nil || moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
		return nil
	}
	return err
}

func rejectBoundLifecycleDDL(
	ctx context.Context,
	accountID uint32,
	physicalTableID uint64,
	operation string,
	query lifecycleDDLQuery,
) error {
	// Lifecycle bindings are forbidden in the system account. Keep ordinary
	// system-account DDL completely outside the Lifecycle Catalog path.
	if accountID == 0 {
		return nil
	}
	if query == nil || physicalTableID == 0 {
		return moerr.NewInternalError(ctx, "Lifecycle DDL fence input is incomplete")
	}
	result, err := query(
		fmt.Sprintf(
			`select binding_id from mo_catalog.mo_lifecycle_bindings
where account_id=%d and physical_table_id=%d
and state in ('ACTIVE','PAUSED','BLOCKED') limit 1`,
			accountID,
			physicalTableID,
		),
		int32(accountID),
	)
	if err != nil {
		return ignoreMissingLifecycleCatalog(err)
	}
	defer result.Close()
	bound := false
	result.ReadRows(func(rows int, _ []*vector.Vector) bool {
		bound = rows > 0
		return false
	})
	if bound {
		return moerr.NewNotSupportedf(
			ctx,
			"%s on a Lifecycle-bound table; UNSET LIFECYCLE first",
			operation,
		)
	}
	return nil
}

func (c *Compile) rejectBoundLifecycleDDL(
	physicalTableID uint64,
	operation string,
) error {
	// Lifecycle bindings are a user-facing control-plane contract. Background
	// and internal DDL re-entry must keep using the ordinary MO path and must
	// not acquire a new Lifecycle Catalog dependency.
	if !c.proc.Base.IsFrontend {
		return nil
	}
	// The caller and SET LIFECYCLE already hold the same target mo_tables row
	// lock. That existing lock closes their first-Binding race, so table DDL
	// needs only one indexed Binding lookup and no cluster-wide Lifecycle lock.
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	return rejectBoundLifecycleDDL(
		c.proc.Ctx,
		accountID,
		physicalTableID,
		operation,
		c.runSqlWithResult,
	)
}

func (c *Compile) detachLifecycleBindingForDrop(
	physicalTableID uint64,
) error {
	if !c.proc.Base.IsFrontend {
		return nil
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	return ignoreMissingLifecycleCatalog(c.runSqlWithAccountId(
		fmt.Sprintf(
			`delete from mo_catalog.mo_lifecycle_bindings
where account_id=%d and physical_table_id=%d`,
			accountID,
			physicalTableID,
		),
		int32(accountID),
	))
}

func lifecycleDatabaseDropBindingDeleteSQL(
	accountID uint32,
	databaseID uint64,
) string {
	return fmt.Sprintf(
		`delete from mo_catalog.mo_lifecycle_bindings
where account_id=%d and database_id=%d`,
		accountID,
		databaseID,
	)
}

func (c *Compile) detachLifecycleBindingsForDatabaseDrop(
	accountID uint32,
	databaseID uint64,
) error {
	if !c.proc.Base.IsFrontend {
		return nil
	}
	return ignoreMissingLifecycleCatalog(c.runSqlWithAccountId(
		lifecycleDatabaseDropBindingDeleteSQL(accountID, databaseID),
		int32(accountID),
	))
}

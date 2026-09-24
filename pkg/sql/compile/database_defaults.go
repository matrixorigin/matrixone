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
	"fmt"
	"math"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (c *Compile) readDatabaseDefaults(db engine.Database) (*plan.DatabaseDefaults, error) {
	id, err := strconv.ParseUint(db.GetDatabaseId(c.proc.Ctx), 10, 64)
	if err != nil {
		return nil, err
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return nil, err
	}
	result, err := c.runSqlWithResult(plan2.DatabaseDefaultsSelectSQL(accountID, id), int32(accountID))
	if err != nil {
		return nil, err
	}
	defer result.Close()
	return plan2.DecodeDatabaseDefaults(c.proc.Ctx, result, id)
}

func (c *Compile) insertDatabaseDefaults(db engine.Database, defaults *plan.DatabaseDefaults, version uint64) error {
	id, err := strconv.ParseUint(db.GetDatabaseId(c.proc.Ctx), 10, 64)
	if err != nil {
		return err
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	// These two values originate in NormalizeDatabaseDefaults, not raw SQL.
	if defaults.CharacterSet != "utf8mb4" || (defaults.Collation != "utf8mb4_bin" && defaults.Collation != "utf8mb4_general_ci") || version == 0 {
		return moerr.NewInvalidInput(c.proc.Ctx, "invalid database defaults")
	}
	return c.runSql(fmt.Sprintf("insert into mo_catalog.%s (account_id,database_id,character_set,collation_name,version) values (%d,%d,'%s','%s',%d)",
		catalog.MODatabaseDefaults, accountID, id, defaults.CharacterSet, defaults.Collation, version))
}

func (c *Compile) deleteDatabaseDefaults(databaseID uint64) error {
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	return c.runSql(fmt.Sprintf("delete from mo_catalog.%s where account_id=%d and database_id=%d", catalog.MODatabaseDefaults, accountID, databaseID))
}

func (s *Scope) AlterDatabase(c *Compile) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()
	if err := plan2.RequireDatabaseDefaults(c.proc.Ctx, c.proc.GetService()); err != nil {
		return err
	}
	qry := s.Plan.GetDdl().GetAlterDatabase()
	name := qry.Database
	if name == "" {
		return moerr.NewNoDB(c.proc.Ctx)
	}
	if plan2.DatabaseDefaultsSystemDatabase(name) {
		return moerr.NewNotSupported(c.proc.Ctx, "altering system database defaults")
	}
	if err := lockMoDatabase(c, name, lock.LockMode_Exclusive); err != nil {
		return err
	}
	db, err := c.e.Database(c.proc.Ctx, name, c.proc.GetTxnOperator())
	if err != nil {
		return convertDBEOB(c.proc.Ctx, err, name)
	}
	if db.IsSubscription(c.proc.Ctx) {
		return moerr.NewNotSupported(c.proc.Ctx, "altering subscription database defaults")
	}
	// CCPR database writability is already checked by the shared frontend
	// database-write admission, including prepared execution.
	current, err := c.readDatabaseDefaults(db)
	if err != nil {
		return err
	}
	if qry.Defaults == nil {
		return moerr.NewInvalidInput(c.proc.Ctx, "missing database defaults")
	}
	if current.CharacterSet == qry.Defaults.CharacterSet && current.Collation == qry.Defaults.Collation {
		c.setAffectedRows(0)
		return nil
	}
	if current.Version == math.MaxUint64 {
		return moerr.NewInternalError(c.proc.Ctx, "database defaults version exhausted")
	}
	if current.Version != 0 {
		if err := c.deleteDatabaseDefaults(current.DatabaseId); err != nil {
			return err
		}
	}
	if err := c.insertDatabaseDefaults(db, qry.Defaults, current.Version+1); err != nil {
		return err
	}
	c.setAffectedRows(0)
	return nil
}

// The logical plan's types were bound using this database generation. Check it
// after the database lock, before any table/index is created. The standard
// definition-change retry rebuilds the complete plan and all derived types.
func (c *Compile) validateDatabaseDefaults(db engine.Database, expected *plan.DatabaseDefaults) error {
	if expected == nil {
		return nil
	}
	if err := plan2.RequireDatabaseDefaults(c.proc.Ctx, c.proc.GetService()); err != nil {
		return err
	}
	actual, err := c.readDatabaseDefaults(db)
	if err != nil {
		return err
	}
	if actual.DatabaseId != expected.DatabaseId || actual.Version != expected.Version ||
		actual.CharacterSet != expected.CharacterSet || actual.Collation != expected.Collation {
		return moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
	}
	return nil
}

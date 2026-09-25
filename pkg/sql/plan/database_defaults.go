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

package plan

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// DatabaseDefaultsEnabled is the cluster-wide rollout gate. Missing protocol
// state is an old cluster, not permission to write metadata old CNs ignore.
func DatabaseDefaultsEnabled(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	v, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	version, ok := v.(int64)
	return ok && version >= defines.MORPCVersion96
}

func DatabaseDefaultsSystemDatabase(name string) bool {
	switch strings.ToLower(name) {
	case "mo_catalog", "information_schema", "mysql", "system", "system_metrics", "mo_task", "mo_debug":
		return true
	}
	return false
}

func RequireDatabaseDefaults(ctx context.Context, service string) error {
	if !DatabaseDefaultsEnabled(service) {
		return moerr.NewNotSupportedf(ctx, "database charset/collation defaults require cluster protocol version %d", defines.MORPCVersion96)
	}
	return nil
}

// NormalizeDatabaseDefaults only promises the two UTF-8 collation identities
// implemented by MatrixOne. In particular UCA aliases are not persisted as if
// their advertised MySQL semantics were implemented.
func NormalizeDatabaseDefaults(ctx context.Context, options []tree.CreateOption, fallback string) (*plan.DatabaseDefaults, error) {
	var charset, collation string
	for _, option := range options {
		switch opt := option.(type) {
		case *tree.CreateOptionCharset:
			value := strings.ToLower(opt.Charset)
			if charset != "" && charset != value {
				return nil, moerr.NewInvalidInput(ctx, "conflicting database character sets")
			}
			charset = value
		case *tree.CreateOptionCollate:
			value := strings.ToLower(opt.Collate)
			if collation != "" && collation != value {
				return nil, moerr.NewInvalidInput(ctx, "conflicting database collations")
			}
			collation = value
		case *tree.CreateOptionEncryption:
			return nil, moerr.NewNotSupported(ctx, "database ENCRYPTION")
		default:
			return nil, moerr.NewNotSupported(ctx, "database option")
		}
	}
	if charset != "" && charset != "utf8mb4" {
		return nil, moerr.NewInvalidInputf(ctx, "unsupported database character set '%s'", charset)
	}
	if collation == "" {
		if charset != "" {
			collation = "utf8mb4_general_ci"
		} else {
			collation = strings.ToLower(fallback)
		}
	}
	if collation != "utf8mb4_general_ci" && collation != "utf8mb4_bin" {
		return nil, moerr.NewInvalidInputf(ctx, "unsupported database collation '%s'", collation)
	}
	return &plan.DatabaseDefaults{CharacterSet: "utf8mb4", Collation: collation, Version: 1}, nil
}

func databaseServerCollation(ctx CompilerContext) (string, error) {
	value, err := ctx.ResolveVariable("collation_server", true, false)
	if err != nil {
		return "", err
	}
	if value == nil {
		return "utf8mb4_general_ci", nil
	}
	name, ok := value.(string)
	if !ok {
		return "", moerr.NewInternalError(ctx.GetContext(), "collation_server is not a string")
	}
	if name == "" {
		name = "utf8mb4_general_ci"
	}
	return name, nil
}

func buildAlterDatabase(stmt *tree.AlterDatabase, ctx CompilerContext) (*Plan, error) {
	if err := RequireDatabaseDefaults(ctx.GetContext(), ctx.GetProcess().GetService()); err != nil {
		return nil, err
	}
	name := string(stmt.Name)
	if name == "" {
		name = ctx.DefaultDatabase()
	}
	if name == "" {
		return nil, moerr.NewNoDB(ctx.GetContext())
	}
	if err := validateIdentifier(ctx.GetContext(), name); err != nil {
		return nil, err
	}
	if DatabaseDefaultsSystemDatabase(name) {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "altering system database defaults")
	}
	defaults, err := NormalizeDatabaseDefaults(ctx.GetContext(), stmt.Options, "utf8mb4_general_ci")
	if err != nil {
		return nil, err
	}
	return &Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{
		DdlType:    plan.DataDefinition_ALTER_DATABASE,
		Definition: &plan.DataDefinition_AlterDatabase{AlterDatabase: &plan.AlterDatabase{Database: name, Defaults: defaults}},
	}}}, nil
}

func DatabaseDefaultsSelectSQL(accountID uint32, databaseID uint64) string {
	return fmt.Sprintf("select character_set, collation_name, version from mo_catalog.%s where account_id = %d and database_id = %d", catalog.MODatabaseDefaults, accountID, databaseID)
}

// DecodeDatabaseDefaults does not own result; every caller must close it even
// when the metadata is invalid. A missing row is the only legacy fallback.
func DecodeDatabaseDefaults(ctx context.Context, result executor.Result, databaseID uint64) (*plan.DatabaseDefaults, error) {
	defaults := &plan.DatabaseDefaults{DatabaseId: databaseID}
	count := 0
	var err error
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			count++
			if count > 1 || len(cols) != 3 || cols[0].IsNull(uint64(i)) || cols[1].IsNull(uint64(i)) || cols[2].IsNull(uint64(i)) {
				err = moerr.NewInternalError(ctx, "invalid database default metadata")
				return false
			}
			defaults.CharacterSet = cols[0].GetStringAt(i)
			defaults.Collation = cols[1].GetStringAt(i)
			defaults.Version = executor.GetFixedRows[uint64](cols[2])[i]
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if count > 0 && (defaults.Version == 0 || defaults.CharacterSet != "utf8mb4" ||
		(defaults.Collation != "utf8mb4_general_ci" && defaults.Collation != "utf8mb4_bin")) {
		return nil, moerr.NewInternalError(ctx, "invalid database default metadata")
	}
	return defaults, nil
}

// GetDatabaseDefaults reads through the caller's transaction and snapshot. It
// deliberately shares one implementation between frontend and internal SQL.
func GetDatabaseDefaults(ctx CompilerContext, name string, snapshot *Snapshot) (*plan.DatabaseDefaults, error) {
	if DatabaseDefaultsSystemDatabase(name) || !DatabaseDefaultsEnabled(ctx.GetProcess().GetService()) {
		return nil, nil
	}
	id, err := ctx.GetDatabaseId(name, snapshot)
	if err != nil {
		return nil, err
	}
	accountID, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	if snapshot != nil && snapshot.Tenant != nil {
		accountID = snapshot.Tenant.TenantID
	}
	available, err := databaseDefaultsAvailableAtSnapshot(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	if !available {
		return &plan.DatabaseDefaults{DatabaseId: id}, nil
	}
	result, err := runSqlWithSnapshot(ctx, DatabaseDefaultsSelectSQL(accountID, id), snapshot)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	return DecodeDatabaseDefaults(ctx.GetContext(), result, id)
}

func tableDatabaseDefaults(ctx CompilerContext, database string, options []tree.TableOption) (*plan.DatabaseDefaults, error) {
	for _, option := range options {
		switch option.(type) {
		case *tree.TableOptionCharset, *tree.TableOptionCollate:
			return nil, nil
		}
	}
	return GetDatabaseDefaults(ctx, database, nil)
}

func databaseDefaultsAvailableAtSnapshot(ctx CompilerContext, snapshot *Snapshot) (bool, error) {
	if !IsSnapshotValid(snapshot) {
		return true, nil
	}
	_, def, err := ctx.Resolve(catalog.MO_CATALOG, catalog.MODatabaseDefaults, snapshot)
	if err != nil {
		return false, err
	}
	// A historical catalog lookup proves whether this snapshot predates the
	// new table. Current-state missing tables never use this fallback.
	return def != nil, nil
}

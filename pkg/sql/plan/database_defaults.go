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
	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// DatabaseDefaultsEnabled requires cluster-wide admission, not a local flag.
func DatabaseDefaultsEnabled(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	v, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := v.(int64)
	return ok && valid && version >= defines.MORPCVersion109
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
		return moerr.NewNotSupportedf(ctx, "database charset/collation defaults require cluster protocol version %d", defines.MORPCVersion109)
	}
	return nil
}

// DatabaseDefaultsFromIdentity derives display names from the admitted semantic
// identity. Catalog reads never re-resolve a spelling through compatibility aliases.
func DatabaseDefaultsFromIdentity(ctx context.Context, identity, revision uint32, version uint64) (*plan.DatabaseDefaults, error) {
	if identity == uint32(collation.LegacyIdentity) || version == 0 {
		return nil, moerr.NewInvalidInput(ctx, "invalid database default metadata")
	}
	if err := collation.RequireLegacy(identity, revision, collation.KeyFormatLegacy); err != nil {
		return nil, err
	}
	d, err := collation.EffectiveDefinition(identity, revision)
	if err != nil {
		return nil, err
	}
	return &plan.DatabaseDefaults{
		CharacterSet: d.Charset.Name(), Collation: d.Name, Version: version,
		CollationId: identity, CollationRevision: revision,
	}, nil
}

// NormalizeDatabaseDefaults shares table/column DDL admission, including the
// distinction between legacy aliases and executable versioned UCA identities.
func NormalizeDatabaseDefaults(ctx context.Context, options []tree.CreateOption, fallback string) (*plan.DatabaseDefaults, error) {
	var charsetName, collationName string
	for _, option := range options {
		switch opt := option.(type) {
		case *tree.CreateOptionCharset:
			value := strings.ToLower(opt.Charset)
			if charsetName != "" && charsetName != value {
				return nil, moerr.NewInvalidInput(ctx, "conflicting database character sets")
			}
			charsetName = value
		case *tree.CreateOptionCollate:
			value := strings.ToLower(opt.Collate)
			if collationName != "" && collationName != value {
				return nil, moerr.NewInvalidInput(ctx, "conflicting database collations")
			}
			collationName = value
		case *tree.CreateOptionEncryption:
			return nil, moerr.NewNotSupported(ctx, "database ENCRYPTION")
		default:
			return nil, moerr.NewNotSupported(ctx, "database option")
		}
	}
	identity := uint32(types.CharsetUTF8)
	if charsetName != "" {
		var ok bool
		identity, ok = charsetForName(charsetName)
		if !ok {
			return nil, moerr.NewInvalidInputf(ctx, "unsupported character set '%s'", charsetName)
		}
	}
	if collationName == "" && charsetName == "" {
		collationName = fallback
	}
	if collationName != "" {
		var ok bool
		identity, ok = collationForName(collationName)
		if !ok {
			return nil, unsupportedCollationError(ctx, collationName)
		}
	}
	if charsetName != "" && collationName != "" && !charsetAndCollationCompatible(charsetName, collationName) {
		return nil, moerr.NewInvalidInputf(ctx, "COLLATION '%s' is not valid for CHARACTER SET '%s'", collationName, charsetName)
	}
	revision := uint32(types.CollationVersionLegacy)
	if types.IsUnicodeCollation(uint8(identity)) {
		revision = uint32(types.CollationVersionV1)
	}
	return DatabaseDefaultsFromIdentity(ctx, identity, revision, 1)
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
	return fmt.Sprintf("select collation_id, collation_revision, version from mo_catalog.%s where account_id = %d and database_id = %d", catalog.MODatabaseDefaults, accountID, databaseID)
}

// DecodeDatabaseDefaults borrows result. Its caller owns Close even on failure.
// Missing rows alone represent a legacy database, not corrupt metadata.
func DecodeDatabaseDefaults(ctx context.Context, result executor.Result, databaseID uint64) (*plan.DatabaseDefaults, error) {
	defaults := &plan.DatabaseDefaults{DatabaseId: databaseID}
	count := 0
	var readErr error
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if len(cols) != 3 || cols[0].GetType().Oid != types.T_uint32 || cols[1].GetType().Oid != types.T_uint32 || cols[2].GetType().Oid != types.T_uint64 {
			readErr = moerr.NewInternalError(ctx, "invalid database default metadata")
			return false
		}
		for i := 0; i < rows; i++ {
			count++
			if count > 1 || cols[0].IsNull(uint64(i)) || cols[1].IsNull(uint64(i)) || cols[2].IsNull(uint64(i)) {
				readErr = moerr.NewInternalError(ctx, "invalid database default metadata")
				return false
			}
			defaults, readErr = DatabaseDefaultsFromIdentity(ctx, executor.GetFixedRows[uint32](cols[0])[i], executor.GetFixedRows[uint32](cols[1])[i], executor.GetFixedRows[uint64](cols[2])[i])
			if readErr != nil {
				return false
			}
			defaults.DatabaseId = databaseID
		}
		return true
	})
	return defaults, readErr
}

// GetDatabaseDefaults uses the caller's transaction/snapshot for both frontend
// and background planning. Only a historical missing table permits fallback.
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
	return def != nil, err
}

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

package plan

import (
	"context"
	"encoding/hex"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type temporaryTableOwner interface {
	OwnsTemporaryTable(database, physicalName string) bool
}

// TemporaryTableAccessAllowed is the shared relation-resolution gate for
// session temporary roots and their generated index relations. Durable kind
// is authoritative; a name prefix alone must never hide a permanent object.
// Internal cleanup receives an explicit context capability, while a temporary
// DDL staging alias is allowed before its owner publishes the reverse map.
func TemporaryTableAccessAllowed(
	ctx context.Context,
	session any,
	database, physical string,
	tableDef *TableDef,
	extraInfo *api.SchemaExtra,
	aliasResolved bool,
) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	if tableDef == nil {
		return true
	}
	temporary := tableDef.TableType == catalog.SystemTemporaryTable ||
		(tableDef.TableType == catalog.SystemIndexRel && isGeneratedTemporaryName(physical, database)) ||
		function.IsLegacyTemporaryTable(ctx, tableDef.TableType, physical, database, tableDef.Createsql, string(api.MustMarshalTblExtra(extraInfo))) ||
		(isGeneratedTemporaryName(physical, database) && !isUserVisibleRelationKind(tableDef.TableType))
	if !temporary {
		return true
	}
	if defines.HasTemporaryTableAccess(ctx) || aliasResolved {
		return true
	}
	owner, ok := session.(temporaryTableOwner)
	if !ok {
		return false
	}
	return owner.OwnsTemporaryTable(database, physical)
}

func isUserVisibleRelationKind(kind string) bool {
	switch kind {
	case catalog.SystemOrdinaryRel, catalog.SystemViewRel, catalog.SystemExternalRel,
		catalog.SystemMaterializedRel, catalog.SystemSourceRel, catalog.SystemClusterRel,
		catalog.SystemPartitionRel, catalog.SystemSequenceRel:
		return true
	default:
		return false
	}
}

// isGeneratedTemporaryName mirrors the legacy catalog fallback: the session
// UUID and database component are fixed, while the alias may contain '_'.
func isGeneratedTemporaryName(name, database string) bool {
	if database == "" || !defines.IsTempTableName(name) {
		return false
	}
	remainder := strings.TrimPrefix(name, defines.TempTableNamePrefix)
	if len(remainder) <= 33 || remainder[32] != '_' || len(remainder) <= 33+len(database)+1 {
		return false
	}
	if _, err := hex.DecodeString(remainder[:32]); err != nil {
		return false
	}
	prefix := database + "_"
	return strings.HasPrefix(remainder[33:], prefix) && len(remainder[33+len(prefix):]) > 0
}

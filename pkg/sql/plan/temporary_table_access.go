// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

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
	aliasResolved bool,
) bool {
	if tableDef == nil {
		return true
	}
	temporary := tableDef.TableType == catalog.SystemTemporaryTable ||
		(tableDef.TableType == catalog.SystemIndexRel && defines.IsTempTableName(physical))
	if !temporary {
		return true
	}
	if defines.HasTemporaryTableAccess(ctx) || aliasResolved {
		return true
	}
	owner, ok := session.(process.TemporaryTableDDL)
	return ok && owner.OwnsTemporaryTable(database, physical)
}

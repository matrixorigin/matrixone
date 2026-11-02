// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0. See the LICENSE file.

package maintenance

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
)

// RunSQL requires scoped generated-operator provenance from the SQL owner.
func RunSQL(ctx compileplugin.CompileContext, sql, function, indexName string, roles ...string) error {
	if runner, ok := ctx.(compileplugin.IndexMaintenanceRunner); ok {
		return runner.RunIndexMaintenanceSQL(sql, function, indexName, roles...)
	}
	return moerr.NewInternalErrorNoCtx("index maintenance owner cannot attest generated SQL")
}

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package sysview

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestMetadataVisibilitySQL(t *testing.T) {
	for _, version := range []int64{defines.MORPCVersion11, defines.MORPCVersion12} {
		for _, database := range []string{"select", `a'b\\c`} {
			sql := MetadataVisibilityCTE(version, database) + "SELECT rel_id FROM __mo_visible_tables"
			_, err := mysql.ParseOne(t.Context(), sql, 1)
			require.NoError(t, err)
			require.Equal(t, 2, strings.Count(sql, "SELECT DISTINCT rp.obj_id"))
			require.Contains(t, sql, "SELECT COUNT(*) AS n")
			require.Contains(t, sql, "tbl.rel_logical_id = e.obj_id")
			require.NotContains(t, sql, "EXISTS (")
		}
	}
}

func TestSubscriptionMetadataVisibilitySQL(t *testing.T) {
	for _, version := range []int64{defines.MORPCVersion11, defines.MORPCVersion12} {
		sql := SubscriptionMetadataVisibilitySQL("'db'", version) + " WHERE EXISTS (SELECT 1 FROM mo_catalog.mo_database d WHERE d.datname = 'db') LIMIT 1"
		_, err := mysql.ParseOne(t.Context(), sql, 1)
		require.NoError(t, err)
		require.NotContains(t, sql, "rel_logical_id")
		require.Contains(t, sql, "SELECT DISTINCT rp.obj_id")
	}
}

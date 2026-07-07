// Copyright 2021 - 2024 Matrix Origin
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

package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// parseColumnType parses a single-column CREATE TABLE and returns the column's
// parsed type.
func parseColumnType(t *testing.T, colSQL string) *tree.T {
	t.Helper()
	stmt, err := ParseOne(context.Background(), "create table t (g "+colSQL+")", 1)
	require.NoError(t, err)
	ct, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)
	require.Len(t, ct.Defs, 1)
	col, ok := ct.Defs[0].(*tree.ColumnTableDef)
	require.True(t, ok)
	return col.Type.(*tree.T)
}

func TestSpatialTypeAliases(t *testing.T) {
	cases := []struct {
		sql         string
		subtype     string
		float32     bool
		srid        uint32
		sridDefined bool
	}{
		// base float64 family
		{"geometry", "GEOMETRY", false, 0, false},
		{"point", "POINT", false, 0, false},
		{"linestring", "LINESTRING", false, 0, false},
		{"polygon", "POLYGON", false, 0, false},
		{"multipoint", "MULTIPOINT", false, 0, false},
		{"multilinestring", "MULTILINESTRING", false, 0, false},
		{"multipolygon", "MULTIPOLYGON", false, 0, false},
		{"geometrycollection", "GEOMETRYCOLLECTION", false, 0, false},
		// geography = generic geometry defaulting to SRID 4326
		{"geography", "GEOMETRY", false, 4326, true},
		// float32 family
		{"geometry32", "GEOMETRY", true, 0, false},
		{"point32", "POINT", true, 0, false},
		{"multipolygon32", "MULTIPOLYGON", true, 0, false},
		{"geography32", "GEOMETRY", true, 4326, true},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			typ := parseColumnType(t, c.sql)
			require.NotNil(t, typ.InternalType.GeoMetadata, "GeoMetadata must be set")
			gm := typ.InternalType.GeoMetadata
			require.Equal(t, c.subtype, gm.Subtype)
			require.Equal(t, c.float32, gm.Float32)
			require.Equal(t, c.sridDefined, gm.SRIDDefined)
			if c.sridDefined {
				require.Equal(t, c.srid, gm.SRID)
			}
		})
	}
}

// The new alias keywords remain non-reserved, so they are still usable as
// identifiers (column names).
func TestSpatialAliasesNonReserved(t *testing.T) {
	for _, name := range []string{"geometry32", "geography", "point32", "multipolygon32"} {
		_, err := ParseOne(context.Background(), "create table t ("+name+" int)", 1)
		require.NoError(t, err, "%s should be usable as a column name", name)
	}
}

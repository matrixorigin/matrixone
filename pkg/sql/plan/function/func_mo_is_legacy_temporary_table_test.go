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

package function

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

func TestIsLegacyTemporaryTableAssociatesCreatingStatement(t *testing.T) {
	const physicalPrefix = "__mo_tmp_123e4567e89b12d3a456426614174000_test_db_"
	tests := []struct {
		name      string
		relKind   string
		relName   string
		database  string
		createSQL string
		extraInfo string
		want      bool
	}{
		{
			name:      "permanent exact physical name wins over temporary alias",
			relKind:   catalog.SystemOrdinaryRel,
			relName:   physicalPrefix + "t",
			database:  "test_db",
			createSQL: "CREATE TEMPORARY TABLE t(id int); CREATE TABLE __mo_tmp_123e4567e89b12d3a456426614174000_test_db_t(id int)",
		},
		{
			name:      "qualified permanent exact physical name wins over temporary alias",
			relKind:   catalog.SystemOrdinaryRel,
			relName:   physicalPrefix + "t",
			database:  "test_db",
			createSQL: "CREATE TEMPORARY TABLE t(id int); CREATE TABLE test_db.__mo_tmp_123e4567e89b12d3a456426614174000_test_db_t(id int)",
		},
		{
			name:      "renamed permanent physical name stays visible",
			relKind:   catalog.SystemOrdinaryRel,
			relName:   physicalPrefix + "t",
			database:  "test_db",
			createSQL: "CREATE TEMPORARY TABLE t(id int); CREATE TABLE permanent_t(id int)",
			extraInfo: string(api.MustMarshalTblExtra(&api.SchemaExtra{OldName: "permanent_t"})),
		},
		{
			name:      "malformed rename metadata fails open",
			relKind:   catalog.SystemOrdinaryRel,
			relName:   physicalPrefix + "t",
			database:  "test_db",
			createSQL: "CREATE TEMPORARY TABLE t(id int)",
			extraInfo: "not protobuf",
		},
		{
			name:      "temporary then permanent classifies temporary",
			relKind:   catalog.SystemOrdinaryRel,
			relName:   physicalPrefix + "temp_t",
			database:  "test_db",
			createSQL: "CREATE TEMPORARY TABLE temp_t(id int); CREATE TABLE permanent_t(id int)",
			want:      true,
		},
		{
			name:      "permanent row in temporary-first request stays visible",
			relKind:   catalog.SystemOrdinaryRel,
			relName:   physicalPrefix + "permanent_t",
			database:  "test_db",
			createSQL: "CREATE TEMPORARY TABLE temp_t(id int); CREATE TABLE permanent_t(id int)",
		},
		{
			name:      "permanent then temporary",
			relKind:   catalog.SystemOrdinaryRel,
			relName:   physicalPrefix + "temp_t",
			database:  "test_db",
			createSQL: "CREATE TABLE permanent_t(id int); CREATE TEMPORARY TABLE temp_t(id int)",
			want:      true,
		},
		{
			name:      "comments between keywords",
			relKind:   catalog.SystemOrdinaryRel,
			relName:   physicalPrefix + "temp_t",
			database:  "test_db",
			createSQL: "CREATE /* first */ TEMPORARY -- second\n TABLE `test_db`.`temp_t`(id int)",
			want:      true,
		},
		{
			name:      "different temporary alias",
			relKind:   catalog.SystemOrdinaryRel,
			relName:   physicalPrefix + "permanent_t",
			database:  "test_db",
			createSQL: "CREATE TEMPORARY TABLE temp_t(id int)",
		},
		{
			name:      "new marker handled outside compatibility function",
			relKind:   catalog.SystemTemporaryTable,
			relName:   physicalPrefix + "temp_t",
			database:  "test_db",
			createSQL: "CREATE TEMPORARY TABLE temp_t(id int)",
		},
		{
			name:      "malformed request fails open",
			relKind:   catalog.SystemOrdinaryRel,
			relName:   physicalPrefix + "temp_t",
			database:  "test_db",
			createSQL: "CREATE TEMPORARY TABLE",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, isLegacyTemporaryTable(
				context.Background(), test.relKind, test.relName, test.database, test.createSQL, test.extraInfo,
			))
		})
	}
}

func TestLegacyTemporaryTableLogicalName(t *testing.T) {
	name, ok := legacyTemporaryTableLogicalName(
		"__mo_tmp_123e4567e89b12d3a456426614174000_db_with_underscores_alias_with_underscores",
		"db_with_underscores",
	)
	require.True(t, ok)
	require.Equal(t, "alias_with_underscores", name)

	_, ok = legacyTemporaryTableLogicalName("__mo_tmp_not-a-session_db_t", "db")
	require.False(t, ok)
}

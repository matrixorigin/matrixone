// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestNormalizeConnectionType(t *testing.T) {
	typ, err := normalizeConnectionType(context.Background(), " PG ")
	require.NoError(t, err)
	require.Equal(t, "postgresql", typ)
}

func TestNormalizeConnectionOptionsRejectsDuplicates(t *testing.T) {
	_, err := normalizeConnectionOptions(context.Background(), []*tree.ConnectionOption{
		{Key: tree.Identifier("host"), Value: "127.0.0.1"},
		{Key: tree.Identifier("HOST"), Value: "localhost"},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, `duplicate connection option "host"`)
}

func TestValidateConnectionOptionsOracleNeedsServiceNameOrSID(t *testing.T) {
	err := validateConnectionOptions(context.Background(), "oracle", map[string]string{
		"host":     "127.0.0.1",
		"port":     "1521",
		"user":     "hr",
		"password": "pw",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "oracle connection requires service_name or sid")
}

func TestBuildShowCreateConnectionSQLMasksSecrets(t *testing.T) {
	sql := buildShowCreateConnectionSQL("conn1", "mysql", map[string]string{
		"password": "pw'1",
		"host":     "127.0.0.1",
		"port":     "3306",
		"user":     "report_user",
	})

	require.Equal(
		t,
		"create connection conn1 type = 'mysql' options (host = '127.0.0.1', password = '***', port = '3306', user = 'report_user')",
		sql,
	)
	require.NotContains(t, sql, "pw''1")
}

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

package tree

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestIcebergOptionFormatRedactsSensitiveValues(t *testing.T) {
	stmt := &CreateIcebergCatalog{
		Name: "ksa",
		Options: IcebergOptions{
			NewIcebergOption("uri", "https://catalog.example/rest"),
			NewIcebergOption("token_secret", "secret://catalog/raw-token"),
			NewIcebergOption("s3.access-key-id", "raw-access-key"),
		},
	}
	formatted := String(stmt, dialect.MYSQL)
	require.Contains(t, formatted, `"token_secret" = '<redacted>'`)
	require.Contains(t, formatted, `"s3.access-key-id" = '<redacted>'`)
	require.NotContains(t, formatted, "raw-token")
	require.NotContains(t, formatted, "raw-access-key")
}

func TestIcebergShowFormatUsesFrozenFromSyntax(t *testing.T) {
	require.Equal(t, "show iceberg namespaces from ksa", String(&ShowIcebergNamespaces{Catalog: "ksa"}, dialect.MYSQL))
	require.Equal(t, "show iceberg tables from ksa.sales", String(&ShowIcebergTables{Catalog: "ksa", Namespace: "sales"}, dialect.MYSQL))
}

func TestIcebergOptionFormatEscapesStringValues(t *testing.T) {
	formatted := String(NewIcebergOption("uri", `https://catalog.example/a\'b`), dialect.MYSQL)
	if strings.Contains(formatted, `a\'b`) {
		t.Fatalf("expected backslash quote to be escaped: %s", formatted)
	}
	require.Contains(t, formatted, `a\\''b`)
}

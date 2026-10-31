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

package testutil

import (
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/require"
)

func TestEngineTableDefBySchemaPreservesNullability(t *testing.T) {
	schema := catalog.MockSchemaAll(2, 1)
	defs, err := EngineTableDefBySchema(schema)
	require.NoError(t, err)
	columns, err := pkgcatalog.GenColumnsFromDefs(0, "t", "db", 2, 1, defs)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(columns), 2)
	require.Equal(t, int8(0), columns[0].NotNull)
	require.Equal(t, int8(1), columns[1].NotNull)
}

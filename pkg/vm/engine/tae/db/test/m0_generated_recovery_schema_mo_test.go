//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/require"
)

func generatedRecoveryAlterSchema(t *testing.T) *catalog.Schema {
	t.Helper()
	schema := catalog.NewEmptySchema("generated_target")
	require.NoError(t, schema.AppendCol("a", types.T_int32.ToType()))
	require.NoError(t, schema.AppendCol("b", types.T_int32.ToType()))
	require.NoError(t, schema.AppendPKCol(pkgcatalog.CPrimaryKeyColName, types.T_varchar.ToType(), 0))
	schema.ColDefs[2].Hidden = true
	require.NoError(t, schema.Finalize(false))
	return schema
}

func generatedRecoveryIncludeLossCases() bool { return true }

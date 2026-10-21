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

package jobs

import (
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/require"
)

func TestM0WriteSchemaVersionUsesCurrentCompletePrefix(t *testing.T) {
	schema := catalog.NewEmptySchema("customer")
	for _, name := range []string{"a", "b"} {
		require.NoError(t, schema.AppendCol(name, types.T_int32.ToType()))
	}
	require.NoError(t, schema.AppendPKCol(pkgcatalog.CPrimaryKeyColName, types.T_varchar.ToType(), 0))
	schema.ColDefs[2].Hidden = true
	require.NoError(t, schema.Finalize(false))
	require.NoError(t, schema.ApplyAlterTable(api.NewAddColumnReq(
		0, 0, "tier", plan.Type{Id: int32(types.T_varchar)}, -1,
	)))
	require.NoError(t, schema.ApplyAlterTable(api.NewAddColumnReq(
		0, 0, "credit", plan.Type{Id: int32(types.T_int32)}, -1,
	)))
	schema.Version = 1

	complete := []uint16{0, 1, 2, 3, 4, objectio.SEQNUM_COMMITTS}
	require.Equal(t, uint32(2), m0WriteSchemaVersion(schema, complete, schema.Version))
	require.Equal(t, schema.Version, m0WriteSchemaVersion(
		schema, []uint16{0, 1, 2, 4, objectio.SEQNUM_COMMITTS}, schema.Version,
	))
}

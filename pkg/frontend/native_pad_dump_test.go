// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNativePADTableDumpLegacyManifestAndHash(t *testing.T) {
	source := &pbplan.TableDef{
		Name: "n", DefaultCharset: uint32(types.CharsetUTF8MB4Bin),
		Createsql: "create table n (s varchar(8) collate utf8mb4_bin) collate utf8mb4_bin",
		Cols: []*pbplan.ColDef{{Name: "s", OriginName: "s",
			Typ:     pbplan.Type{Id: int32(types.T_varchar), Width: 8, Charset: uint32(types.CharsetUTF8MB4Bin)},
			Default: &pbplan.Default{NullAbility: true}}},
	}
	before := proto.Clone(source)
	// This is the spelling hashed by existing manifests, independent of the
	// new portable rendering. Changing it would reject otherwise valid dumps.
	frozen := "CREATE TABLE `__table_dump_target__` (\n  `s` varchar(8) COLLATE utf8mb4_bin DEFAULT NULL\n) COLLATE=utf8mb4_bin"
	sum := sha256.Sum256([]byte(frozen))
	got, err := tableSchemaHash(source)
	require.NoError(t, err)
	assert.Equal(t, hex.EncodeToString(sum[:]), got, "existing manifest hash is a persisted format")

	text, err := tableDumpManifestCreateSQL(t.Context(), source, nil)
	require.NoError(t, err)
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		old, exists := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if exists {
				rt.SetGlobalVariables(key, old)
			} else if value, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, value)
			}
		})
		rt.SetGlobalVariables(key, defines.MORPCVersion125)
	}
	ctx := plan.NewMockCompilerContext(false, proc)
	ctx.SetContext(plan.WithCollationBindingRevision(t.Context(), 1))
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, text, 1)
	require.NoError(t, err)
	defer stmt.Free()
	rebuilt, err := plan.BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	target := rebuilt.GetDdl().GetCreateTable().TableDef
	require.Zero(t, target.CollationVersion, "manifest SQL must not upgrade the table default")
	require.Equal(t, source.Cols[0].Typ, target.Cols[0].Typ, "manifest SQL must not upgrade columns")
	require.Equal(t, before, source)
}

func TestNativePADTableDumpRejectsBeforeNormalization(t *testing.T) {
	for _, source := range []*pbplan.TableDef{
		{Name: "n", Createsql: "create table n (s varchar(8) collate utf8mb4_general_ci)",
			Cols: []*pbplan.ColDef{{Name: "s", Typ: pbplan.Type{Id: int32(types.T_varchar), Width: 8, Charset: 3, CollationVersion: 1}}}},
		{Name: "n", TableType: catalog.SystemIndexRel, DefaultCharset: 3, CollationVersion: 1,
			Cols: []*pbplan.ColDef{{Name: "i", Typ: pbplan.Type{Id: int32(types.T_int64)}}}},
		{Name: "v", TableType: catalog.SystemViewRel, ViewSql: &pbplan.ViewDef{View: `{"Stmt":"create view v as select true as b","collation_binding_revision":1,"required_protocol_version":125}`},
			Cols: []*pbplan.ColDef{{Name: "b", Typ: pbplan.Type{Id: int32(types.T_bool)}}}},
	} {
		t.Run(source.Name+"/"+source.TableType, func(t *testing.T) {
			before := proto.Clone(source)
			_, err := tableSchemaHash(source)
			assert.Error(t, err, "fallback hashing must not hide native source obligations")
			_, err = tableLogicalSchemaHashWithResolver(t.Context(), source, nil)
			assert.Error(t, err, "normalization must not erase native defaults or View provenance")
			_, err = tableDumpManifestCreateSQL(t.Context(), source, nil)
			assert.Error(t, err, "raw Createsql is not proof of standalone reconstruction")
			require.Equal(t, before, source)
		})
	}
}

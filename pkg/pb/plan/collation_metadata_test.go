// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestCollationMetadataWireAndAdmission(t *testing.T) {
	typ := Type{Id: 61, Width: 8, Charset: 4, CollationVersion: 1,
		CollationCoercibilitySet: true, CollationCoercibility: 0, CollationMergeConflict: true}
	table := &TableDef{DefaultCharset: 4, CollationVersion: 1, KeyFormat: 1,
		Cols: []*ColDef{{Name: "v", Typ: typ}}, Indexes: []*IndexDef{{KeyFormat: 1}}}
	wire, err := table.Marshal()
	require.NoError(t, err)
	var restored TableDef
	require.NoError(t, restored.Unmarshal(wire))
	require.True(t, proto.Equal(table, &restored))
	require.Error(t, RequireLegacyCollations(&restored), "known metadata is not production admission")
	for _, owner := range []any{typ, &typ, table, table.Indexes[0],
		&Expr{Typ: typ}, []*Type{&typ}, map[string]Type{"v": typ},
		struct{ Types []Type }{[]Type{typ}}, struct{ Schema *TableDef }{table}} {
		require.Error(t, RequireLegacyCollations(owner), "%T", owner)
	}
	legacy := Type{Id: 61, Charset: 3, CollationCoercibilitySet: true}
	require.NoError(t, RequireLegacyCollations(&legacy))
	require.NoError(t, RequireLegacyCollations((*Plan)(nil)))
	require.NoError(t, RequireLegacyCollations(&TableDef{DefaultCharset: 3, Cols: []*ColDef{{Typ: legacy}}}))
	require.NoError(t, RequireLegacyCollations(Type{Id: 23, Charset: 255}))
	type cyclic struct {
		Next *cyclic
		Typ  Type
		Data []byte
	}
	cycle := &cyclic{Typ: legacy, Data: []byte{255}}
	cycle.Next = cycle
	require.NoError(t, RequireLegacyCollations(cycle))
	for _, mutate := range []func(*Type){
		func(t *Type) { t.CollationVersion = 1 },
		func(t *Type) { t.CollationCoercibilitySet = false },
		func(t *Type) { t.CollationCoercibility = 2 },
		func(t *Type) { t.CollationMergeConflict = true },
		func(t *Type) { t.Charset = 2 },
	} {
		other := legacy
		mutate(&other)
		require.False(t, legacy.SameCollation(other))
	}
	require.True(t, legacy.SameCollation(legacy))
}

func TestCollationMetadataUnknownValuesFailClosed(t *testing.T) {
	for _, typ := range []Type{
		{Id: 61, Charset: 259}, {Id: 61, Charset: 255}, {Id: 61, Charset: 4},
		{Id: 317, Charset: 255}, {Id: -1}, {Id: 23, Charset: 254},
		{Id: 61, Charset: 3, CollationVersion: 2}, {Id: 61, Charset: 3, CollationVersion: 257},
		{Id: 61, Charset: 3, CollationCoercibilitySet: true, CollationCoercibility: 7},
		{Id: 61, Charset: 3, CollationCoercibility: 2}, {Id: 23, CollationVersion: 1},
	} {
		wire, err := typ.Marshal()
		require.NoError(t, err)
		var restored Type
		require.Error(t, restored.Unmarshal(wire))
		require.Error(t, RequireLegacyCollations(&typ))
	}
	for _, table := range []*TableDef{{KeyFormat: 2}, {CollationVersion: 2}, {DefaultCharset: 260}, {DefaultCharset: 4}} {
		wire, err := table.Marshal()
		require.NoError(t, err)
		var restored TableDef
		require.Error(t, restored.Unmarshal(wire))
		require.Error(t, RequireLegacyCollations(table))
	}
	index := &IndexDef{KeyFormat: 257}
	wire, err := index.Marshal()
	require.NoError(t, err)
	var restored IndexDef
	require.Error(t, restored.Unmarshal(wire))
	require.Error(t, RequireLegacyCollations(index))
}

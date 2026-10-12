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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestAlterTableCharsetContract(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion110)
	for _, tc := range []struct {
		name, source, option, failure string
		algorithm                     plan.AlterTable_AlgorithmType
		defaultIdentity               uint32
		columns                       map[string]uint32
		indexes                       []string
	}{
		{name: "default does not convert existing columns", option: "default collate=utf8mb4_general_ci", algorithm: plan.AlterTable_INPLACE, defaultIdentity: 3, columns: map[string]uint32{"v": 2}},
		{name: "add before default", option: "add column added varchar(8), default collate=utf8mb4_general_ci", algorithm: plan.AlterTable_COPY, defaultIdentity: 3, columns: map[string]uint32{"v": 2, "added": 3}},
		{name: "default with drop index", source: "create table charset_target(v varchar(8), index iv(v)) collate=utf8mb4_bin", option: "default collate=utf8mb4_general_ci, drop index iv", algorithm: plan.AlterTable_INPLACE, defaultIdentity: 3, columns: map[string]uint32{"v": 2}, indexes: []string{}},
		{name: "default with add index", option: "add index iv(v), default collate=utf8mb4_general_ci", algorithm: plan.AlterTable_INPLACE, defaultIdentity: 3, columns: map[string]uint32{"v": 2}, indexes: []string{"iv"}},
		{name: "default before add", option: "default collate=utf8mb4_general_ci, add column added varchar(8)", algorithm: plan.AlterTable_COPY, defaultIdentity: 3, columns: map[string]uint32{"v": 2, "added": 3}},
		{name: "default before modify requires domain validation", option: "default collate=utf8_unicode_ci, modify v varchar(8)", algorithm: plan.AlterTable_COPY, defaultIdentity: 9, columns: map[string]uint32{"v": 9}},
		{name: "default after modify requires domain validation", option: "modify v varchar(8), default collate=utf8_unicode_ci", algorithm: plan.AlterTable_COPY, defaultIdentity: 9, columns: map[string]uint32{"v": 9}},
		{name: "default before change requires domain validation", option: "default collate=utf8_unicode_ci, change v v varchar(8)", algorithm: plan.AlterTable_COPY, defaultIdentity: 9, columns: map[string]uint32{"v": 9}},
		{name: "default after change requires domain validation", option: "change v v varchar(8), default collate=utf8_unicode_ci", algorithm: plan.AlterTable_COPY, defaultIdentity: 9, columns: map[string]uint32{"v": 9}},
		{name: "final default before modify preserves stored domain", source: "create table charset_target(v varchar(8) collate utf8mb4_bin) collate utf8_unicode_ci", option: "default collate=utf8mb4_bin, modify v varchar(8)", algorithm: plan.AlterTable_INPLACE, defaultIdentity: 2, columns: map[string]uint32{"v": 2}},
		{name: "final default after modify preserves stored domain", source: "create table charset_target(v varchar(8) collate utf8mb4_bin) collate utf8_unicode_ci", option: "modify v varchar(8), default collate=utf8mb4_bin", algorithm: plan.AlterTable_INPLACE, defaultIdentity: 2, columns: map[string]uint32{"v": 2}},
		{name: "final default before change preserves stored domain", source: "create table charset_target(v varchar(8) collate utf8mb4_bin) collate utf8_unicode_ci", option: "default collate=utf8mb4_bin, change v v varchar(8)", algorithm: plan.AlterTable_INPLACE, defaultIdentity: 2, columns: map[string]uint32{"v": 2}},
		{name: "final default after change preserves stored domain", source: "create table charset_target(v varchar(8) collate utf8mb4_bin) collate utf8_unicode_ci", option: "change v v varchar(8), default collate=utf8mb4_bin", algorithm: plan.AlterTable_INPLACE, defaultIdentity: 2, columns: map[string]uint32{"v": 2}},
		{name: "unchanged domain admits explicit inplace", source: "create table charset_target(v varchar(8) collate utf8mb4_bin) collate utf8_unicode_ci", option: "default collate=utf8mb4_bin, modify v varchar(8), algorithm=inplace", algorithm: plan.AlterTable_INPLACE, defaultIdentity: 2, columns: map[string]uint32{"v": 2}},
		{name: "unchanged domain permits explicit copy", source: "create table charset_target(v varchar(8) collate utf8mb4_bin) collate utf8_unicode_ci", option: "default collate=utf8mb4_bin, modify v varchar(8), algorithm=copy", algorithm: plan.AlterTable_COPY, defaultIdentity: 2, columns: map[string]uint32{"v": 2}},
		{name: "explicit unchanged column domain overrides inheritance", option: "default collate=utf8_unicode_ci, modify v varchar(8) collate utf8mb4_bin", algorithm: plan.AlterTable_COPY, defaultIdentity: 9, columns: map[string]uint32{"v": 2}},
		{name: "domain change cannot force inplace", option: "default collate=utf8_unicode_ci, modify v varchar(8), algorithm=inplace", failure: "requires ALGORITHM=COPY"},
		{name: "domain change cannot force instant", option: "change v v varchar(8), default collate=utf8_unicode_ci, algorithm=instant", failure: "requires ALGORITHM=COPY"},
		{name: "domain change cannot use lock none", option: "modify v varchar(8), default collate=utf8_unicode_ci, lock=none", failure: "COPY algorithm"},
		{name: "native conversion carries revision", option: "convert to character set utf8mb4 collate utf8mb4_unicode_ci", algorithm: plan.AlterTable_COPY, defaultIdentity: 10, columns: map[string]uint32{"v": 10}},
		{name: "conversion owns new columns but not final default", option: "convert to character set utf8mb4 collate utf8mb4_unicode_ci, default collate=utf8mb4_general_ci, add column added varchar(8)", algorithm: plan.AlterTable_COPY, defaultIdentity: 3, columns: map[string]uint32{"v": 10, "added": 10}},
		{name: "binary conversion", option: "convert to character set binary", algorithm: plan.AlterTable_COPY, defaultIdentity: 1, columns: map[string]uint32{"v": 1}},
		{name: "conversion cannot use inplace", option: "convert to character set utf8mb4, algorithm=inplace", failure: "requires ALGORITHM=COPY"},
		{name: "conversion cannot use lock none", option: "convert to character set utf8mb4, lock=none", failure: "COPY algorithm"},
		{name: "reject disabled target before add", option: "add column added int, convert to character set gbk", failure: "unsupported character set"},
		{name: "preserve native unique admission", source: "create table charset_target(v varchar(8) unique) collate=utf8mb4_bin", option: "convert to character set utf8mb4 collate utf8mb4_unicode_ci", failure: "unique index"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			sourceSQL := tc.source
			if sourceSQL == "" {
				sourceSQL = "create table charset_target(v varchar(8)) collate=utf8mb4_bin"
			}
			source, err := buildTestCreateTableStmt(mock, sourceSQL)
			require.NoError(t, err)
			mock.ctxt.tables["charset_target"] = source
			mock.ctxt.objects["charset_target"] = &ObjectRef{SchemaName: "tpch", ObjName: "charset_target"}
			if len(source.Indexes) != 0 {
				registerAlterIndexVisibilityRows(t, mock, source)
			}
			before, err := source.Marshal()
			require.NoError(t, err)
			p, err := buildSingleStmt(mock, t, "alter table charset_target "+tc.option)
			after, marshalErr := source.Marshal()
			require.NoError(t, marshalErr)
			require.Equal(t, before, after, "planning must not mutate the source catalog")
			if tc.failure != "" {
				require.ErrorContains(t, err, tc.failure)
				return
			}
			require.NoError(t, err)
			alter := p.GetDdl().GetAlterTable()
			require.Equal(t, tc.algorithm, alter.AlgorithmType)
			require.NotNil(t, alter.CopyTableDef)
			require.Equal(t, tc.defaultIdentity, alter.CopyTableDef.DefaultCharset)
			if tc.indexes != nil {
				names := make([]string, 0, len(alter.CopyTableDef.Indexes))
				for _, index := range alter.CopyTableDef.Indexes {
					names = append(names, index.IndexName)
				}
				require.ElementsMatch(t, tc.indexes, names)
			}
			for name, identity := range tc.columns {
				column := FindColumn(alter.CopyTableDef.Cols, name)
				require.NotNil(t, column, name)
				require.Equal(t, identity, column.Typ.Charset, name)
				if identity == 9 || identity == 10 {
					require.Equal(t, uint32(1), column.Typ.CollationVersion, name)
				}
			}
			if tc.algorithm == plan.AlterTable_COPY && tc.defaultIdentity == 10 {
				require.Equal(t, uint32(1), alter.CopyTableDef.CollationVersion)
			}
		})
	}
}

func TestAlterCopyCollationInvalidatesDedupProof(t *testing.T) {
	old := &ColDef{Name: "v", Typ: plan.Type{Id: int32(types.T_varchar), Charset: 2}}
	changed := *old
	changed.Typ.Charset = 3
	require.False(t, alterCopyKeyColumnValueUnchanged(old, &changed))
	changed = *old
	changed.Typ.CollationVersion = 1
	require.False(t, alterCopyKeyColumnValueUnchanged(old, &changed))
	changed = *old
	require.True(t, alterCopyKeyColumnValueUnchanged(old, &changed))
}

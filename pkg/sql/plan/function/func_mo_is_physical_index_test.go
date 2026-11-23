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

package function

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestIsPhysicalIndexCatalogRows(t *testing.T) {
	indexExtra := api.MustMarshalTblExtra(&api.SchemaExtra{FeatureFlag: features.IndexTable})
	otherExtra := api.MustMarshalTblExtra(&api.SchemaExtra{FeatureFlag: features.Partition})
	for _, tc := range []struct {
		label, name, kind string
		extra             []byte
		want              bool
	}{
		{"ordinary legacy", "ordinary", catalog.SystemOrdinaryRel, nil, false},
		{"ordinary empty kind", "ordinary", "", nil, false},
		{"system persistent", "mo_tables", catalog.SystemPersistRel, nil, false},
		{"system transient", "ordinary", catalog.SystemTransientRel, nil, false},
		{"view", "ordinary", catalog.SystemViewRel, nil, false},
		{"unrelated feature", "ordinary", catalog.SystemOrdinaryRel, otherExtra, false},
		{"durable index flag", "renamed_child", catalog.SystemOrdinaryRel, indexExtra, true},
		{"temporary index flag", "renamed_child", catalog.SystemTemporaryTable, indexExtra, true},
		{"legacy index kind", "renamed_child", catalog.SystemIndexRel, nil, true},
		{"unknown kind", "renamed_child", "plugin_kind", nil, true},
		{"case insensitive prefix", strings.ToUpper(catalog.IndexTableNamePrefix) + "child", catalog.SystemOrdinaryRel, nil, true},
		{"similar ordinary name", "orders__mo_index_child", catalog.SystemOrdinaryRel, nil, false},
		{"malformed unknown", "ordinary", catalog.SystemOrdinaryRel, []byte{0xff}, true},
		{"malformed known", "renamed_child", catalog.SystemIndexRel, []byte{0xff}, true},
	} {
		t.Run(tc.label, func(t *testing.T) {
			require.Equal(t, tc.want, isPhysicalIndex(tc.name, tc.kind, tc.extra))
		})
	}
}

func TestIsPhysicalIndexVectorNullAndSelection(t *testing.T) {
	proc := testutil.NewProcess(t)
	indexExtra := string(api.MustMarshalTblExtra(&api.SchemaExtra{FeatureFlag: features.IndexTable}))
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"ordinary", "renamed", "child", "ordinary", "ordinary"}, []bool{false, false, false, true, false}),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{catalog.SystemOrdinaryRel, catalog.SystemOrdinaryRel, catalog.SystemIndexRel, catalog.SystemOrdinaryRel, catalog.SystemOrdinaryRel}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"", indexExtra, "", "", ""}, []bool{false, false, false, false, true}),
	}
	for _, tc := range []struct {
		name      string
		selection *FunctionSelectList
		want      []bool
		nulls     []bool
	}{
		{"all", nil, []bool{false, true, true, false, false}, []bool{false, false, false, true, true}},
		{"partial", &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true, true, true}}, []bool{false, false, true, false, false}, []bool{false, true, false, true, true}},
		{"none", &FunctionSelectList{AllNull: true}, []bool{false, false, false, false, false}, []bool{true, true, true, true, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(types.T_bool.ToType(), false, tc.want, tc.nulls), builtInIsPhysicalIndex).WithSelectList(tc.selection)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		})
	}
}

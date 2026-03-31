// Copyright 2024 Matrix Origin
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

package readutil

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestTryUpdateColumns_FulltextBF(t *testing.T) {
	tableDef := &plan.TableDef{
		Name:      "__mo_index_secondary_ft",
		TableType: catalog.FullTextIndex_TblType,
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: catalog.FakePrimaryKeyColName,
			Names:       []string{catalog.FakePrimaryKeyColName},
		},
		Cols: []*plan.ColDef{
			{Name: catalog.FullTextIndex_TabCol_Id, Seqnum: 10, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "pos", Seqnum: 11, Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "word", Seqnum: 12, Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: catalog.FakePrimaryKeyColName, Seqnum: 13, Typ: plan.Type{Id: int32(types.T_int64)}},
		},
		Name2ColIndex: map[string]int32{
			catalog.FullTextIndex_TabCol_Id: 0,
			"pos":                           1,
			"word":                          2,
			catalog.FakePrimaryKeyColName:   3,
		},
	}

	mixin := withFilterMixin{
		tableDef: tableDef,
	}
	mixin.filterState.hasBF = true

	// Internal SQL only queries doc_id, pos, word (no __mo_fake_pk_col)
	cols := []string{catalog.FullTextIndex_TabCol_Id, "pos", "word"}
	mixin.tryUpdateColumns(cols)

	// filterState.seqnums should use doc_id column (seqnum=10)
	require.Equal(t, []uint16{10}, mixin.filterState.seqnums)
	require.Len(t, mixin.filterState.colTypes, 1)
	require.Equal(t, types.T_int64, mixin.filterState.colTypes[0].Oid)
}

func TestTryUpdateColumns_FulltextNoBF(t *testing.T) {
	tableDef := &plan.TableDef{
		Name:      "__mo_index_secondary_ft",
		TableType: catalog.FullTextIndex_TblType,
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: catalog.FakePrimaryKeyColName,
			Names:       []string{catalog.FakePrimaryKeyColName},
		},
		Cols: []*plan.ColDef{
			{Name: catalog.FullTextIndex_TabCol_Id, Seqnum: 10, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "pos", Seqnum: 11, Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "word", Seqnum: 12, Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: catalog.FakePrimaryKeyColName, Seqnum: 13, Typ: plan.Type{Id: int32(types.T_int64)}},
		},
		Name2ColIndex: map[string]int32{
			catalog.FullTextIndex_TabCol_Id: 0,
			"pos":                           1,
			"word":                          2,
			catalog.FakePrimaryKeyColName:   3,
		},
	}

	mixin := withFilterMixin{
		tableDef: tableDef,
	}
	mixin.filterState.hasBF = false

	cols := []string{catalog.FullTextIndex_TabCol_Id, "pos", "word"}
	mixin.tryUpdateColumns(cols)

	// Without BF, filterState.seqnums should NOT be set for fulltext
	require.Nil(t, mixin.filterState.seqnums)
}

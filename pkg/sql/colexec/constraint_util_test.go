// Copyright 2025 Matrix Origin
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

package colexec

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

type tempSession struct {
	m map[string]string
}

func (s *tempSession) GetTempTable(dbName, alias string) (string, bool) {
	val, ok := s.m[alias]
	return val, ok
}

func (s *tempSession) AddTempTable(string, string, string) {}
func (s *tempSession) RemoveTempTable(string, string)      {}
func (s *tempSession) RemoveTempTableByRealName(string)    {}
func (s *tempSession) GetSqlModeNoAutoValueOnZero() (bool, bool) {
	return false, false
}

func TestGetRelationByObjRefTempAlias(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEngine := mock_frontend.NewMockEngine(ctrl)
	mockDb := mock_frontend.NewMockDatabase(ctrl)
	mockRel := mock_frontend.NewMockRelation(ctrl)

	proc := testutil.NewProc(t)
	proc.Session = &tempSession{m: map[string]string{"t1": "real_t1"}}

	ref := &plan.ObjectRef{
		SchemaName: "db",
		ObjName:    "t1",
	}

	mockEngine.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(mockDb, nil)
	mockDb.EXPECT().Relation(gomock.Any(), "real_t1", gomock.Any()).Return(mockRel, nil)

	rel, err := getRelationByObjRef(proc.Ctx, proc, mockEngine, ref)
	require.NoError(t, err)
	assert.Equal(t, mockRel, rel)
}

func TestBatchDataNotNullCheckResolvesAttributesByName(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{
				Name:       "__mo_cpkey_col",
				OriginName: "__mo_cpkey_col",
				Hidden:     true,
				Primary:    true,
				Default:    &plan.Default{NullAbility: false},
			},
			{
				Name:       "optional",
				OriginName: "optional",
				Default:    &plan.Default{NullAbility: true},
			},
			{
				Name:       "required",
				OriginName: "required",
				Default:    &plan.Default{NullAbility: false},
			},
		},
		Name2ColIndex: map[string]int32{
			"__mo_cpkey_col": 0,
			"optional":       1,
			"required":       2,
		},
	}
	typ := types.T_int32.ToType()
	tableDef.Name2ColIndex["optional"] = 0 // stale index must fall back to the name

	optional := vector.NewConstNull(typ, 1, mp)
	defer optional.Free(mp)
	require.NoError(t, BatchDataNotNullCheck(
		[]*vector.Vector{optional}, []string{"optional"}, tableDef, t.Context(),
	))

	required := vector.NewConstNull(typ, 1, mp)
	defer required.Free(mp)
	require.ErrorContains(t, BatchDataNotNullCheck(
		[]*vector.Vector{required}, []string{"required"}, tableDef, t.Context(),
	), "Column 'required' cannot be null")
}

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

// Copyright 2021 Matrix Origin
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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestBuildReadersPassesMembershipFilter(t *testing.T) {
	t.Run("from source", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expectedMembershipFilter := []byte{1, 2, 3}

		mockRel := &mockRelationForMembershipFilter{}
		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				Rel:                   mockRel,
				MembershipFilterBytes: expectedMembershipFilter,
				node: &plan.Node{
					TableDef: &plan.TableDef{
						TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
					},
				},
			},
			NodeInfo: engine.Node{
				Mcpu: 1,
			},
		}

		c := NewMockCompile(t)
		c.proc = proc
		s.DataSource.FilterList = []*plan.Expr{plan2.MakeFalseExpr()}
		s.DataSource.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{}

		readers, err := s.buildReaders(c)
		require.NoError(t, err)
		require.NotNil(t, readers)
		require.Equal(t, expectedMembershipFilter, mockRel.capturedHint.MembershipFilterBytes)
	})

	t.Run("from context", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expectedMembershipFilter := []byte{7, 8, 9}
		proc.Ctx = context.WithValue(proc.Ctx, defines.IvfMembershipFilter{}, expectedMembershipFilter)

		mockRel := &mockRelationForMembershipFilter{}
		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				Rel:                   mockRel,
				MembershipFilterBytes: nil, // Trigger else if
				node: &plan.Node{
					TableDef: &plan.TableDef{
						TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
					},
				},
			},
			NodeInfo: engine.Node{
				Mcpu: 1,
			},
		}

		c := NewMockCompile(t)
		c.proc = proc
		s.DataSource.FilterList = []*plan.Expr{plan2.MakeFalseExpr()}
		s.DataSource.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{}

		readers, err := s.buildReaders(c)
		require.NoError(t, err)
		require.NotNil(t, readers)
		require.Equal(t, expectedMembershipFilter, mockRel.capturedHint.MembershipFilterBytes)
	})
}

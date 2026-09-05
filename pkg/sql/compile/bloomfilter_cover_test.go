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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestReaderFilterHintPrefersSourceAndFallsBackToContext(t *testing.T) {
	t.Run("from source", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expectedMembershipFilter := []byte{1, 2, 3}
		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				MembershipFilterBytes: expectedMembershipFilter,
			},
		}
		c := NewMockCompile(t)
		c.proc = proc

		hint := s.readerFilterHint(c, &plan.TableDef{
			TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
		})
		require.Equal(t, expectedMembershipFilter, hint.MembershipFilterBytes)
	})

	t.Run("from context", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expectedMembershipFilter := []byte{7, 8, 9}
		proc.Ctx = context.WithValue(proc.Ctx, defines.IvfMembershipFilter{}, expectedMembershipFilter)

		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				MembershipFilterBytes: nil,
			},
		}
		c := NewMockCompile(t)
		c.proc = proc

		hint := s.readerFilterHint(c, &plan.TableDef{
			TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
		})
		require.Equal(t, expectedMembershipFilter, hint.MembershipFilterBytes)
	})
}

func TestReaderFilterHintScopesMembershipFilterToIndexTable(t *testing.T) {
	ivfFilter := []byte{1, 2, 3}
	fulltextFilter := []byte{4, 5, 6}
	tests := []struct {
		name     string
		tableDef *plan.TableDef
		expected []byte
	}{
		{
			name:     "ordinary table ignores unrelated filters",
			tableDef: &plan.TableDef{Name: "t"},
		},
		{
			name: "IVF entries use IVF filter",
			tableDef: &plan.TableDef{
				Name:      "__mo_index_secondary_ivf_entries",
				TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
			},
			expected: ivfFilter,
		},
		{
			name: "fulltext table uses fulltext filter",
			tableDef: &plan.TableDef{
				Name:      "__mo_index_secondary_fulltext",
				TableType: catalog.FullTextIndex_TblType,
			},
			expected: fulltextFilter,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.Ctx = context.WithValue(proc.Ctx, defines.IvfMembershipFilter{}, ivfFilter)
			proc.Ctx = context.WithValue(proc.Ctx, defines.FulltextMembershipFilter{}, fulltextFilter)
			scope := &Scope{
				Proc:       proc,
				DataSource: &Source{},
			}
			compile := NewMockCompile(t)
			compile.proc = proc

			hint := scope.readerFilterHint(compile, test.tableDef)
			require.Equal(t, test.expected, hint.MembershipFilterBytes)
		})
	}
}

func TestReaderFilterHintPreservesFulltextSourceScope(t *testing.T) {
	fulltextTable := &plan.TableDef{
		Name:      "__mo_index_secondary_fulltext",
		TableType: catalog.FullTextIndex_TblType,
	}
	sourceFilter := []byte{1, 2, 3}

	t.Run("local scan ignores source field", func(t *testing.T) {
		compile := NewMockCompile(t)
		scope := &Scope{DataSource: &Source{MembershipFilterBytes: sourceFilter}}

		hint := scope.readerFilterHint(compile, fulltextTable)
		require.Nil(t, hint.MembershipFilterBytes)
	})

	t.Run("remote scan uses source field", func(t *testing.T) {
		compile := NewMockCompile(t)
		scope := &Scope{
			IsRemote:   true,
			DataSource: &Source{MembershipFilterBytes: sourceFilter},
		}

		hint := scope.readerFilterHint(compile, fulltextTable)
		require.Equal(t, sourceFilter, hint.MembershipFilterBytes)
	})
}

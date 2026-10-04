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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

type membershipFilterCaptureEngine struct {
	engine.Engine
	hint engine.FilterHint
}

func (e *membershipFilterCaptureEngine) BuildBlockReaders(
	_ context.Context,
	_ any,
	_ timestamp.Timestamp,
	_ *plan.Expr,
	_ *plan.TableDef,
	_ engine.RelData,
	_ int,
	filterHint ...engine.FilterHint,
) ([]engine.Reader, error) {
	e.hint = engine.FilterHint{}
	if len(filterHint) > 0 {
		e.hint = filterHint[0]
	}
	return []engine.Reader{new(readutil.EmptyReader)}, nil
}

func TestRemoteBuildReadersScopesMembershipFilterToIndexTable(t *testing.T) {
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
			proc.Ctx = context.WithValue(proc.Ctx, defines.FulltextMembershipFilter{}, fulltextFilter)
			capture := new(membershipFilterCaptureEngine)
			scope := &Scope{
				Proc:     proc,
				IsRemote: true,
				DataSource: &Source{
					TableDef:           test.tableDef,
					FilterList:         []*plan.Expr{plan2.MakeFalseExpr()},
					RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{},
				},
				NodeInfo: engine.Node{Mcpu: 1},
			}
			compile := NewMockCompile(t)
			compile.proc = proc
			compile.e = capture

			readers, err := scope.buildReaders(compile)
			require.NoError(t, err)
			require.Len(t, readers, 1)
			require.Equal(t, test.expected, capture.hint.MembershipFilterBytes)
		})
	}
}

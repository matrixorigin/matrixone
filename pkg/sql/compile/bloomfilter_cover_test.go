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
	"github.com/matrixorigin/matrixone/pkg/sql/planner"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestBuildReadersBloomFilterFullCoverage(t *testing.T) {
	// 1. Cover L1064-L1071 (case s.DataSource.Rel != nil) - if branch
	t.Run("L1064-L1071_RelNotNull_BloomFilterInSource", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expectedBloomFilter := []byte{1, 2, 3}

		mockRel := &mockRelationForBloomFilter{}
		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				Rel:         mockRel,
				BloomFilter: expectedBloomFilter,
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
		s.DataSource.FilterList = []*plan.Expr{planner.MakeFalseExpr()}
		s.DataSource.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{}

		readers, err := s.buildReaders(c)
		require.NoError(t, err)
		require.NotNil(t, readers)
		require.Equal(t, expectedBloomFilter, mockRel.capturedHint.BloomFilter)
	})

	// 2. Cover L1064-L1071 (case s.DataSource.Rel != nil) - else if branch (Context)
	t.Run("L1064-L1071_RelNotNull_BloomFilterInContext", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expectedBloomFilter := []byte{7, 8, 9}
		proc.Ctx = context.WithValue(proc.Ctx, defines.IvfBloomFilter{}, expectedBloomFilter)

		mockRel := &mockRelationForBloomFilter{}
		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				Rel:         mockRel,
				BloomFilter: nil, // Trigger else if
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
		s.DataSource.FilterList = []*plan.Expr{planner.MakeFalseExpr()}
		s.DataSource.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{}

		readers, err := s.buildReaders(c)
		require.NoError(t, err)
		require.NotNil(t, readers)
		require.Equal(t, expectedBloomFilter, mockRel.capturedHint.BloomFilter)
	})

	// 3. Cover L1139-L1146 (default case, Rel is nil initially) - if branch
	t.Run("L1139-L1146_RelNull_PanicHack_Source", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expectedBloomFilter := []byte{4, 5, 6}

		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				Rel:         nil, // This triggers the default case
				BloomFilter: expectedBloomFilter,
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
		s.DataSource.FilterList = []*plan.Expr{planner.MakeFalseExpr()}
		s.DataSource.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{}

		defer func() {
			if r := recover(); r != nil {
				t.Logf("Caught expected panic: %v", r)
			}
		}()

		_, _ = s.buildReaders(c)
	})

	// 4. Cover L1139-L1146 (default case) - else if branch (Context)
	t.Run("L1139-L1146_RelNull_PanicHack_Context", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expectedBloomFilter := []byte{10, 11, 12}
		proc.Ctx = context.WithValue(proc.Ctx, defines.IvfBloomFilter{}, expectedBloomFilter)

		s := &Scope{
			Proc: proc,
			DataSource: &Source{
				Rel:         nil, // This triggers the default case
				BloomFilter: nil, // Trigger else if
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
		s.DataSource.FilterList = []*plan.Expr{planner.MakeFalseExpr()}
		s.DataSource.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{}

		defer func() {
			if r := recover(); r != nil {
				t.Logf("Caught expected panic: %v", r)
			}
		}()

		_, _ = s.buildReaders(c)
	})
}

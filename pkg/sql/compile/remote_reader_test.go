// Copyright 2026 Matrix Origin
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

type readerPathCaptureEngine struct {
	engine.Engine
	buildBlockReadersCalls int
}

func (e *readerPathCaptureEngine) BuildBlockReaders(
	context.Context,
	any,
	timestamp.Timestamp,
	*plan.Expr,
	*plan.TableDef,
	engine.RelData,
	int,
	...engine.FilterHint,
) ([]engine.Reader, error) {
	e.buildBlockReadersCalls++
	return []engine.Reader{new(readutil.EmptyReader)}, nil
}

type readerPathCaptureRelation struct {
	engine.Relation
	buildReadersCalls int
	ctx               context.Context
	hint              engine.FilterHint
}

func (r *readerPathCaptureRelation) BuildReaders(
	ctx context.Context,
	_ any,
	_ *plan.Expr,
	_ engine.RelData,
	_ int,
	_ int,
	_ bool,
	_ engine.TombstoneApplyPolicy,
	filterHint engine.FilterHint,
) ([]engine.Reader, error) {
	r.buildReadersCalls++
	r.ctx = ctx
	r.hint = filterHint
	return []engine.Reader{new(readutil.EmptyReader)}, nil
}

func TestBuildReadersChoosesOwnerByScanPlacement(t *testing.T) {
	tests := []struct {
		name               string
		isRemote           bool
		cnCount            int32
		wantRelationCalls  int
		wantBlockReadCalls int
	}{
		{
			name:              "local scope reads complete relation data",
			cnCount:           2,
			wantRelationCalls: 1,
		},
		{
			name:              "single remote scope reads complete relation data",
			isRemote:          true,
			cnCount:           1,
			wantRelationCalls: 1,
		},
		{
			name:               "distributed remote scope reads persisted blocks",
			isRemote:           true,
			cnCount:            2,
			wantBlockReadCalls: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			captureEngine := new(readerPathCaptureEngine)
			captureRelation := new(readerPathCaptureRelation)
			scope := &Scope{
				Proc:     proc,
				IsRemote: test.isRemote,
				DataSource: &Source{
					Rel:                captureRelation,
					TableDef:           &plan.TableDef{Name: "t"},
					FilterList:         []*plan.Expr{plan2.MakeFalseExpr()},
					RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{},
				},
				NodeInfo: engine.Node{
					Mcpu:  1,
					CNCNT: test.cnCount,
				},
			}
			compile := NewMockCompile(t)
			compile.proc = proc
			compile.e = captureEngine

			readers, err := scope.buildReaders(compile)
			require.NoError(t, err)
			require.Len(t, readers, 1)
			require.Equal(t, test.wantRelationCalls, captureRelation.buildReadersCalls)
			require.Equal(t, test.wantBlockReadCalls, captureEngine.buildBlockReadersCalls)
		})
	}
}

func TestSingleRemoteRelationReaderPreservesRemoteMetadata(t *testing.T) {
	tests := []struct {
		name                     string
		tableName                string
		tableType                string
		account                  *plan.PubInfo
		serializedFilter         []byte
		contextFilter            []byte
		wantMembershipFilter     []byte
		wantReaderContextAccount uint32
	}{
		{
			name:                     "serialized fulltext metadata",
			tableName:                "__mo_index_secondary_fulltext",
			tableType:                catalog.FullTextIndex_TblType,
			account:                  &plan.PubInfo{TenantId: 42},
			serializedFilter:         []byte{1, 2, 3},
			wantMembershipFilter:     []byte{1, 2, 3},
			wantReaderContextAccount: 42,
		},
		{
			name:                     "context fulltext metadata fallback",
			tableName:                "__mo_index_secondary_fulltext",
			tableType:                catalog.FullTextIndex_TblType,
			account:                  &plan.PubInfo{TenantId: 43},
			contextFilter:            []byte{4, 5, 6},
			wantMembershipFilter:     []byte{4, 5, 6},
			wantReaderContextAccount: 43,
		},
		{
			name:                     "cluster table account",
			tableName:                "cluster_table",
			tableType:                catalog.SystemClusterRel,
			wantReaderContextAccount: catalog.System_Account,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			if len(test.contextFilter) > 0 {
				proc.Ctx = context.WithValue(
					proc.Ctx,
					defines.FulltextMembershipFilter{},
					test.contextFilter,
				)
			}
			tableDef := &plan.TableDef{Name: test.tableName, TableType: test.tableType}
			captureRelation := new(readerPathCaptureRelation)
			scope := &Scope{
				Proc:     proc,
				IsRemote: true,
				DataSource: &Source{
					Rel:                   captureRelation,
					node:                  &plan.Node{TableDef: tableDef},
					TableDef:              tableDef,
					AccountId:             test.account,
					MembershipFilterBytes: test.serializedFilter,
					FilterList:            []*plan.Expr{plan2.MakeFalseExpr()},
				},
				NodeInfo: engine.Node{Mcpu: 1, CNCNT: 1},
			}
			compile := NewMockCompile(t)
			compile.proc = proc
			compile.e = new(readerPathCaptureEngine)

			readers, err := scope.buildReaders(compile)
			require.NoError(t, err)
			require.Len(t, readers, 1)
			require.Equal(t, test.wantMembershipFilter, captureRelation.hint.MembershipFilterBytes)
			accountID, err := defines.GetAccountId(captureRelation.ctx)
			require.NoError(t, err)
			require.Equal(t, test.wantReaderContextAccount, accountID)
		})
	}
}

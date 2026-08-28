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

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type readerPathCaptureEngine struct {
	engine.Engine
	database               engine.Database
	buildBlockReadersCalls int
}

func (e *readerPathCaptureEngine) Database(
	context.Context,
	string,
	client.TxnOperator,
) (engine.Database, error) {
	return e.database, nil
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

type readerPathCaptureDatabase struct {
	engine.Database
	relation engine.Relation
}

func (db *readerPathCaptureDatabase) Relation(
	context.Context,
	string,
	any,
) (engine.Relation, error) {
	return db.relation, nil
}

type readerPathCaptureRelation struct {
	engine.Relation
	buildReadersCalls int
	rangesCalls       int
	rangesData        engine.RelData
	readerRelData     engine.RelData
	ctx               context.Context
	hint              engine.FilterHint
}

func (r *readerPathCaptureRelation) Ranges(
	context.Context,
	engine.RangesParam,
) (engine.RelData, error) {
	r.rangesCalls++
	return r.rangesData, nil
}

func (r *readerPathCaptureRelation) BuildReaders(
	ctx context.Context,
	_ any,
	_ *plan.Expr,
	relData engine.RelData,
	_ int,
	_ int,
	_ bool,
	_ engine.TombstoneApplyPolicy,
	filterHint engine.FilterHint,
) ([]engine.Reader, error) {
	r.buildReadersCalls++
	r.readerRelData = relData
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

func TestDecodedSingleRemoteScopeBuildsRelationReader(t *testing.T) {
	tests := []struct {
		name                 string
		tableName            string
		tableType            string
		pubInfo              *plan.PubInfo
		membershipFilter     []byte
		wantReaderAccount    uint32
		wantMembershipFilter []byte
	}{
		{
			name:                 "published fulltext table",
			tableName:            "__mo_index_secondary_fulltext",
			tableType:            catalog.FullTextIndex_TblType,
			pubInfo:              &plan.PubInfo{TenantId: 42},
			membershipFilter:     []byte{1, 2, 3},
			wantReaderAccount:    42,
			wantMembershipFilter: []byte{1, 2, 3},
		},
		{
			name:              "cluster table",
			tableName:         "cluster_table",
			tableType:         catalog.SystemClusterRel,
			wantReaderAccount: catalog.System_Account,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			tableDef := &plan.TableDef{Name: test.tableName, TableType: test.tableType}
			node := &plan.Node{
				ObjRef: &plan.ObjectRef{
					SchemaName: "test_db",
					ObjName:    tableDef.Name,
					PubInfo:    test.pubInfo,
				},
				TableDef: tableDef,
			}
			memoryRanges := readutil.NewBlockListRelationData(1)
			captureRelation := &readerPathCaptureRelation{rangesData: memoryRanges}
			captureEngine := &readerPathCaptureEngine{
				database: &readerPathCaptureDatabase{relation: captureRelation},
			}

			senderProc := testutil.NewProcess(t)
			if len(test.membershipFilter) > 0 {
				senderProc.Ctx = context.WithValue(
					senderProc.Ctx,
					defines.FulltextMembershipFilter{},
					test.membershipFilter,
				)
			}
			senderScope := &Scope{
				Magic: Remote,
				Proc:  senderProc,
				DataSource: &Source{
					Rel:          captureRelation,
					node:         node,
					TableDef:     tableDef,
					SchemaName:   node.ObjRef.SchemaName,
					RelationName: tableDef.Name,
				},
				NodeInfo: engine.Node{Mcpu: 1, CNCNT: 1},
			}
			encodeCtx := &scopeContext{regs: make(map[*process.WaitRegister]int32)}
			encodeCtx.root = encodeCtx
			encoded, _, err := generatePipeline(senderScope, encodeCtx, 1)
			require.NoError(t, err)

			remoteProc := testutil.NewProcess(t)
			remoteProc.Ctx = defines.AttachAccountId(remoteProc.Ctx, 99)
			txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
			txnOperator.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
			remoteProc.Base.TxnOperator = txnOperator
			decodeCtx := &scopeContext{regs: make(map[*process.WaitRegister]int32)}
			decodeCtx.root = decodeCtx
			decoded, err := generateScope(remoteProc, encoded, decodeCtx, true)
			require.NoError(t, err)
			require.Nil(t, decoded.DataSource.Rel,
				"relation handles are local execution state and are not serialized")

			compile := &Compile{proc: remoteProc, e: captureEngine}
			readers, err := decoded.buildReaders(compile)
			require.NoError(t, err)
			require.Len(t, readers, 1)
			require.Equal(t, 1, captureRelation.rangesCalls)
			require.Equal(t, 1, captureRelation.buildReadersCalls)
			require.Zero(t, captureEngine.buildBlockReadersCalls)
			require.Same(t, memoryRanges, captureRelation.readerRelData)
			firstBlock := captureRelation.readerRelData.GetBlockInfo(0)
			require.True(t, firstBlock.IsMemBlk())
			require.Equal(t, test.wantMembershipFilter, captureRelation.hint.MembershipFilterBytes)
			accountID, err := defines.GetAccountId(captureRelation.ctx)
			require.NoError(t, err)
			require.Equal(t, test.wantReaderAccount, accountID)
		})
	}
}

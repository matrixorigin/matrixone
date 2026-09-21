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
	"errors"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type readerPathCaptureEngine struct {
	engine.Engine
	database               engine.Database
	buildBlockReadersCalls int
	readerRelData          engine.RelData
	readerTimestamp        timestamp.Timestamp
	readerContext          context.Context
	readerHint             engine.FilterHint
}

func (e *readerPathCaptureEngine) Database(
	context.Context,
	string,
	client.TxnOperator,
) (engine.Database, error) {
	return e.database, nil
}

func (e *readerPathCaptureEngine) BuildBlockReaders(
	ctx context.Context,
	_ any,
	ts timestamp.Timestamp,
	_ *plan.Expr,
	_ *plan.TableDef,
	data engine.RelData,
	_ int,
	hints ...engine.FilterHint,
) ([]engine.Reader, error) {
	e.buildBlockReadersCalls++
	e.readerRelData = data
	e.readerTimestamp = ts
	e.readerContext = ctx
	if len(hints) > 0 {
		e.readerHint = hints[0]
	}
	return []engine.Reader{new(readutil.EmptyReader)}, nil
}

type readerPathAttachFailure struct {
	engine.RelData
	err error
}

func (r *readerPathAttachFailure) AttachTombstones(engine.Tombstoner) error { return r.err }

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
	rangesParam       engine.RangesParam
	readerRelData     engine.RelData
	ctx               context.Context
	hint              engine.FilterHint
}

func (r *readerPathCaptureRelation) Ranges(
	_ context.Context,
	param engine.RangesParam,
) (engine.RelData, error) {
	r.rangesCalls++
	r.rangesParam = param
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

func TestDecodedRemoteScopePreservesReaderContract(t *testing.T) {
	tests := []struct {
		name                 string
		tableName            string
		tableType            string
		pubInfo              *plan.PubInfo
		membershipFilter     []byte
		wantReaderAccount    uint32
		wantMembershipFilter []byte
		partitionedMultiCN   bool
		attachError          error
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
		{
			name:               "distributed partitioned table",
			tableName:          "partitioned_generated",
			wantReaderAccount:  99,
			partitionedMultiCN: true,
		},
		{
			name:               "distributed tombstone attachment failure",
			tableName:          "partitioned_generated",
			partitionedMultiCN: true,
			attachError:        errors.New("attach shipped tombstones"),
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
			var memoryRanges engine.RelData = readutil.NewBlockListRelationData(1)
			if test.partitionedMultiCN {
				memoryRanges = &disttae.CombinedRelData{}
			}
			if test.attachError != nil {
				memoryRanges = &readerPathAttachFailure{RelData: readutil.NewBlockListRelationData(1), err: test.attachError}
			}
			captureRelation := &readerPathCaptureRelation{rangesData: memoryRanges}
			captureEngine := &readerPathCaptureEngine{
				database: &readerPathCaptureDatabase{relation: captureRelation},
			}

			senderProc := testutil.NewProcess(t)
			t.Cleanup(senderProc.Free)
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
					Timestamp:    timestamp.Timestamp{PhysicalTime: 42},
				},
				NodeInfo: engine.Node{Mcpu: 1, CNCNT: 1},
			}
			if test.partitionedMultiCN {
				senderScope.NodeInfo.CNCNT = 2
				senderScope.NodeInfo.CNIDX = 1
				senderScope.NodeInfo.Data = readutil.BuildEmptyRelData()
			}
			encodeCtx := &scopeContext{regs: make(map[*process.WaitRegister]int32)}
			encodeCtx.root = encodeCtx
			encoded, _, err := generatePipeline(senderScope, encodeCtx, 1)
			require.NoError(t, err)

			remoteProc := testutil.NewProcess(t)
			t.Cleanup(remoteProc.Free)
			remoteProc.Ctx = defines.AttachAccountId(remoteProc.Ctx, 99)
			txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
			txnOperator.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
			remoteProc.Base.TxnOperator = txnOperator
			decodeCtx := &scopeContext{regs: make(map[*process.WaitRegister]int32)}
			decodeCtx.root = decodeCtx
			decoded, err := generateScope(remoteProc, encoded, decodeCtx, true)
			require.NoError(t, err)
			t.Cleanup(func() { ReleaseScopes([]*Scope{decoded}) })
			require.Nil(t, decoded.DataSource.Rel,
				"relation handles are local execution state and are not serialized")

			compile := &Compile{proc: remoteProc, e: captureEngine}
			originalData := decoded.NodeInfo.Data
			readers, err := decoded.buildReaders(compile)
			t.Cleanup(func() {
				for _, reader := range readers {
					require.NoError(t, reader.Close())
				}
			})
			if test.attachError != nil {
				require.ErrorIs(t, err, test.attachError)
				require.Empty(t, readers)
				require.Zero(t, captureEngine.buildBlockReadersCalls)
				require.Zero(t, captureRelation.buildReadersCalls)
				require.Same(t, originalData, decoded.NodeInfo.Data, "failed attachment must not publish partial ranges")
				return
			}
			require.NoError(t, err)
			require.Len(t, readers, 1)
			require.Equal(t, 1, captureRelation.rangesCalls)
			readerContext := captureRelation.ctx
			readerHint := captureRelation.hint
			if test.partitionedMultiCN {
				require.Equal(t, 1, captureEngine.buildBlockReadersCalls)
				require.Zero(t, captureRelation.buildReadersCalls)
				require.Same(t, memoryRanges, captureEngine.readerRelData)
				require.Equal(t, senderScope.DataSource.Timestamp, captureEngine.readerTimestamp)
				readerContext = captureEngine.readerContext
				readerHint = captureEngine.readerHint
				require.Equal(t, engine.DataCollectPolicy(engine.Policy_CollectCommittedPersistedData), captureRelation.rangesParam.Policy)
				require.False(t, captureRelation.rangesParam.Rsp.IsLocalCN)
				require.Equal(t, int32(2), captureRelation.rangesParam.Rsp.CNCNT)
				require.Equal(t, int32(1), captureRelation.rangesParam.Rsp.CNIDX)
			} else {
				require.Equal(t, 1, captureRelation.buildReadersCalls)
				require.Zero(t, captureEngine.buildBlockReadersCalls)
				require.Same(t, memoryRanges, captureRelation.readerRelData)
				firstBlock := captureRelation.readerRelData.GetBlockInfo(0)
				require.True(t, firstBlock.IsMemBlk())
			}
			require.Equal(t, test.wantMembershipFilter, readerHint.MembershipFilterBytes)
			accountID, err := defines.GetAccountId(readerContext)
			require.NoError(t, err)
			require.Equal(t, test.wantReaderAccount, accountID)
		})
	}
}

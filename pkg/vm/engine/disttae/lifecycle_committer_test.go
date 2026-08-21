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

package disttae

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestTxnLifecycleFinalCommitterWritesBindingFenceBeforeDataset(t *testing.T) {
	fake := &lifecycleCommitSQLExecutor{
		t: t,
		expect: []string{
			"update mo_catalog.mo_lifecycle_bindings",
			"insert into mo_catalog.mo_lifecycle_datasets",
		},
	}
	committer := TxnLifecycleFinalCommitter{SQLExecutor: fake}
	now := time.Now().UTC()
	request := LifecycleFinalizeRequest{
		Binding: lifecyclepkg.Binding{
			ID:              "00112233-4455-6677-8899-aabbccddeeff",
			AccountID:       17,
			DatabaseID:      41,
			LogicalTableID:  42,
			PhysicalTableID: 43,
			Generation:      7,
			Version:         11,
			SchemaDigest:    strings.Repeat("ab", 32),
			StageID:         9,
		},
		Root: lifecyclepkg.CleanupRoot{
			RootID:           "2d55f9be-4d3e-4ac7-a58a-1f7995d88f7f",
			AttemptID:        "e091026d-114b-44f9-81f3-326bf6481446",
			ArchiveNamespace: `{"stage_id":9}`,
			SourceSetDigest:  [32]byte{1},
		},
		Manifest: &lifecyclepkg.ArchiveManifest{
			SchemaDigest: [32]byte{2},
			ContentHash:  [32]byte{3},
			RowCount:     100,
			LogicalBytes: 2048,
			LifecycleRange: &lifecyclepkg.ArchiveLifecycleRange{
				SourceColumnID: 5,
				TypeID:         int32(types.T_timestamp),
				Min:            100,
				Max:            200,
			},
		},
		ManifestKey:     "prefix/manifest.json",
		ManifestDigest:  [32]byte{4},
		FinalTxnID:      "final-txn",
		PurgeEligibleAt: now.Add(24 * time.Hour),
		Cutoff:          now.Add(-90 * 24 * time.Hour),
		EvaluationTime:  now,
		Control: &api.LifecycleCommitEntry{
			DatasetId:        "cf177f35-7f6a-4aac-8ee3-f3d5f04cf147",
			SourceSnapshotTs: ptrTimestamp(types.BuildTS(100, 2).ToTimestamp()),
		},
	}
	require.NoError(t, committer.writeArchiveCatalog(
		context.Background(),
		nil,
		request,
	))
	require.Equal(t, 2, fake.offset)
}

func TestTxnLifecycleFinalCommitterWritesBindingFenceBeforeTTLReceipt(t *testing.T) {
	fake := &lifecycleCommitSQLExecutor{
		t: t,
		expect: []string{
			"update mo_catalog.mo_lifecycle_bindings",
			"insert into mo_catalog.mo_lifecycle_ttl_receipts",
		},
	}
	committer := TxnLifecycleFinalCommitter{SQLExecutor: fake}
	now := time.Now().UTC()
	request := LifecycleFinalizeRequest{
		Binding: lifecyclepkg.Binding{
			ID:              "00112233-4455-6677-8899-aabbccddeeff",
			AccountID:       17,
			DatabaseID:      41,
			LogicalTableID:  42,
			PhysicalTableID: 43,
			Generation:      7,
			Version:         11,
			SchemaDigest:    strings.Repeat("ab", 32),
		},
		FinalTxnID:     "final-txn",
		Cutoff:         now.Add(-90 * 24 * time.Hour),
		EvaluationTime: now,
		ExpiredRows:    100,
		RetiredBytes:   2048,
		Control: &api.LifecycleCommitEntry{
			ReceiptId:        "cf177f35-7f6a-4aac-8ee3-f3d5f04cf147",
			SourceSnapshotTs: ptrTimestamp(types.BuildTS(100, 2).ToTimestamp()),
			SourceSetDigest:  []byte(strings.Repeat("x", 32)),
		},
	}
	require.NoError(t, committer.writeTTLCatalog(
		context.Background(),
		nil,
		request,
	))
	require.Equal(t, 2, fake.offset)
}

func TestTxnLifecycleFinalCommitterLocksMoTablesDDLRow(t *testing.T) {
	fake := &lifecycleCommitSQLExecutor{
		t:      t,
		expect: []string{"select rel_id from mo_catalog.mo_tables"},
	}
	committer := TxnLifecycleFinalCommitter{SQLExecutor: fake}
	require.NoError(t, committer.lockLifecycleTableDDL(
		context.Background(),
		nil,
		lifecyclepkg.Binding{
			AccountID:       17,
			DatabaseID:      41,
			PhysicalTableID: 43,
		},
	))
	require.Equal(t, 1, fake.offset)
}

func TestTxnLifecycleFinalCommitterRollsBackSetupFailures(t *testing.T) {
	expected := errors.New("finalizer setup failed")
	tests := []struct {
		name           string
		newTxnErr      error
		engineNewErr   error
		lockErr        error
		getRelationErr error
		expectRollback bool
	}{
		{name: "create txn", newTxnErr: expected},
		{name: "open workspace", engineNewErr: expected, expectRollback: true},
		{name: "lock table fence", lockErr: expected, expectRollback: true},
		{name: "resolve relation", getRelationErr: expected, expectRollback: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			txnClient := mock_frontend.NewMockTxnClient(ctrl)
			operator := mock_frontend.NewMockTxnOperator(ctrl)
			request := lifecycleWholeTTLFinalizeRequest()
			commitEngine := &failingLifecycleCommitEngine{
				newErr:         test.engineNewErr,
				getRelationErr: test.getRelationErr,
			}
			sqlExecutor := &lifecycleCommitSQLExecutor{
				t:      t,
				expect: []string{"select rel_id from mo_catalog.mo_tables"},
				err:    test.lockErr,
			}
			txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(operator, test.newTxnErr)
			if test.expectRollback {
				operator.EXPECT().Rollback(gomock.Any()).Return(nil)
			}

			err := (TxnLifecycleFinalCommitter{
				Engine:      commitEngine,
				TxnClient:   txnClient,
				SQLExecutor: sqlExecutor,
			}).Finalize(context.Background(), request)
			require.ErrorIs(t, err, expected)
			if test.newTxnErr != nil || test.engineNewErr != nil {
				require.Zero(t, sqlExecutor.offset)
			} else {
				require.Equal(t, 1, sqlExecutor.offset)
			}
		})
	}
}

func TestValidateLifecycleFinalizeRequestFailsClosedOnIdentityDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*LifecycleFinalizeRequest)
	}{
		{name: "missing control", mutate: func(request *LifecycleFinalizeRequest) { request.Control = nil }},
		{name: "binding database", mutate: func(request *LifecycleFinalizeRequest) { request.Control.DatabaseId++ }},
		{name: "binding logical table", mutate: func(request *LifecycleFinalizeRequest) { request.Control.LogicalTableId++ }},
		{name: "binding physical table", mutate: func(request *LifecycleFinalizeRequest) { request.Control.PhysicalTableId++ }},
		{name: "binding generation", mutate: func(request *LifecycleFinalizeRequest) { request.Control.BindingGeneration++ }},
		{name: "ambiguous mode", mutate: func(request *LifecycleFinalizeRequest) { request.Control.DatasetId = "dataset" }},
		{name: "ttl contains archive manifest", mutate: func(request *LifecycleFinalizeRequest) { request.Manifest = &lifecyclepkg.ArchiveManifest{} }},
		{name: "rewrite root missing", mutate: func(request *LifecycleFinalizeRequest) { request.Control.RetireMode = api.LifecycleCommitEntry_Rewrite }},
		{name: "whole degraded root mismatch", mutate: func(request *LifecycleFinalizeRequest) {
			request.Root.RootID = "2d55f9be-4d3e-4ac7-a58a-1f7995d88f7f"
			request.Control.RootId = request.Root.RootID
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := lifecycleWholeTTLFinalizeRequest()
			test.mutate(&request)
			require.Error(t, validateLifecycleFinalizeRequest(request))
		})
	}
	require.NoError(t, validateLifecycleFinalizeRequest(
		lifecycleWholeTTLFinalizeRequest(),
	))
}

func TestTxnLifecycleFinalCommitterRejectsIncompleteModeBeforeTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	committer := TxnLifecycleFinalCommitter{
		Engine:      &failingLifecycleCommitEngine{},
		TxnClient:   txnClient,
		SQLExecutor: &lifecycleCommitSQLExecutor{t: t},
	}
	tests := []struct {
		name   string
		mutate func(*LifecycleFinalizeRequest)
	}{
		{name: "missing final txn", mutate: func(request *LifecycleFinalizeRequest) { request.FinalTxnID = "" }},
		{name: "missing protection", mutate: func(request *LifecycleFinalizeRequest) { request.SyncProtectionJobID = "" }},
		{name: "missing evaluation", mutate: func(request *LifecycleFinalizeRequest) { request.EvaluationTime = time.Time{} }},
		{name: "ambiguous mode", mutate: func(request *LifecycleFinalizeRequest) { request.Control.DatasetId = "dataset" }},
		{name: "ttl zero rows", mutate: func(request *LifecycleFinalizeRequest) { request.ExpiredRows = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := lifecycleWholeTTLFinalizeRequest()
			test.mutate(&request)
			require.Error(t, committer.Finalize(context.Background(), request))
		})
	}
}

func TestTxnLifecycleCatalogWritesFailClosedOnCASAndIdentity(t *testing.T) {
	request := lifecycleWholeTTLFinalizeRequest()
	request.Binding.ID = "not-a-uuid"
	committer := TxnLifecycleFinalCommitter{
		SQLExecutor: &lifecycleCommitSQLExecutor{t: t},
	}
	require.Error(t, committer.writeBindingFence(
		context.Background(),
		nil,
		request,
	))

	request = lifecycleWholeTTLFinalizeRequest()
	fake := &lifecycleCommitSQLExecutor{
		t:               t,
		expect:          []string{"update mo_catalog.mo_lifecycle_bindings"},
		useAffectedRows: true,
	}
	committer.SQLExecutor = fake
	require.Error(t, committer.writeBindingFence(
		context.Background(),
		nil,
		request,
	))
	require.Equal(t, 1, fake.offset)

	request.Control.ReceiptId = "not-a-uuid"
	fake = &lifecycleCommitSQLExecutor{
		t:      t,
		expect: []string{"update mo_catalog.mo_lifecycle_bindings"},
	}
	committer.SQLExecutor = fake
	require.Error(t, committer.writeTTLCatalog(
		context.Background(),
		nil,
		request,
	))
	require.Equal(t, 1, fake.offset)
}

func lifecycleWholeTTLFinalizeRequest() LifecycleFinalizeRequest {
	now := time.Now().UTC()
	return LifecycleFinalizeRequest{
		Binding: lifecyclepkg.Binding{
			ID:              "00112233-4455-6677-8899-aabbccddeeff",
			AccountID:       17,
			DatabaseID:      41,
			LogicalTableID:  42,
			PhysicalTableID: 43,
			Generation:      7,
		},
		Control: &api.LifecycleCommitEntry{
			DatabaseId:        41,
			LogicalTableId:    42,
			PhysicalTableId:   43,
			BindingGeneration: 7,
			ReceiptId:         "cf177f35-7f6a-4aac-8ee3-f3d5f04cf147",
			RetireMode:        api.LifecycleCommitEntry_Whole,
		},
		SyncProtectionJobID: "lifecycle/attempt/protection",
		FinalTxnID:          "final-txn",
		Cutoff:              now.Add(-24 * time.Hour),
		EvaluationTime:      now,
		ExpiredRows:         10,
	}
}

type failingLifecycleCommitEngine struct {
	newErr         error
	getRelationErr error
}

func (*failingLifecycleCommitEngine) LatestLogtailAppliedTime() timestamp.Timestamp {
	return timestamp.Timestamp{}
}

func (engine *failingLifecycleCommitEngine) New(
	context.Context,
	client.TxnOperator,
) error {
	return engine.newErr
}

func (engine *failingLifecycleCommitEngine) GetRelationById(
	context.Context,
	client.TxnOperator,
	uint64,
) (string, string, engine.Relation, error) {
	return "", "", nil, engine.getRelationErr
}

type lifecycleCommitSQLExecutor struct {
	t               *testing.T
	expect          []string
	offset          int
	err             error
	affectedRows    uint64
	useAffectedRows bool
}

func (fake *lifecycleCommitSQLExecutor) Exec(
	_ context.Context,
	sql string,
	options executor.Options,
) (executor.Result, error) {
	require.Less(fake.t, fake.offset, len(fake.expect))
	require.Contains(
		fake.t,
		strings.ToLower(sql),
		strings.ToLower(fake.expect[fake.offset]),
	)
	require.Equal(fake.t, uint32(17), options.AccountID())
	fake.offset++
	affectedRows := uint64(1)
	if fake.useAffectedRows {
		affectedRows = fake.affectedRows
	}
	return executor.Result{AffectedRows: affectedRows}, fake.err
}

func (*lifecycleCommitSQLExecutor) ExecTxn(
	context.Context,
	func(executor.TxnExecutor) error,
	executor.Options,
) error {
	panic("unexpected transaction")
}

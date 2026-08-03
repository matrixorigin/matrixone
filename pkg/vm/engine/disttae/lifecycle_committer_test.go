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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
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

type lifecycleCommitSQLExecutor struct {
	t      *testing.T
	expect []string
	offset int
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
	return executor.Result{AffectedRows: 1}, nil
}

func (*lifecycleCommitSQLExecutor) ExecTxn(
	context.Context,
	func(executor.TxnExecutor) error,
	executor.Options,
) error {
	panic("unexpected transaction")
}

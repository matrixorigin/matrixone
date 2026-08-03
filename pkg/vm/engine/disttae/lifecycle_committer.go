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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
)

type lifecycleTxnEngine interface {
	LatestLogtailAppliedTime() timestamp.Timestamp
	New(context.Context, client.TxnOperator) error
	GetRelationById(
		context.Context,
		client.TxnOperator,
		uint64,
	) (string, string, engine.Relation, error)
}

type lifecycleCommitStoreProvider interface {
	LifecycleCommitStore() (DNStore, error)
}

var _ lifecycleCommitStoreProvider = (*txnTableDelegate)(nil)

// TxnLifecycleFinalCommitter uses one private ordinary MO transaction. The
// only Lifecycle-specific operation is the thin retire control appended after
// the Dataset and Binding CAS writes.
type TxnLifecycleFinalCommitter struct {
	Engine      lifecycleTxnEngine
	TxnClient   client.TxnClient
	SQLExecutor executor.SQLExecutor
}

func (committer TxnLifecycleFinalCommitter) Finalize(
	ctx context.Context,
	request LifecycleFinalizeRequest,
) (err error) {
	if committer.Engine == nil ||
		committer.TxnClient == nil ||
		committer.SQLExecutor == nil ||
		request.Control == nil ||
		request.SyncProtectionJobID == "" ||
		request.FinalTxnID == "" ||
		request.Cutoff.IsZero() ||
		request.EvaluationTime.IsZero() {
		return fmt.Errorf("Lifecycle final committer input is incomplete")
	}
	archive := request.Control.DatasetId != ""
	ttl := request.Control.ReceiptId != ""
	if archive == ttl ||
		(archive && (request.Manifest == nil ||
			request.PurgeEligibleAt.IsZero())) ||
		(ttl && (request.Manifest != nil || request.ExpiredRows == 0)) {
		return fmt.Errorf("Lifecycle final committer mode is incomplete")
	}
	if err := validateLifecycleFinalizeRequest(request); err != nil {
		return err
	}
	ctx = defines.AttachAccount(
		ctx,
		request.Binding.AccountID,
		catalog.System_User,
		catalog.System_Role,
	)
	operator, err := committer.TxnClient.New(
		ctx,
		committer.Engine.LatestLogtailAppliedTime(),
		client.WithTxnCreateBy(
			request.Binding.AccountID,
			"",
			"tae object lifecycle finalizer",
			0,
		),
	)
	if err != nil {
		return err
	}
	ownedByFinalizer := false
	defer func() {
		if !ownedByFinalizer && err != nil {
			rollbackCtx, cancelRollback := lifecycleRollbackContext(ctx)
			defer cancelRollback()
			err = errors.Join(err, operator.Rollback(rollbackCtx))
		}
	}()
	if err = committer.Engine.New(ctx, operator); err != nil {
		return err
	}
	if err = committer.lockLifecycleTableDDL(
		ctx,
		operator,
		request.Binding,
	); err != nil {
		return err
	}
	_, _, relation, err := committer.Engine.GetRelationById(
		ctx,
		operator,
		request.Binding.PhysicalTableID,
	)
	if err != nil {
		return err
	}
	tableDef := relation.GetTableDef(ctx)
	currentSchemaDigest := lifecyclepkg.BindingSchemaDigest(tableDef)
	if tableDef == nil ||
		tableDef.TblId != request.Binding.PhysicalTableID ||
		!strings.EqualFold(
			hex.EncodeToString(currentSchemaDigest[:]),
			request.Binding.SchemaDigest,
		) {
		return fmt.Errorf("Lifecycle final table/schema fence changed")
	}
	storeProvider, ok := relation.(lifecycleCommitStoreProvider)
	if !ok {
		return fmt.Errorf(
			"table %d does not expose Lifecycle commit routing",
			request.Binding.PhysicalTableID,
		)
	}
	store, err := storeProvider.LifecycleCommitStore()
	if err != nil {
		return err
	}
	workspace, ok := operator.GetWorkspace().(*Transaction)
	if !ok || workspace == nil {
		return fmt.Errorf("Lifecycle finalizer does not own a disttae workspace")
	}
	workspace.SetSyncProtectionJobID(request.SyncProtectionJobID)
	ownedByFinalizer = true
	return FinalizeLifecycleCommit(
		ctx,
		operator,
		store,
		request.Control,
		func(
			writeCtx context.Context,
			writeOperator client.TxnOperator,
		) error {
			if request.Control.DatasetId != "" {
				return committer.writeArchiveCatalog(
					writeCtx,
					writeOperator,
					request,
				)
			}
			return committer.writeTTLCatalog(
				writeCtx,
				writeOperator,
				request,
			)
		},
	)
}

func validateLifecycleFinalizeRequest(request LifecycleFinalizeRequest) error {
	if request.Control == nil {
		return fmt.Errorf("Lifecycle final commit control is nil")
	}
	control := request.Control
	if control.DatabaseId != request.Binding.DatabaseID ||
		control.LogicalTableId != request.Binding.LogicalTableID ||
		control.PhysicalTableId != request.Binding.PhysicalTableID ||
		control.BindingGeneration != request.Binding.Generation {
		return fmt.Errorf("Lifecycle final Binding identity mismatch")
	}
	archive := control.DatasetId != ""
	ttl := control.ReceiptId != ""
	if archive == ttl {
		return fmt.Errorf("Lifecycle final mode identity mismatch")
	}
	if archive {
		if request.Root.State != lifecyclepkg.CleanupRootFinalizing ||
			(request.Root.Mode != lifecyclepkg.CleanupModeArchiveWhole &&
				request.Root.Mode != lifecyclepkg.CleanupModeArchiveRewrite) ||
			request.Manifest == nil ||
			request.Manifest.VerificationStatus != "FULL_READBACK_VERIFIED" {
			return fmt.Errorf(
				"Lifecycle Archive is not full-readback verified and FINALIZING",
			)
		}
		if request.Root.RootID != control.RootId ||
			request.Root.AttemptID != control.AttemptId ||
			request.Manifest.RootID != control.RootId ||
			request.Manifest.AttemptID != control.AttemptId {
			return fmt.Errorf("Lifecycle Archive Root/attempt identity mismatch")
		}
		if request.Root.ManifestKey != request.ManifestKey ||
			request.Root.ManifestDigest != request.ManifestDigest {
			return fmt.Errorf("Lifecycle Archive persisted Manifest identity mismatch")
		}
		_, digest, err := lifecyclepkg.MarshalArchiveManifest(request.Manifest)
		if err != nil {
			return err
		}
		if digest != request.ManifestDigest ||
			!strings.HasPrefix(
				request.ManifestKey,
				strings.TrimSuffix(request.Root.ArchivePrefix, "/")+"/",
			) ||
			!strings.HasSuffix(
				request.ManifestKey,
				"manifest-"+hex.EncodeToString(digest[:])+".json",
			) {
			return fmt.Errorf("Lifecycle Archive Manifest digest/key mismatch")
		}
		if !bytes.Equal(control.SchemaDigest, request.Manifest.SchemaDigest[:]) ||
			!bytes.Equal(control.SourceSetDigest, request.Root.SourceSetDigest[:]) {
			return fmt.Errorf("Lifecycle Archive source/schema identity mismatch")
		}
		return nil
	}

	if request.Manifest != nil || request.ManifestKey != "" ||
		request.ManifestDigest != ([32]byte{}) {
		return fmt.Errorf("Lifecycle TTL finalization contains Archive state")
	}
	if control.RetireMode == api.LifecycleCommitEntry_Rewrite {
		if request.Root.State != lifecyclepkg.CleanupRootFinalizing ||
			request.Root.Mode != lifecyclepkg.CleanupModeTTLRewrite ||
			request.Root.RootID != control.RootId ||
			request.Root.AttemptID != control.AttemptId ||
			!bytes.Equal(control.SourceSetDigest, request.Root.SourceSetDigest[:]) {
			return fmt.Errorf("Lifecycle TTL Rewrite Root identity mismatch")
		}
	} else if request.Root.RootID != "" || control.RootId != "" {
		// A Mixed Rewrite can discover that every visible row is expired only
		// after its Root-owned output namespace has been established. It then
		// retires the exact source as Whole, while the existing Root still owns
		// the unused segment/booking namespace until commit is known. Direct
		// Whole TTL has neither Root nor external side effects.
		if request.Root.State != lifecyclepkg.CleanupRootFinalizing ||
			request.Root.Mode != lifecyclepkg.CleanupModeTTLRewrite ||
			request.Root.RootID != control.RootId ||
			request.Root.AttemptID != control.AttemptId ||
			!bytes.Equal(control.SourceSetDigest, request.Root.SourceSetDigest[:]) {
			return fmt.Errorf(
				"Lifecycle Whole-degraded TTL Root identity mismatch",
			)
		}
	}
	return nil
}

func (committer TxnLifecycleFinalCommitter) lockLifecycleTableDDL(
	ctx context.Context,
	operator client.TxnOperator,
	binding lifecyclepkg.Binding,
) error {
	result, err := committer.SQLExecutor.Exec(
		ctx,
		fmt.Sprintf(
			`select rel_id from mo_catalog.mo_tables
where rel_id=%d and reldatabase_id=%d for update`,
			binding.PhysicalTableID,
			binding.DatabaseID,
		),
		executor.Options{}.
			WithTxn(operator).
			WithAccountID(binding.AccountID),
	)
	if err != nil {
		return err
	}
	result.Close()
	return nil
}

func (committer TxnLifecycleFinalCommitter) writeArchiveCatalog(
	ctx context.Context,
	operator client.TxnOperator,
	request LifecycleFinalizeRequest,
) error {
	if err := committer.writeBindingFence(ctx, operator, request); err != nil {
		return err
	}
	return committer.insertArchiveDataset(ctx, operator, request)
}

func (committer TxnLifecycleFinalCommitter) writeBindingFence(
	ctx context.Context,
	operator client.TxnOperator,
	request LifecycleFinalizeRequest,
) error {
	bindingID, err := lifecycleCatalogUUID(request.Binding.ID)
	if err != nil {
		return err
	}
	result, err := committer.SQLExecutor.Exec(
		ctx,
		fmt.Sprintf(
			`update mo_catalog.mo_lifecycle_bindings
set version=version+1,updated_at=utc_timestamp()
where binding_id=unhex('%s') and account_id=%d and physical_table_id=%d
and binding_generation=%d and schema_digest=unhex('%s') and version=%d
and state='ACTIVE'`,
			bindingID,
			request.Binding.AccountID,
			request.Binding.PhysicalTableID,
			request.Binding.Generation,
			request.Binding.SchemaDigest,
			request.Binding.Version,
		),
		executor.Options{}.
			WithTxn(operator).
			WithAccountID(request.Binding.AccountID),
	)
	if err != nil {
		return err
	}
	affected := result.AffectedRows
	result.Close()
	if affected != 1 {
		return fmt.Errorf("Lifecycle Binding final fence CAS failed")
	}
	return nil
}

func (committer TxnLifecycleFinalCommitter) insertArchiveDataset(
	ctx context.Context,
	operator client.TxnOperator,
	request LifecycleFinalizeRequest,
) error {
	datasetID, err := lifecycleCatalogUUID(request.Control.DatasetId)
	if err != nil {
		return err
	}
	bindingID, err := lifecycleCatalogUUID(request.Binding.ID)
	if err != nil {
		return err
	}
	rootID, err := lifecycleCatalogUUID(request.Root.RootID)
	if err != nil {
		return err
	}
	attemptID, err := lifecycleCatalogUUID(request.Root.AttemptID)
	if err != nil {
		return err
	}
	sourceSnapshot := types.TimestampToTS(*request.Control.SourceSnapshotTs)
	result, err := committer.SQLExecutor.Exec(
		ctx,
		fmt.Sprintf(
			`insert into mo_catalog.mo_lifecycle_datasets(
dataset_id,account_id,binding_id,binding_generation,logical_table_id,
source_physical_table_id,source_snapshot_ts,evaluation_time,cutoff,
source_set_digest,schema_descriptor_digest,lifecycle_min,lifecycle_max,
root_id,attempt_id,manifest_key,manifest_sha256,content_hash,row_count,
logical_bytes,stage_id,stage_identity_blob,purge_eligible_at,state,version,
access_generation,restore_lease_id,restore_deadline,publish_txn_id,
created_at,updated_at)
values(unhex('%s'),%d,unhex('%s'),%d,%d,%d,unhex('%s'),%s,%s,
unhex('%s'),unhex('%s'),null,null,unhex('%s'),unhex('%s'),%s,unhex('%s'),
unhex('%s'),%d,%d,%d,%s,%s,'PUBLISHED',1,1,null,null,%s,
utc_timestamp(),utc_timestamp())`,
			datasetID,
			request.Binding.AccountID,
			bindingID,
			request.Binding.Generation,
			request.Binding.LogicalTableID,
			request.Binding.PhysicalTableID,
			hex.EncodeToString(sourceSnapshot[:]),
			lifecycleCatalogTime(request.EvaluationTime),
			lifecycleCatalogTime(request.Cutoff),
			hex.EncodeToString(request.Root.SourceSetDigest[:]),
			hex.EncodeToString(request.Manifest.SchemaDigest[:]),
			rootID,
			attemptID,
			lifecycleCatalogQuote(request.ManifestKey),
			hex.EncodeToString(request.ManifestDigest[:]),
			hex.EncodeToString(request.Manifest.ContentHash[:]),
			request.Manifest.RowCount,
			request.Manifest.LogicalBytes,
			request.Binding.StageID,
			lifecycleCatalogQuote(request.Root.ArchiveNamespace),
			lifecycleCatalogQuote(
				request.PurgeEligibleAt.UTC().Format("2006-01-02 15:04:05.999999"),
			),
			lifecycleCatalogQuote(request.FinalTxnID),
		),
		executor.Options{}.
			WithTxn(operator).
			WithAccountID(request.Binding.AccountID),
	)
	if err != nil {
		return err
	}
	defer result.Close()
	if result.AffectedRows != 1 {
		return fmt.Errorf(
			"Lifecycle Dataset insert affected %d rows",
			result.AffectedRows,
		)
	}
	return nil
}

func (committer TxnLifecycleFinalCommitter) writeTTLCatalog(
	ctx context.Context,
	operator client.TxnOperator,
	request LifecycleFinalizeRequest,
) error {
	if err := committer.writeBindingFence(ctx, operator, request); err != nil {
		return err
	}
	receiptID, err := lifecycleCatalogUUID(request.Control.ReceiptId)
	if err != nil {
		return err
	}
	bindingID, err := lifecycleCatalogUUID(request.Binding.ID)
	if err != nil {
		return err
	}
	rootID, err := lifecycleNullableCatalogUUID(request.Root.RootID)
	if err != nil {
		return err
	}
	attemptID := "null"
	if request.Root.RootID != "" {
		attemptID, err = lifecycleNullableCatalogUUID(request.Root.AttemptID)
		if err != nil {
			return err
		}
	}
	if request.Control.SourceSnapshotTs == nil ||
		len(request.Control.SourceSetDigest) != 32 {
		return fmt.Errorf("Lifecycle TTL Receipt source identity is incomplete")
	}
	sourceSnapshot := types.TimestampToTS(*request.Control.SourceSnapshotTs)
	result, err := committer.SQLExecutor.Exec(
		ctx,
		fmt.Sprintf(
			`insert into mo_catalog.mo_lifecycle_ttl_receipts(
receipt_id,account_id,binding_id,binding_generation,physical_table_id,
source_snapshot_ts,evaluation_time,cutoff,source_set_digest,expired_rows,
retired_bytes,root_id,attempt_id,publish_txn_id,created_at)
values(unhex('%s'),%d,unhex('%s'),%d,%d,unhex('%s'),%s,%s,unhex('%s'),
%d,%d,%s,%s,%s,utc_timestamp())`,
			receiptID,
			request.Binding.AccountID,
			bindingID,
			request.Binding.Generation,
			request.Binding.PhysicalTableID,
			hex.EncodeToString(sourceSnapshot[:]),
			lifecycleCatalogTime(request.EvaluationTime),
			lifecycleCatalogTime(request.Cutoff),
			hex.EncodeToString(request.Control.SourceSetDigest),
			request.ExpiredRows,
			request.RetiredBytes,
			rootID,
			attemptID,
			lifecycleCatalogQuote(request.FinalTxnID),
		),
		executor.Options{}.
			WithTxn(operator).
			WithAccountID(request.Binding.AccountID),
	)
	if err != nil {
		return err
	}
	defer result.Close()
	if result.AffectedRows != 1 {
		return fmt.Errorf(
			"Lifecycle TTL Receipt insert affected %d rows",
			result.AffectedRows,
		)
	}
	return nil
}

func (tbl *txnTable) LifecycleCommitStore() (DNStore, error) {
	transaction := tbl.getTxn()
	if transaction == nil || len(transaction.tnStores) == 0 {
		return DNStore{}, fmt.Errorf("Lifecycle table has no TN route")
	}
	return transaction.tnStores[0], nil
}

func (tbl *txnTableDelegate) LifecycleCommitStore() (DNStore, error) {
	return tbl.origin.LifecycleCommitStore()
}

func lifecycleCatalogUUID(value string) (string, error) {
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", fmt.Errorf("invalid Lifecycle Catalog UUID %q: %w", value, err)
	}
	return hex.EncodeToString(parsed[:]), nil
}

func lifecycleNullableCatalogUUID(value string) (string, error) {
	if value == "" {
		return "null", nil
	}
	encoded, err := lifecycleCatalogUUID(value)
	if err != nil {
		return "", err
	}
	return "unhex('" + encoded + "')", nil
}

func lifecycleCatalogQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func lifecycleCatalogTime(value time.Time) string {
	return lifecycleCatalogQuote(
		value.UTC().Truncate(time.Microsecond).
			Format("2006-01-02 15:04:05.999999"),
	)
}

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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
)

// LifecycleObjectReader is the narrow engine capability used by the
// Lifecycle worker. The concrete txnTable stays private to disttae.
type LifecycleObjectReader interface {
	LifecycleReadObject(
		context.Context,
		types.TS,
		objectio.ObjectStats,
		uint64,
		lifecyclepkg.ExactBlockConsumer,
	) (lifecyclepkg.ObjectScanReport, error)
}

func (tbl *txnTable) LifecycleReadObject(
	ctx context.Context,
	snapshot types.TS,
	source objectio.ObjectStats,
	maxCertifiedBlockReadBytes uint64,
	consume lifecyclepkg.ExactBlockConsumer,
) (report lifecyclepkg.ObjectScanReport, err error) {
	if snapshot.IsEmpty() ||
		source.IsZero() ||
		source.GetAppendable() ||
		maxCertifiedBlockReadBytes == 0 ||
		consume == nil {
		return report, moerr.NewInvalidInput(ctx, "Lifecycle exact Object reader input is incomplete")
	}
	report = lifecyclepkg.NewObjectScanReport(
		source.BlkCnt(),
		uint64(source.Rows()),
	)
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return report, err
	}
	current, exists := state.GetObject(*source.ObjectShortName())
	if !exists ||
		(!current.DeleteTime.IsEmpty() && current.DeleteTime.LE(&snapshot)) ||
		current.ObjectStats != source {
		return report, moerr.NewTxnWWConflictNoCtx(
			tbl.tableId,
			"Lifecycle reader source Object identity changed",
		)
	}
	tbl.ensureSeqnumsAndTypesExpectRowid()
	sortKeyPos, sortKeyIsPK := tbl.getSortKeyPosAndSortKeyIsPK()
	host, err := newCNMergeTask(
		ctx,
		tbl,
		snapshot,
		sortKeyPos,
		sortKeyIsPK,
		[]objectio.ObjectStats{source},
		0,
	)
	if err != nil {
		return report, err
	}
	defer host.Release()
	if err := host.configureLifecycleBlockReadBudget(
		ctx,
		maxCertifiedBlockReadBytes,
	); err != nil {
		return report, err
	}
	for {
		value, deleted, release, err := host.LoadNextBatch(ctx, 0, nil)
		if errors.Is(err, mergesort.ErrNoMoreBlocks) {
			if validateErr := report.ValidatePhysicalComplete(); validateErr != nil {
				return report, validateErr
			}
			return report, nil
		}
		if err != nil {
			return report, err
		}
		if value == nil || release == nil {
			if release != nil {
				release()
			}
			return report, moerr.NewInternalError(
				ctx,
				"Lifecycle exact Object reader returned incomplete ownership",
			)
		}
		if err := report.ObservePhysicalBlock(value.RowCount(), deleted); err != nil {
			release()
			return report, err
		}
		err = func() error {
			defer release()
			return consume(value, deleted)
		}()
		if err != nil {
			return report, err
		}
	}
}

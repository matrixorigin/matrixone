// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"slices"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// rebaseDeltaKeyState describes a single logical key after both sides have
// been reduced to their final visible states. Equality must include presence
// and use the admitted SQL comparison semantics, not Go value equality.
type rebaseDeltaKeyState struct {
	featureChanged, upstreamChanged bool
	headsEqual                      bool
	featurePresent, upstreamPresent bool
}

type rebaseDeltaDecision struct {
	counts [6]uint64
	remove bool
	insert bool
}

// decideRebaseDeltaKey shares the logical accounting and physical action
// decision. The caller must finish all FAIL-policy conflict checks before
// consuming actions; provisional decisions do not authorize partial writes.
// policy is validated by the coordinator before delta extraction begins.
func decideRebaseDeltaKey(state rebaseDeltaKeyState, policy rebaseConflictPolicy) rebaseDeltaDecision {
	var decision rebaseDeltaDecision
	if state.featureChanged {
		decision.counts[0] = 1
	}
	if state.upstreamChanged {
		decision.counts[1] = 1
	}
	conflict := state.featureChanged && state.upstreamChanged && !state.headsEqual
	if conflict {
		decision.counts[2] = 1
	}
	if !state.featureChanged || state.headsEqual || (conflict && policy != rebaseKeepBranch) {
		return decision
	}
	decision.remove = state.upstreamPresent
	decision.insert = state.featurePresent
	switch {
	case state.featurePresent && !state.upstreamPresent:
		decision.counts[3] = 1
	case state.featurePresent && state.upstreamPresent:
		decision.counts[4] = 1
	case !state.featurePresent && state.upstreamPresent:
		decision.counts[5] = 1
	}
	return decision
}

// collectRebaseChangedKeyBatches borrows an admitted transaction and protected
// endpoints. DIFF output is only a candidate-key source: callers must normalize
// the fixed endpoint states before counting or applying changes. In particular,
// neither physical event ordering nor merge conflict annotations are Rebase
// decisions. consume must copy any retained keys before returning.
func collectRebaseChangedKeyBatches(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	table string,
	base, head rebaseSQLRead,
	allocator *branchHashmapAllocator,
	consume func([]*vector.Vector) error,
) error {
	if table == "" || base.database == "" || head.database == "" ||
		base.timestamp <= 0 || head.timestamp <= 0 || allocator == nil || consume == nil {
		return moerr.NewInternalError(ctx, "invalid Rebase delta source")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	endpoint := func(read rebaseSQLRead) tree.TableName {
		return *tree.NewTableName(tree.Identifier(table), tree.ObjectNamePrefix{
			SchemaName: tree.Identifier(read.database), ExplicitSchema: true,
		}, newMoTimestampHint(read.timestamp))
	}
	tables, err := getTableStuff(ctx, ses, bh, endpoint(head), endpoint(base))
	if err != nil {
		return err
	}
	// All maps, including changes-handle fallback storage, charge the same
	// statement allocator rather than acquiring a budget for each endpoint.
	tables.hashmapAllocator = allocator
	tables.strictHashmapCapacity = true
	tables.rebaseKeyConsumer = consume
	defer tables.retPool.freeAllRetBatches(ses.proc.Mp())
	if err = prepareDataBranchWorker(nil, nil, &tables); err != nil {
		return err
	}
	defer tables.worker.Release()
	dag, err := decideLCABranchTSFromBranchDAG(ctx, ses, bh, tables)
	if err != nil {
		return err
	}
	if err = reconcileDataBranchEndpointSchema(ctx, ses, bh, &tables, &dag); err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	// Only change collection runs; Rebase normalizes candidate keys itself.
	options := compositeOption{onlineApply: true, outputSQL: true, expandUpdate: true}
	return diffOnBase(ctx, ses, bh, &wg, dag, tables, options,
		func(wrapped batchWithKind) (bool, error) {
			return false, moerr.NewInternalError(ctx, "Rebase candidate-key mode was not preserved")
		}, nil, nil, nil, nil)
}

// Reads every collected data/tombstone key before DIFF's value equality can
// collapse stored values such as signed zeros. False positives are safe: the
// fixed-state SQL probe removes them before accounting or writing.
func collectRebaseHashmapKeys(ctx context.Context, ses *Session, table tableStuff,
	base, baseDeletes, head, headDeletes databranchutils.BranchHashmap) error {
	indexes := slices.Clone(table.def.pkColIdxes)
	slices.Sort(indexes)
	resources := dataBranchVisibleStateRecoveryResources{allocator: table.hashmapAllocator}
	if err := resources.ReserveBuffer(rebaseDeltaKeyScratchBytes); err != nil {
		return err
	}
	defer resources.ReleaseBuffer(rebaseDeltaKeyScratchBytes)
	output := batch.NewWithSize(len(indexes))
	for i, index := range indexes {
		output.Vecs[i] = vector.NewVec(table.def.colTypes[index])
	}
	defer output.Clean(ses.proc.Mp())
	flush := func() error {
		if output.RowCount() == 0 {
			return nil
		}
		if err := table.rebaseKeyConsumer(output.Vecs); err != nil {
			return err
		}
		output.CleanOnlyData()
		return nil
	}
	for _, source := range []struct {
		store databranchutils.BranchHashmap
		tomb  bool
	}{{base, false}, {baseDeletes, true}, {head, false}, {headDeletes, true}} {
		if source.store == nil {
			continue
		}
		err := source.store.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
			return cursor.ForEach(func(_, row []byte) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				tuple, _, err := source.store.DecodeRow(row)
				if err != nil {
					return err
				}
				var tombKeys types.Tuple
				if source.tomb {
					if len(tuple) < 2 {
						return moerr.NewInternalError(ctx, "invalid Rebase delta tombstone")
					}
					if table.def.pkKind == compositeKind {
						encoded, ok := tuple[1].([]byte)
						if !ok {
							return moerr.NewInternalError(ctx, "invalid Rebase composite tombstone key")
						}
						tombKeys, err = types.Unpack(encoded)
						if err != nil {
							return err
						}
					} else {
						tombKeys = types.Tuple{tuple[1]}
					}
					if len(tombKeys) != len(indexes) {
						return moerr.NewInternalError(ctx, "Rebase tombstone key width differs")
					}
				}
				for i, index := range indexes {
					var value any
					if source.tomb {
						value = tombKeys[slices.Index(table.def.pkColIdxes, index)]
					} else {
						value, err = getTupleColumnValue(tuple, table, index)
						if err != nil {
							return err
						}
					}
					if value == nil {
						return moerr.NewInternalError(ctx, "NULL Rebase delta key")
					}
					if err = appendTupleValueToVector(output.Vecs[i], value, ses.proc.Mp()); err != nil {
						return err
					}
				}
				output.SetRowCount(output.Vecs[0].Length())
				if output.RowCount() == rebaseDeltaKeyBatch {
					return flush()
				}
				return nil
			})
		}, 1)
		if err != nil {
			return err
		}
	}
	return flush()
}

func rebaseDeltaBatchKeys(ctx context.Context, wrapped batchWithKind, indexes []int) ([]*vector.Vector, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if wrapped.batch == nil || len(indexes) == 0 {
		return nil, moerr.NewInternalError(ctx, "invalid Rebase delta batch")
	}
	keys := make([]*vector.Vector, len(indexes))
	for i, index := range indexes {
		if index < 0 || index >= len(wrapped.batch.Vecs) || wrapped.batch.Vecs[index] == nil {
			return nil, moerr.NewInternalError(ctx, "invalid Rebase delta key layout")
		}
		keys[i] = wrapped.batch.Vecs[index]
	}
	return keys, nil
}

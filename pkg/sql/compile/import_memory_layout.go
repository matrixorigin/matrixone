// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	MemoryStageCSV      = "csv"
	MemoryStageOutput   = "output"
	MemoryStageJoin     = "join"
	MemoryStageProducer = "producer"
)

// MemoryManifest describes the complete eligible local graph before allocation.
type MemoryManifest struct {
	Producers  []MemoryProducerDescriptor
	TargetUnit int64
	Parallel   int
	// NoWait comes from the transaction options.
	// It forbids admission FIFO/full-reclaim waiting, not ordinary sampling,
	// short mutex acquisition or the existing best-effort RSS scavenger.
	NoWait bool
	Stages []MemoryStageDescriptor
}

type MemoryStageDescriptor struct {
	Producer        uint32
	Role            string
	Count           uint32
	Schema          []types.Type
	Projections     [][]types.Type
	RetainedFields  int
	RecordBytes     int64
	TableCount      int
	InsertOnly      bool
	LeftTypes       []types.Type
	RightTypes      []types.Type
	NonEqExpression *plan.Expr
	KeyExpressions  [][]*plan.Expr
	ResultCols      []colexec.ResultPos
}

// A legal expression shape can exceed vector allocation capacity at a large
// batch unit, while a smaller unit has a finite envelope.
type memoryEnvelopeCapacityError struct{ message string }

func (e *memoryEnvelopeCapacityError) Error() string { return e.message }

// memoryManifestLayout computes the complete promise before readers, execution
// locks or operator Prepare allocate their working storage.
func memoryManifestLayout(manifest MemoryManifest, unit int64) (*process.ImportMemory, rscthrottler.WorkingSetSpec, bool, error) {
	meta := &process.ImportMemory{UnitBytes: unit}
	spec := rscthrottler.WorkingSetSpec{UnitBytes: unit, Inputs: []rscthrottler.InputSlotSpec{{Name: "csv"}, {Name: "output"}, {Name: "join-input"}}}
	var projections [][]types.Type
	var maxRow int64
	var producerBounds []memoryProducerBound
	if len(manifest.Producers) > 0 {
		var err error
		producerBounds, err = memoryProducerBounds(manifest.Producers, unit)
		if err != nil {
			return nil, spec, false, err
		}
		for _, bound := range producerBounds {
			// Encoded writer frames are consumed only by metadata flushers, not
			// by raw-data writers/joins. Their byte bound comes from the final
			// output promise below; feeding it back into the raw-row floor would
			// introduce a false circular envelope.
			if !bound.Metadata {
				maxRow = max(maxRow, bound.Row)
			}
		}
	}
	for _, stage := range manifest.Stages {
		if stage.Count == 0 {
			continue
		}
		schemas := stage.Projections
		if len(stage.Schema) > 0 {
			schemas = append(append([][]types.Type(nil), schemas...), stage.Schema)
		}
		if stage.Role == MemoryStageJoin {
			schemas = append(append([][]types.Type(nil), schemas...), stage.LeftTypes, stage.RightTypes)
		}
		projections = append(projections, schemas...)
		// A complete producer graph already bounds every physical row,
		// including expression expansion. Schema-only manifests need the
		// conservative fallback; reapplying it to proven inputs loses their
		// smaller record bound.
		if len(producerBounds) == 0 {
			for _, schema := range schemas {
				row, err := external.MaxRowBytes(schema, stage.RecordBytes)
				if err != nil {
					return nil, spec, false, err
				}
				maxRow = max(maxRow, row)
			}
		}
	}
	controlled := false
	var producerStages []MemoryStageDescriptor
	for _, stage := range manifest.Stages {
		if stage.Count == 0 {
			continue
		}
		controlled = true
		switch stage.Role {
		case MemoryStageCSV:
			capacity, err := external.SourceEnvelopeForFields(stage.Schema, unit, stage.RecordBytes, stage.RetainedFields)
			if err != nil {
				return nil, spec, false, err
			}
			meta.SourceBytes = max(meta.SourceBytes, capacity)
			if err = addMemorySlots(&spec.Inputs[0].Count, int(stage.Count), 1); err != nil {
				return nil, spec, false, err
			}
		case MemoryStageOutput:
			var capacity int64
			var err error
			if stage.InsertOnly {
				capacity, err = colexec.InsertMetadataOutputEnvelope(unit, max(1, stage.TableCount))
			} else {
				capacity, err = multi_update.OutputEnvelope(unit, maxRow, max(1, stage.TableCount))
			}
			if err != nil {
				return nil, spec, false, err
			}
			meta.OutputBytes = max(meta.OutputBytes, capacity)
			if err = addMemorySlots(&spec.Inputs[1].Count, int(stage.Count), 1); err != nil {
				return nil, spec, false, err
			}
		case MemoryStageJoin:
			join := hashjoin.HashJoin{LeftTypes: stage.LeftTypes, RightTypes: stage.RightTypes, ResultCols: stage.ResultCols, EqConds: stage.KeyExpressions, NonEqCond: stage.NonEqExpression}
			for _, col := range stage.ResultCols {
				if col.Rel < 0 || col.Rel > 1 || col.Pos < 0 || col.Rel == 0 && int(col.Pos) >= len(stage.LeftTypes) || col.Rel == 1 && int(col.Pos) >= len(stage.RightTypes) {
					return nil, spec, false, moerr.NewInternalErrorNoCtx("invalid join memory projection")
				}
			}
			capacity, ok := join.InputEnvelope(max(unit, maxRow))
			if !ok {
				return nil, spec, false, &memoryEnvelopeCapacityError{"join input working envelope overflows"}
			}
			meta.JoinInputBytes = max(meta.JoinInputBytes, capacity)
			capacity, ok = join.OutputEnvelope(unit, maxRow)
			if !ok {
				return nil, spec, false, &memoryEnvelopeCapacityError{"join output working envelope overflows"}
			}
			meta.OutputBytes = max(meta.OutputBytes, capacity)
			if err := addMemorySlots(&spec.Inputs[1].Count, int(stage.Count), 1); err != nil {
				return nil, spec, false, err
			}
			if err := addMemorySlots(&spec.Inputs[2].Count, int(stage.Count), 2); err != nil {
				return nil, spec, false, err
			}
		case MemoryStageProducer:
			if uint64(stage.Producer) >= uint64(len(producerBounds)) {
				return nil, spec, false, moerr.NewInternalErrorNoCtx("invalid producer stage index")
			}
			producerStages = append(producerStages, stage)
		default:
			return nil, spec, false, moerr.NewInternalErrorNoCtxf("unknown memory stage %q", stage.Role)
		}
	}
	if !controlled {
		return meta, spec, false, nil
	}
	var err error
	meta.IOBytes, err = ioutil.IOEnvelope(projections, unit, maxRow)
	if err != nil {
		return nil, spec, false, err
	}
	// Hash spill writes directly to an anonymous FD with MarshalBinaryTo;
	// object writer arenas are not live in that operation. These are mutually
	// exclusive users of the one process IO slot, so reserve their larger peak.
	meta.IOBytes = max(meta.IOBytes, meta.JoinInputBytes)
	spec.IOBytes = meta.IOBytes
	spec.Inputs[0].Bytes = meta.SourceBytes
	spec.Inputs[1].Bytes = meta.OutputBytes
	spec.Inputs[2].Bytes = meta.JoinInputBytes
	if len(producerBounds) > 0 && meta.OutputBytes > 0 {
		// Older manifests and raw-delete metadata retain the conservative
		// shared output limit. Explicit INSERT-only producers derive their own
		// unit-based protocol cap; a larger unrelated output owner must never
		// enlarge their frames or downstream spool/consumer promises.
		producerBounds, err = memoryProducerBoundsWithFrame(manifest.Producers, unit, meta.OutputBytes/4)
		if err != nil {
			return nil, spec, false, err
		}
	}
	channelCapacity, valid := memoryPipelineChannelCapacity(manifest)
	if !valid {
		return nil, spec, false, moerr.NewInternalErrorNoCtx("pipeline metadata capacity overflows")
	}
	edgeMetadata, valid := process.PipelineEdgeMetadataBound(channelCapacity)
	if !valid {
		return nil, spec, false, moerr.NewInternalErrorNoCtx("pipeline edge metadata overflows")
	}
	for _, stage := range producerStages {
		capacity := producerBounds[stage.Producer].Scratch
		if producer := manifest.Producers[stage.Producer]; producer.Receivers > 0 {
			edges, ok := memoryProduct(edgeMetadata, int64(producer.Receivers))
			if !ok {
				return nil, spec, false, moerr.NewInternalErrorNoCtx("pipeline receiver metadata overflows")
			}
			capacity, ok = memorySum(capacity, edges)
			if !ok {
				return nil, spec, false, moerr.NewInternalErrorNoCtx("pipeline metadata promise overflows")
			}
		}
		if capacity > 0 {
			spec.Inputs = append(spec.Inputs, rscthrottler.InputSlotSpec{Name: fmt.Sprintf("producer-%d", stage.Producer), Count: stage.Count, Bytes: capacity})
		}
	}
	return meta, spec, true, nil
}

// memoryManifestCandidates owns all precomputed envelopes before a request
// enters FIFO. Equal total bytes are insufficient: shared W makes I/W layout
// significant, so only identical complete input and workspace promises merge.
func sameMemoryWorkingSetEnvelope(left, right rscthrottler.WorkingSetSpec) bool {
	return left.IOBytes == right.IOBytes && slices.Equal(left.Inputs, right.Inputs)
}

func memoryManifestCandidates(manifest MemoryManifest, target int64) ([]rscthrottler.WorkingSetSpec, map[int64]*process.ImportMemory, error) {
	var specs []rscthrottler.WorkingSetSpec
	metas := make(map[int64]*process.ImportMemory)
	var capacityErr error
	for unit := target; ; unit = max(1, unit/2) {
		// A row inside this batch must already fit its complete reservation.
		// Scale the record proof with smaller candidates instead of admitting a
		// large batch with only one read block and growing it under pressure.
		recordBytes := min(max(unit, int64(csvparser.ReadBlockSize)), int64(config.LargestEntryLimit))
		meta, spec, controlled, err := memoryManifestLayout(memoryRecordManifest(manifest, recordBytes), unit)
		if err != nil {
			var capacity *memoryEnvelopeCapacityError
			if !errors.As(err, &capacity) {
				return nil, nil, err
			}
			capacityErr = err
		} else if !controlled {
			return nil, nil, nil
		} else {
			equivalent := false
			for _, previous := range specs {
				if sameMemoryWorkingSetEnvelope(previous, spec) {
					equivalent = true
					break
				}
			}
			if !equivalent {
				meta.RecordBytes = recordBytes
				specs = append(specs, spec)
				metas[unit] = meta
			}
		}
		if unit == 1 {
			break
		}
	}
	if len(specs) == 0 {
		return nil, nil, capacityErr
	}
	return specs, metas, nil
}

// memoryRecordManifest tightens only the raw-record proof. Topology, counts and
// batch units remain unchanged, and the source manifest stays immutable.
func memoryRecordManifest(manifest MemoryManifest, bytes int64) MemoryManifest {
	bound := func(limit int64) int64 {
		if limit <= 0 {
			limit = int64(config.LargestEntryLimit)
		}
		return min(bytes, limit)
	}
	manifest.Stages = slices.Clone(manifest.Stages)
	for i := range manifest.Stages {
		manifest.Stages[i].RecordBytes = bound(manifest.Stages[i].RecordBytes)
	}
	manifest.Producers = slices.Clone(manifest.Producers)
	for i := range manifest.Producers {
		if manifest.Producers[i].Kind == "csv" {
			manifest.Producers[i].RecordBytes = bound(manifest.Producers[i].RecordBytes)
		}
	}
	return manifest
}

// Freeze the finite growth choices before execution mutates operator trees.
// Runtime retains specifications only; ordinary rows never enter the owner.
func bindMemoryRecordGrowth(meta *process.ImportMemory, manifest MemoryManifest) {
	type step struct {
		bytes int64
		spec  rscthrottler.WorkingSetSpec
		err   error
	}
	limit := int64(config.LargestEntryLimit)
	var steps []step
	for bytes := meta.RecordBytes; bytes < limit; {
		bytes += min(bytes, limit-bytes)
		_, spec, _, err := memoryManifestLayout(memoryRecordManifest(manifest, bytes), meta.UnitBytes)
		steps = append(steps, step{bytes, spec, err})
	}
	meta.GrowRecord = func(ctx context.Context, need int64) (int64, error) {
		for _, step := range steps {
			if need <= step.bytes {
				if step.err != nil {
					return 0, step.err
				}
				return step.bytes, meta.WorkingSet.TryGrow(ctx, step.spec)
			}
		}
		return 0, moerr.NewInternalErrorNoCtx("CSV record exceeds the configured entry limit")
	}
}

func admitMemoryManifest(ctx context.Context, throttler rscthrottler.WorkingSetThrottler, manifest MemoryManifest) (*process.ImportMemory, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	target := min(manifest.TargetUnit, rscthrottler.BatchBytes(throttler))
	if target <= 0 {
		return nil, moerr.NewInternalErrorNoCtx("no legal import batch unit")
	}
	if config.LargestEntryLimit <= 0 {
		return nil, moerr.NewInternalErrorNoCtx("no legal CSV record budget")
	}
	specs, metas, err := memoryManifestCandidates(manifest, target)
	if err != nil || len(specs) == 0 {
		return nil, err
	}
	if manifest.Parallel > 1 {
		headroom := throttler.WorkingSetHeadroom()
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		// A target that cannot fit even an empty process gains nothing from
		// reclaim. Prefer a smaller graph before permanently shrinking its
		// unit. Temporary pressure still uses the complete candidate set.
		if headroom.Limit > 0 && workingLayoutTotal(specs[0]) > headroom.Limit {
			return nil, &importDOPRetryError{max(1, manifest.Parallel/2)}
		}
	}
	var working *rscthrottler.WorkingSet
	if manifest.NoWait {
		working, err = tryMemoryCandidates(ctx, throttler, specs)
	} else if manifest.Parallel <= 1 {
		working, err = throttler.ReserveWorkingSetCandidatesUntilFit(ctx, specs)
	} else {
		working, err = throttler.ReserveWorkingSetCandidates(ctx, specs)
	}
	if err != nil {
		var capacity *rscthrottler.WorkingSetCapacityError
		if errors.As(err, &capacity) && manifest.Parallel > 1 {
			return nil, &importDOPRetryError{max(1, manifest.Parallel/2)}
		}
		return nil, err
	}
	meta := metas[working.UnitBytes()]
	meta.WorkingSet = working
	// A writer may otherwise have to write each upstream batch separately when
	// the already admitted input and workspace promises leave no ordinary data
	// credits. Reserve at most one shared batch unit when it fits; failure keeps
	// the existing zero-credit path and never shrinks or delays admission.
	// Avoid consuming headroom needed for a later CSV record-budget growth.
	if meta.RecordBytes == int64(config.LargestEntryLimit) && manifestHasCSVWriter(manifest) {
		if _, err = working.TryReserveStaging(ctx, meta.UnitBytes); err != nil {
			working.Close()
			return nil, err
		}
	}
	bindMemoryRecordGrowth(meta, manifest)
	return meta, nil
}

func manifestHasCSVWriter(manifest MemoryManifest) bool {
	var source, writer bool
	for _, stage := range manifest.Stages {
		source = source || stage.Role == MemoryStageCSV && stage.Count > 0
	}
	for _, producer := range manifest.Producers {
		writer = writer || producer.Kind == "insert-meta" || producer.Kind == "multi-meta"
	}
	return source && writer
}

// tryMemoryCandidates never joins an admission queue or full recovery. A
// transaction may own the very workspace/locks that a queued request needs
// released. Try still preserves the existing FIFO and complete I/W accounting.
func tryMemoryCandidates(ctx context.Context, throttler rscthrottler.WorkingSetThrottler, specs []rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, error) {
	if len(specs) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("no legal import working layout")
	}
	minimum := workingLayoutTotal(specs[0])
	for _, spec := range specs {
		minimum = min(minimum, workingLayoutTotal(spec))
		working, granted, err := throttler.TryReserveWorkingSet(ctx, spec)
		if cause := context.Cause(ctx); cause != nil {
			if working != nil {
				working.Close()
			}
			return nil, cause
		}
		var tooLarge *rscthrottler.RequestTooLargeError
		if err != nil && !errors.As(err, &tooLarge) {
			return nil, err
		}
		if granted {
			return working, nil
		}
	}
	headroom := throttler.WorkingSetHeadroom()
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	// FIFO contention can also deny a try even with Available >= Minimum.
	// This reports immediate admission failure, not permanent impossibility.
	return nil, &rscthrottler.WorkingSetCapacityError{Minimum: minimum, Available: headroom.Available}
}

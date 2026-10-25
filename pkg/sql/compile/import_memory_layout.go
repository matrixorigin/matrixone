// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
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
	TableCount      int
	InsertOnly      bool
	LeftTypes       []types.Type
	RightTypes      []types.Type
	NonEqExpression []byte
	KeyExpressions  [][][]byte
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
		for _, schema := range schemas {
			row, err := external.MaxRowBytes(schema)
			if err != nil {
				return nil, spec, false, err
			}
			maxRow = max(maxRow, row)
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
			capacity, err := external.SourceEnvelopeForFields(stage.Schema, unit, stage.RetainedFields)
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
			join := hashjoin.HashJoin{LeftTypes: stage.LeftTypes, RightTypes: stage.RightTypes, ResultCols: stage.ResultCols, EqConds: make([][]*plan.Expr, len(stage.KeyExpressions))}
			for side, expressions := range stage.KeyExpressions {
				for _, encoded := range expressions {
					expr := new(plan.Expr)
					if err := expr.Unmarshal(encoded); err != nil {
						return nil, spec, false, err
					}
					join.EqConds[side] = append(join.EqConds[side], expr)
				}
			}
			if len(stage.NonEqExpression) > 0 {
				join.NonEqCond = new(plan.Expr)
				if err := join.NonEqCond.Unmarshal(stage.NonEqExpression); err != nil {
					return nil, spec, false, err
				}
			}
			for _, col := range stage.ResultCols {
				if col.Rel < 0 || col.Rel > 1 || col.Pos < 0 || col.Rel == 0 && int(col.Pos) >= len(stage.LeftTypes) || col.Rel == 1 && int(col.Pos) >= len(stage.RightTypes) {
					return nil, spec, false, errors.New("invalid join memory projection")
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
				return nil, spec, false, errors.New("invalid producer stage index")
			}
			producerStages = append(producerStages, stage)
		default:
			return nil, spec, false, fmt.Errorf("unknown memory stage %q", stage.Role)
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
		return nil, spec, false, errors.New("pipeline metadata capacity overflows")
	}
	edgeMetadata, valid := process.PipelineEdgeMetadataBound(channelCapacity)
	if !valid {
		return nil, spec, false, errors.New("pipeline edge metadata overflows")
	}
	for _, stage := range producerStages {
		capacity := producerBounds[stage.Producer].Scratch
		if producer := manifest.Producers[stage.Producer]; producer.Receivers > 0 {
			edges, ok := memoryProduct(edgeMetadata, int64(producer.Receivers))
			if !ok {
				return nil, spec, false, errors.New("pipeline receiver metadata overflows")
			}
			capacity, ok = memorySum(capacity, edges)
			if !ok {
				return nil, spec, false, errors.New("pipeline metadata promise overflows")
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
		meta, spec, controlled, err := memoryManifestLayout(manifest, unit)
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

func admitMemoryManifest(ctx context.Context, throttler rscthrottler.RSCThrottler, manifest MemoryManifest) (*process.ImportMemory, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	target := min(manifest.TargetUnit, rscthrottler.BatchBytes(throttler))
	if target <= 0 {
		return nil, errors.New("no legal import batch unit")
	}
	specs, metas, err := memoryManifestCandidates(manifest, target)
	if err != nil || len(specs) == 0 {
		return nil, err
	}
	if manifest.Parallel > 1 {
		headroom := rscthrottler.WorkingSetHeadroom(throttler)
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
	final, untilFit := throttler.(rscthrottler.WorkingSetCandidatesUntilFitThrottler)
	if manifest.NoWait {
		working, err = tryMemoryCandidates(ctx, throttler, specs)
	} else if manifest.Parallel <= 1 && untilFit {
		working, err = final.ReserveWorkingSetCandidatesUntilFit(ctx, specs)
	} else if _, capable := throttler.(rscthrottler.WorkingSetCandidatesThrottler); capable {
		working, err = rscthrottler.ReserveWorkingSetCandidates(ctx, throttler, specs)
	} else {
		// Older test/embedding providers retain their single-spec API. The real CN
		// uses the FIFO candidate capability, which reselects after every release.
		working, err = reserveLegacyMemoryCandidates(ctx, throttler, specs)
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
	return meta, nil
}

// tryMemoryCandidates never joins an admission queue or full recovery. A
// transaction may own the very workspace/locks that a queued request needs
// released. Try still preserves the existing FIFO and complete I/W accounting.
func tryMemoryCandidates(ctx context.Context, throttler rscthrottler.RSCThrottler, specs []rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, error) {
	if len(specs) == 0 {
		return nil, errors.New("no legal import working layout")
	}
	minimum := workingLayoutTotal(specs[0])
	for _, spec := range specs {
		minimum = min(minimum, workingLayoutTotal(spec))
		working, granted, err := rscthrottler.TryReserveWorkingSet(ctx, throttler, spec)
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
	headroom := rscthrottler.WorkingSetHeadroom(throttler)
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	// FIFO contention can also deny a try even with Available >= Minimum.
	// This reports immediate admission failure, not permanent impossibility.
	return nil, &rscthrottler.WorkingSetCapacityError{Minimum: minimum, Available: headroom.Available}
}

func reserveLegacyMemoryCandidates(ctx context.Context, throttler rscthrottler.RSCThrottler, specs []rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, error) {
	for reclaimed := false; ; reclaimed = true {
		for _, spec := range specs {
			working, granted, err := rscthrottler.TryReserveWorkingSet(ctx, throttler, spec)
			var tooLarge *rscthrottler.RequestTooLargeError
			if err != nil && !errors.As(err, &tooLarge) {
				return nil, err
			}
			if granted {
				return working, nil
			}
			if err == nil && rscthrottler.WorkingSetHeadroom(throttler).HasOwners {
				return rscthrottler.ReserveWorkingSet(ctx, throttler, spec)
			}
		}
		if reclaimed {
			headroom := rscthrottler.WorkingSetHeadroom(throttler)
			return nil, &rscthrottler.WorkingSetCapacityError{Minimum: workingLayoutTotal(specs[len(specs)-1]), Available: headroom.Available}
		}
		if err := rscthrottler.ReclaimForWorkingSet(ctx, throttler); err != nil {
			return nil, err
		}
		// Reclaim changed the physical state: retry the largest candidate rather
		// than retaining the tiny unit considered under the previous pressure.
	}
}

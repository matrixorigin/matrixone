// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package compile

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type importDOPRetryError struct{ next int }

func (e *importDOPRetryError) Error() string {
	return fmt.Sprintf("import working set requires recompilation at parallelism %d", e.next)
}

func tableMemorySchema(table *plan.TableDef) []types.Type {
	if table == nil {
		return nil
	}
	result := make([]types.Type, len(table.Cols))
	for i, col := range table.Cols {
		result[i] = plan2.MakeTypeByPlan2Type(col.Typ)
	}
	return result
}

func addMemorySlots(count *uint32, parallel, multiplier int) error {
	if parallel <= 0 {
		parallel = 1
	}
	if uint64(parallel)*uint64(multiplier) > uint64(math.MaxUint32-*count) {
		return errors.New("too many import working slots")
	}
	*count += uint32(parallel * multiplier)
	return nil
}

// importMemoryNoWait is an origin policy: transactions that are not known to
// be autocommit can retain previous statements' workspace and locks. Waiting
// for those resources before this statement returns could wait on itself.
// A nil operator preserves the existing isolated embedding/test contract.
func (c *Compile) importMemoryNoWait() bool {
	if c.proc == nil || c.proc.Base == nil || c.proc.GetTxnOperator() == nil {
		return false
	}
	options := c.proc.GetTxnOperator().TxnOptions()
	return options.ByBegin || !options.Autocommit
}

func memoryStage(op vm.Operator, count int) (MemoryStageDescriptor, bool, error) {
	stage := MemoryStageDescriptor{Count: uint32(count)}
	switch value := op.(type) {
	case *external.External:
		if !value.UsesMemoryInput() {
			return stage, false, nil
		}
		stage.Role = MemoryStageCSV
		stage.Schema, _ = value.MemoryInputSchema()
		stage.RetainedFields = max(0, int(value.Es.ColumnListLen)+2)
		for _, attr := range value.Es.Attrs {
			stage.RetainedFields = max(stage.RetainedFields, int(attr.ColFieldIndex)+1)
		}
	case *multi_update.MultiUpdate:
		if value.Action != multi_update.UpdateWriteS3 {
			return stage, false, nil
		}
		stage.Role = MemoryStageOutput
		stage.InsertOnly = multi_update.InsertOnlyOutput(value.MultiUpdateCtx)
		stage.TableCount = len(value.MultiUpdateCtx)
		for _, table := range value.MultiUpdateCtx {
			stage.Projections = append(stage.Projections, tableMemorySchema(table.TableDef))
		}
	case *insert.Insert:
		if !value.ToWriteS3 || value.InsertCtx == nil {
			return stage, false, nil
		}
		stage.Role = MemoryStageOutput
		stage.InsertOnly = true
		stage.TableCount = 1
		stage.Schema = tableMemorySchema(value.InsertCtx.TableDef)
	case *hashjoin.HashJoin:
		stage.Role = MemoryStageJoin
		stage.LeftTypes = value.LeftTypes
		stage.RightTypes = value.RightTypes
		stage.ResultCols = value.ResultCols
		if value.NonEqCond != nil {
			var err error
			stage.NonEqExpression, err = value.NonEqCond.Marshal()
			if err != nil {
				return stage, false, err
			}
		}
		stage.KeyExpressions = make([][][]byte, len(value.EqConds))
		for side, expressions := range value.EqConds {
			for _, expr := range expressions {
				if expr == nil {
					return stage, false, errors.New("nil join memory expression")
				}
				encoded, err := expr.Marshal()
				if err != nil {
					return stage, false, err
				}
				stage.KeyExpressions[side] = append(stage.KeyExpressions[side], encoded)
			}
		}
	default:
		return stage, false, nil
	}
	return stage, true, nil
}

// localMemoryManifest describes the complete eligible execution graph. The
// producer walk rejects remote placement/receivers before constructing it.
func (c *Compile) localMemoryManifest(unit int64) (MemoryManifest, error) {
	producers, ids, supported, err := c.memoryProducers()
	if err != nil || !supported {
		return MemoryManifest{}, err
	}
	manifest := MemoryManifest{TargetUnit: unit, Parallel: c.ncpu, NoWait: c.importMemoryNoWait(), Producers: producers}
	var walk func(*Scope) error
	walk = func(scope *Scope) error {
		if scope == nil {
			return nil
		}
		if err := vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			count := max(1, scope.NodeInfo.Mcpu)
			if uint64(count) > uint64(^uint32(0)) {
				return errors.New("import parallelism overflows manifest")
			}
			stage, controlled, err := memoryStage(op, count)
			if err != nil {
				return err
			}
			if controlled {
				manifest.Stages = append(manifest.Stages, stage)
			}
			manifest.Stages = append(manifest.Stages, memoryProducerStages(producers, ids[op], uint32(count))...)
			return nil
		}); err != nil {
			return err
		}
		for _, child := range scope.PreScopes {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	for _, scope := range c.scopes {
		if err := walk(scope); err != nil {
			return MemoryManifest{}, err
		}
	}
	if !c.memoryPipelineChannelsFit(manifest) {
		return MemoryManifest{}, nil
	}
	return manifest, nil
}

func (c *Compile) beginStatementImportMemory(ctx context.Context, throttler rscthrottler.RSCThrottler) error {
	if c.isPrepare || c.importMemoryPrepared || !c.supportsBoundedImportKeys() {
		return nil
	}
	manifest, err := c.localMemoryManifest(rscthrottler.BatchBytes(throttler))
	if err != nil {
		return err
	}
	hasSource := false
	for _, stage := range manifest.Stages {
		hasSource = hasSource || stage.Role == MemoryStageCSV && stage.Count > 0
	}
	if !hasSource {
		return nil
	}
	memory, err := admitMemoryManifest(ctx, throttler, manifest)
	if err == nil && memory != nil {
		c.installImportMemory(memory)
	}
	return err
}

// Keep the complete statement on its existing path when any producer lacks a
// bounded lifetime contract. Partition wrappers hide independent writers, memory
// tables return retained raw data, and derived PreInsert columns still evaluate
// an entire batch without a separate input/scratch promise.
func (c *Compile) supportsBoundedImportKeys() bool {
	supported := true
	var walk func(*Scope)
	walk = func(scope *Scope) {
		if scope == nil || !supported {
			return
		}
		vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			switch value := op.(type) {
			case *insert.PartitionInsert, *multi_update.PartitionMultiUpdate:
				supported = false
			case *insert.Insert:
				if value.ToWriteS3 && value.InsertCtx != nil {
					_, memoryTable := value.InsertCtx.Engine.(*memoryengine.BindedEngine)
					supported = supported && !memoryTable
				}
			case *preinsert.PreInsert:
				supported = supported && value.CompPkeyExpr == nil && value.ClusterByExpr == nil
			case *hashbuild.HashBuild:
				supported = supported && hashbuild.SupportsBoundedSpillKeys(value.Conditions)
			case *hashjoin.HashJoin:
				if value.NonEqCond != nil {
					supported = supported && hashbuild.SupportsBoundedSpillKeys([]*plan.Expr{value.NonEqCond})
				}
				supported = supported && len(value.EqConds) == 2
				for _, side := range value.EqConds {
					supported = supported && hashbuild.SupportsBoundedSpillKeys(side)
				}
			}
			return nil
		})
		for _, child := range scope.PreScopes {
			walk(child)
		}
	}
	for _, scope := range c.scopes {
		walk(scope)
	}
	if !supported {
		return false
	}
	_, _, supported, err := c.memoryProducers()
	return supported && err == nil
}

func workingLayoutTotal(spec rscthrottler.WorkingSetSpec) int64 {
	total := spec.IOBytes
	for _, slot := range spec.Inputs {
		if slot.Bytes < 0 || slot.Count > 0 && slot.Bytes > math.MaxInt64/int64(slot.Count) {
			return math.MaxInt64
		}
		n := slot.Bytes * int64(slot.Count)
		if total > math.MaxInt64-n {
			return math.MaxInt64
		}
		total += n
	}
	return total
}

func (c *Compile) beginImportMemory() error {
	if c.proc == nil || c.proc.Base == nil || c.importMemory != nil {
		return nil
	}
	// A nested compile sharing this exact process borrows the parent's ticket;
	// it must never overwrite or close that parent's root reference.
	if c.proc.Base.ImportMemory != nil {
		return nil
	}
	rt := runtime.ServiceRuntime(c.proc.GetService())
	if rt == nil {
		return nil
	}
	value, ok := rt.GetGlobalVariables(runtime.CNMemoryThrottler)
	if !ok {
		return nil
	}
	throttler, ok := value.(rscthrottler.RSCThrottler)
	if !ok {
		return nil
	}
	if _, ok = throttler.(rscthrottler.ContextThrottler); !ok {
		return nil
	}
	ctx := c.proc.Ctx
	if ctx == nil {
		ctx = c.proc.GetTopContext()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return c.beginStatementImportMemory(ctx, throttler)
}

func (c *Compile) installImportMemory(memory *process.ImportMemory) {
	c.importMemory = memory
	c.importMemoryBases = make(map[*process.BaseProcess]*process.ImportMemory)
	set := func(proc *process.Process) {
		if proc == nil || proc.Base == nil {
			return
		}
		if _, ok := c.importMemoryBases[proc.Base]; ok {
			return
		}
		c.importMemoryBases[proc.Base] = proc.Base.ImportMemory
		proc.Base.ImportMemory = memory
	}
	set(c.proc)
	var walk func(*Scope)
	walk = func(scope *Scope) {
		if scope == nil {
			return
		}
		set(scope.Proc)
		for _, child := range scope.PreScopes {
			walk(child)
		}
	}
	for _, scope := range c.scopes {
		walk(scope)
	}
}

func (c *Compile) endImportMemory() {
	if c.importMemory == nil {
		return
	}
	// The joined owner drains every receiver edge (including direct remote
	// notifications), then destroys slots, before returning its complete promise.
	c.closePipelineSpoolsAfterReadersStopped()
	for base, previous := range c.importMemoryBases {
		if base.ImportMemory == c.importMemory {
			base.ImportMemory = previous
		}
	}
	c.importMemory.WorkingSet.Close()
	c.importMemory = nil
	c.importMemoryBases = nil
}

func (c *Compile) recompileImportMemory(ctx context.Context, parallel int) (*Compile, error) {
	next := NewCompile(c.addr, c.db, c.sql, c.tenant, c.uid, c.e, c.proc, c.stmt, c.isInternal, c.cnLabel, c.startAt)
	next.importMemoryPrepared = c.isPrepare || c.importMemoryPrepared
	next.ncpu = parallel
	next.SetOriginSQL(c.originSQL)
	if err := next.Compile(ctx, c.pn, c.fill); err != nil {
		next.Release()
		return nil, err
	}
	return next, nil
}

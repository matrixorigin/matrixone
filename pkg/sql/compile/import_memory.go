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
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
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

func (c *Compile) walkLocalImportOperators(visit func(vm.Operator, int) error) error {
	var walk func(*Scope) error
	walk = func(scope *Scope) error {
		if scope == nil {
			return nil
		}
		for _, child := range scope.PreScopes {
			if err := walk(child); err != nil {
				return err
			}
		}
		if !scope.ipAddrMatch(c.addr) {
			return nil
		}
		return vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error { return visit(op, max(1, scope.NodeInfo.Mcpu)) })
	}
	for _, scope := range c.scopes {
		if err := walk(scope); err != nil {
			return err
		}
	}
	return nil
}

// localMemoryManifest uses actual planned copies, with no source allocation.
func (c *Compile) localMemoryManifest(unit int64) (MemoryManifest, error) {
	manifest := MemoryManifest{TargetUnit: unit, Parallel: c.ncpu}
	err := c.walkLocalImportOperators(func(op vm.Operator, parallel int) error {
		if uint64(parallel) > math.MaxUint32 {
			return errors.New("too many import working slots")
		}
		stage, controlled, err := memoryStage(op, parallel)
		if err != nil {
			return err
		}
		if controlled {
			manifest.Stages = append(manifest.Stages, stage)
		}
		return nil
	})
	return manifest, err
}

func (c *Compile) importMemoryLayout(unit int64) (*process.ImportMemory, rscthrottler.WorkingSetSpec, bool, error) {
	manifest, err := c.localMemoryManifest(unit)
	if err != nil {
		return nil, rscthrottler.WorkingSetSpec{}, false, err
	}
	return memoryManifestLayout(manifest, unit)
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

func (c *Compile) admitImportMemory(ctx context.Context, throttler rscthrottler.RSCThrottler) error {
	manifest, err := c.localMemoryManifest(rscthrottler.BatchBytes(throttler))
	if err != nil {
		return err
	}
	memory, err := admitMemoryManifest(ctx, throttler, manifest)
	if err != nil {
		return err
	}
	if memory != nil {
		c.installImportMemory(memory)
	}
	return nil
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
	locations := c.memoryExecutionLocations()
	var walk func(*Scope)
	walk = func(scope *Scope) {
		if scope == nil {
			return
		}
		if locations[scope] == c.addr {
			set(scope.Proc)
		}
		for _, child := range scope.PreScopes {
			walk(child)
		}
	}
	for _, scope := range c.scopes {
		walk(scope)
	}
}

// attachImportMemory binds a control-owned complete CN ticket. Scope cleanup
// restores the process views and detaches; it never closes the control root.
func (c *Compile) attachImportMemory(memory *process.ImportMemory) {
	if memory == nil {
		return
	}
	c.installImportMemory(memory)
	c.importMemoryBorrowed = true
}

func (c *Compile) endImportMemory() {
	c.clearImportMemoryTokens()
	defer func() {
		closeMemoryControls(c.importMemoryControls, nil)
		c.importMemoryControls = nil
	}()
	if c.importMemory == nil {
		return
	}
	for base, previous := range c.importMemoryBases {
		if base.ImportMemory == c.importMemory {
			base.ImportMemory = previous
		}
	}
	if c.importMemoryDetach != nil {
		c.importMemoryDetach()
		c.importMemoryDetach = nil
	}
	if !c.importMemoryBorrowed {
		c.importMemory.WorkingSet.Close()
	}
	c.importMemoryBorrowed = false
	c.importMemory = nil
	c.importMemoryBases = nil
}

func (c *Compile) recompileImportMemory(ctx context.Context, parallel int) (*Compile, error) {
	next := NewCompile(c.addr, c.db, c.sql, c.tenant, c.uid, c.e, c.proc, c.stmt, c.isInternal, c.cnLabel, c.startAt)
	next.ncpu = parallel
	next.SetOriginSQL(c.originSQL)
	if err := next.Compile(ctx, c.pn, c.fill); err != nil {
		next.Release()
		return nil, err
	}
	return next, nil
}

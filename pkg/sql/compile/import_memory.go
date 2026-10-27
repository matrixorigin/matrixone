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
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
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
		return moerr.NewInternalErrorNoCtx("too many import working slots")
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
		stage.RecordBytes = value.MemoryRecordBytes()
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
		stage.NonEqExpression = value.NonEqCond
		stage.KeyExpressions = value.EqConds
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
				return moerr.NewInternalErrorNoCtx("import parallelism overflows manifest")
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

func (c *Compile) beginStatementImportMemory(ctx context.Context, throttler rscthrottler.WorkingSetThrottler) error {
	if c.isPrepare || c.importMemoryPrepared || !c.hasBoundedImportSource() {
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

// hasBoundedImportSource is deliberately cheaper than producer/expression
// analysis. Ordinary queries must leave before constructing an import proof.
func (c *Compile) hasBoundedImportSource() bool {
	var walk func(*Scope) bool
	walk = func(scope *Scope) bool {
		if scope == nil {
			return false
		}
		found := false
		_ = vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			if source, ok := op.(*external.External); ok && source.UsesMemoryInput() {
				found = true
			}
			return nil
		})
		if found {
			return true
		}
		for _, child := range scope.PreScopes {
			if walk(child) {
				return true
			}
		}
		return false
	}
	for _, scope := range c.scopes {
		if walk(scope) {
			return true
		}
	}
	return false
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
	throttler, ok := value.(rscthrottler.WorkingSetThrottler)
	if !ok {
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

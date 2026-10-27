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
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeblock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffleV2"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// MemoryProducerDescriptor is a topologically ordered, immutable allocation
// proof for an eligible local graph. Inputs refer only to earlier entries, so
// bounds can be computed before readers or executors allocate.
type MemoryProducerDescriptor struct {
	Kind        string
	Inputs      []uint32
	Schema      []types.Type
	RecordBytes int64
	Expressions []*plan.Expr
	Project     bool
	ResultCols  []colexec.ResultPos
	Function    *MemoryFunctionDescriptor
	Copies      uint32
	Receivers   uint32
	OwnedCols   []int32
	LockTargets []lockop.MemoryTarget
	TableCount  int
	ColumnCount int
	InsertOnly  bool
}

type MemoryFunctionDescriptor struct {
	Name   string
	Params []byte
	Args   []*plan.Expr
	Rets   []*plan.ColDef
}

// Row/Batch and Columns bound compact referenced storage (fixed headers plus
// out-of-line payload), not retained capacity. CSV is flat, so WindowSize is at
// most its allocated backing. Inline value bytes fit inside those headers;
// therefore the same bound also covers bytes consumed by key/string encoders.
// Expression logical-result bounds may overcount inline payload conservatively.
// Bitmap/header objects have separate allocation promises in Scratch/source I.
type memoryProducerBound struct {
	Rows                int
	Batch, Row, Scratch int64
	Columns             []table_function.MemoryColumnBound
	Metadata            bool
	MetadataKind        string
}

func memorySum(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}

func memoryProduct(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a != 0 && b > math.MaxInt64/a {
		return 0, false
	}
	return a * b, true
}

// The second list contains filters/scratch whose result does not replace the
// batch. Projection fusion must be enumerated here alongside standalone Project.
func memoryOperatorExpressions(op vm.Operator) (project, scratch []*plan.Expr) {
	switch value := op.(type) {
	case *external.External:
		project = value.ProjectList
	case *projection.Projection:
		project = value.ProjectList
	case *table_scan.TableScan:
		project = value.ProjectList
		scratch = append(append([]*plan.Expr(nil), value.FilterExprs...), value.RuntimeFilterExprs...)
	case *filter.Filter:
		scratch = append(append([]*plan.Expr(nil), value.FilterExprs...), value.RuntimeFilterExprs...)
	case *shuffle.Shuffle:
		if value.ShuffleExpr != nil {
			scratch = []*plan.Expr{value.ShuffleExpr}
		}
	case *shuffleV2.ShuffleV2:
		if value.ShuffleExpr != nil {
			scratch = []*plan.Expr{value.ShuffleExpr}
		}
	case *limit.Limit:
		if value.LimitExpr != nil {
			scratch = []*plan.Expr{value.LimitExpr}
		}
	case *offset.Offset:
		if value.OffsetExpr != nil {
			scratch = []*plan.Expr{value.OffsetExpr}
		}
	}
	return
}

func finiteScanSchema(schema []types.Type) bool {
	if len(schema) == 0 {
		return false
	}
	for _, typ := range schema {
		// Non-strict/LOCAL loads can persist over-width strings; their declared
		// VARCHAR width is not a physical bound on a future reader allocation.
		if !typ.IsFixedLen() || typ.TypeSize() <= 0 {
			return false
		}
	}
	return true
}

func memoryProducerKind(op vm.Operator) (string, bool) {
	switch value := op.(type) {
	case *insert.PartitionInsert, *multi_update.PartitionMultiUpdate:
		return "", false
	case *external.External:
		return "csv", value.UsesMemoryInput()
	case *table_scan.TableScan:
		schema := make([]types.Type, len(value.Types))
		for i, typ := range value.Types {
			schema[i] = plan2.MakeTypeByPlan2Type(typ)
		}
		return "scan", finiteScanSchema(schema)
	case *hashjoin.HashJoin:
		if len(value.EqConds) != 2 || value.NonEqCond != nil && !hashbuild.SupportsBoundedSpillKeys([]*plan.Expr{value.NonEqCond}) {
			return "", false
		}
		for _, side := range value.EqConds {
			if !hashbuild.SupportsBoundedSpillKeys(side) {
				return "", false
			}
		}
		return "join", true
	case *hashbuild.HashBuild:
		return "empty", hashbuild.SupportsBoundedSpillKeys(value.Conditions)
	case *insert.Insert:
		if value.ToWriteS3 && value.InsertCtx != nil {
			_, memoryTable := value.InsertCtx.Engine.(*memoryengine.BindedEngine)
			return "insert-meta", !memoryTable
		}
		return "", false
	case *multi_update.MultiUpdate:
		switch value.Action {
		case multi_update.UpdateWriteS3:
			return "multi-meta", true
		case multi_update.UpdateFlushS3Info:
			return "flush-meta", true
		default:
			return "", false
		}
	case *mergeblock.MergeBlock:
		return "merge-meta", true
	case *connector.Connector:
		return "spool", true
	case *dispatch.Dispatch:
		if len(value.RemoteRegs) > 0 {
			return "", false
		}
		if len(value.LocalRegs) > 0 {
			if uint64(len(value.LocalRegs)) > math.MaxUint32 {
				return "", false
			}
			return "spool", true
		}
		return "", false
	case *preinsert.PreInsert:
		return "preinsert", !value.IsOldUpdate && value.CompPkeyExpr == nil && value.ClusterByExpr == nil
	case *lockop.LockOp:
		_, ok := value.MemoryTargets()
		return "lock", ok
	case *apply.Apply:
		return "apply", value.ApplyType == apply.CROSS && value.TableFunction != nil && value.TableFunction.SupportsMemoryBound() && hashbuild.SupportsBoundedSpillKeys(value.TableFunction.Args)
	default:
		switch op.OpType() {
		case vm.Projection, vm.Filter,
			vm.HashBuild, vm.Merge, vm.Dispatch, vm.Connector, vm.Output,
			vm.Limit, vm.Offset, vm.MergeLimit, vm.MergeOffset,
			vm.PreInsert, vm.Insert, vm.MultiUpdate, vm.MergeBlock:
			return "pass", true
		default:
			// In particular Group/MergeGroup/Fill have additional executors and
			// retained state; Source/ValueScan/TVF can produce unrelated raw rows.
			return "", false
		}
	}
}

func (c *Compile) memoryProducers() ([]MemoryProducerDescriptor, map[vm.Operator]uint32, bool, error) {
	if c.needBlock {
		return nil, nil, false, nil
	}
	var result []MemoryProducerDescriptor
	ids := make(map[vm.Operator]uint32)
	seen := make(map[*Scope]uint32)
	active := make(map[*Scope]bool)
	buildScopes := make(map[int32]*Scope)
	buildOps := make(map[int32]*hashbuild.HashBuild)
	type localSender struct {
		scope *Scope
		op    vm.Operator
	}
	senders := make(map[*process.WaitRegister][]localSender)
	foundScopes := make(map[*Scope]bool)
	var findBuilds func(*Scope)
	findBuilds = func(scope *Scope) {
		if scope == nil || foundScopes[scope] {
			return
		}
		foundScopes[scope] = true
		_ = vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			if build, ok := op.(*hashbuild.HashBuild); ok {
				buildScopes[build.JoinMapTag] = scope
				buildOps[build.JoinMapTag] = build
			}
			switch value := op.(type) {
			case *connector.Connector:
				if value.Reg != nil {
					senders[value.Reg] = append(senders[value.Reg], localSender{scope, op})
				}
			case *dispatch.Dispatch:
				for _, reg := range value.LocalRegs {
					if reg != nil {
						senders[reg] = append(senders[reg], localSender{scope, op})
					}
				}
			}
			return nil
		})
		for _, child := range scope.PreScopes {
			findBuilds(child)
		}
	}
	for _, scope := range c.scopes {
		findBuilds(scope)
	}
	var scopeProducer func(*Scope) (uint32, bool, error)
	scopeProducer = func(scope *Scope) (uint32, bool, error) {
		if id, ok := seen[scope]; ok {
			return id, true, nil
		}
		if scope == nil || active[scope] {
			return 0, false, nil
		}
		if len(scope.RemoteReceivRegInfos) > 0 || scope.Magic == Remote && !scope.ipAddrMatch(c.addr) && checkPipelineStandaloneExecutableAtRemote(scope) {
			return 0, false, nil
		}
		active[scope] = true
		defer delete(active, scope)
		// These expressions are installed only after runtime-filter messages
		// arrive. Their decoded list/hash has no pre-admission scalar contract.
		if scope.DataSource != nil && len(scope.DataSource.RuntimeFilterSpecs) > 0 {
			return 0, false, nil
		}
		if data := scope.DataSource; data != nil {
			if data.FilterExpr != nil || len(data.FilterList) > 0 || len(data.BlockFilterList) > 0 || len(data.OrderBy) > 0 {
				return 0, false, nil
			}
			if data.node != nil && (len(data.node.FilterList) > 0 || len(data.node.BlockFilterList) > 0 || len(data.node.OrderBy) > 0) {
				return 0, false, nil
			}
		}
		var incoming []uint32
		for _, child := range scope.PreScopes {
			id, ok, err := scopeProducer(child)
			if !ok || err != nil {
				return 0, ok, err
			}
			incoming = append(incoming, id)
		}
		result = append(result, MemoryProducerDescriptor{Kind: "max", Inputs: incoming})
		fallback := uint32(len(result) - 1)
		var opProducer func(vm.Operator) (uint32, bool, error)
		opProducer = func(op vm.Operator) (uint32, bool, error) {
			if op == nil {
				return fallback, true, nil
			}
			if _, exists := ids[op]; exists {
				return 0, false, nil
			}
			kind, ok := memoryProducerKind(op)
			if !ok {
				return 0, false, nil
			}
			desc := MemoryProducerDescriptor{Kind: kind}
			channelInputs := false
			if receiver, ok := op.(*merge.Merge); ok && scope.Proc != nil && len(scope.Proc.Reg.MergeReceivers) > 0 {
				// SinkScan is a separate step: its real upstream lives in the
				// WaitRegister graph, not necessarily in this scope's PreScopes.
				regs := scope.Proc.Reg.MergeReceivers
				if receiver.Partial {
					if receiver.StartIDX < 0 || receiver.EndIDX < receiver.StartIDX || int(receiver.EndIDX) > len(regs) {
						return 0, false, nil
					}
					regs = regs[receiver.StartIDX:receiver.EndIDX]
				}
				if uint64(len(regs)) > math.MaxUint32 {
					return 0, false, nil
				}
				desc.Receivers = uint32(len(regs))
				channelInputs = true
				for _, reg := range regs {
					upstream := senders[reg]
					if len(upstream) == 0 {
						return 0, false, nil
					}
					for _, sender := range upstream {
						if _, exists := ids[sender.op]; !exists {
							if _, ok, err := scopeProducer(sender.scope); !ok || err != nil {
								return 0, ok, err
							}
						}
						desc.Inputs = append(desc.Inputs, ids[sender.op])
					}
				}
			}
			for _, child := range op.GetOperatorBase().Children {
				id, ok, err := opProducer(child)
				if !ok || err != nil {
					return 0, ok, err
				}
				desc.Inputs = append(desc.Inputs, id)
			}
			if len(desc.Inputs) == 0 && !channelInputs {
				desc.Inputs = []uint32{fallback}
			}
			switch value := op.(type) {
			case *external.External:
				desc.Schema, _ = value.MemoryInputSchema()
				desc.RecordBytes = value.MemoryRecordBytes()
			case *table_scan.TableScan:
				for _, typ := range value.Types {
					desc.Schema = append(desc.Schema, plan2.MakeTypeByPlan2Type(typ))
				}
			case *hashjoin.HashJoin:
				desc.ResultCols = value.ResultCols
			case *connector.Connector:
				desc.Copies = 2
				desc.Receivers = 1
			case *dispatch.Dispatch:
				desc.Copies = uint32(max(2, len(value.LocalRegs)))
				desc.Receivers = uint32(len(value.LocalRegs))
			case *preinsert.PreInsert:
				if value.TableDef != nil {
					for idx, col := range value.TableDef.Cols {
						if col.Typ.AutoIncr {
							desc.OwnedCols = append(desc.OwnedCols, value.ColOffset+int32(idx))
						}
					}
				}
			case *lockop.LockOp:
				refreshEnabled := true // Unknown clients retain the conservative contract.
				if scope.Proc != nil && scope.Proc.Base != nil && scope.Proc.Base.TxnClient != nil {
					// txnClient's feature is set only at construction, and child
					// pipelines use this same client through the attempt's Base.
					refreshEnabled = scope.Proc.Base.TxnClient.RefreshExpressionEnabled()
				}
				desc.LockTargets, _ = value.MemoryTargetsForRefresh(refreshEnabled)
			case *insert.Insert:
				desc.InsertOnly = value.ToWriteS3 && value.InsertCtx != nil
			case *multi_update.MultiUpdate:
				desc.InsertOnly = multi_update.InsertOnlyOutput(value.MultiUpdateCtx)
				desc.TableCount = len(value.MultiUpdateCtx)
				for _, ctx := range value.MultiUpdateCtx {
					if ctx == nil || ctx.TableDef == nil || len(ctx.TableDef.Cols) > math.MaxInt-desc.ColumnCount {
						return 0, false, nil
					}
					desc.ColumnCount += len(ctx.TableDef.Cols)
				}
			case *apply.Apply:
				tf := value.TableFunction
				desc.Function = &MemoryFunctionDescriptor{Name: tf.FuncName, Params: tf.Params, Args: tf.Args, Rets: tf.Rets}
				desc.ResultCols = value.Result
			}
			if join, ok := op.(*hashjoin.HashJoin); ok && len(desc.Inputs) == 1 {
				buildScope, found := buildScopes[join.JoinMapTag]
				if !found {
					return 0, false, nil
				}
				if _, exists := ids[buildOps[join.JoinMapTag]]; !exists {
					if _, ok, err := scopeProducer(buildScope); !ok || err != nil {
						return 0, ok, err
					}
				}
				buildID := ids[buildOps[join.JoinMapTag]]
				if len(result[buildID].Inputs) != 1 {
					return 0, false, nil
				}
				desc.Inputs = append(desc.Inputs, result[buildID].Inputs[0])
			}
			result = append(result, desc)
			id := uint32(len(result) - 1)
			if kind == "merge-meta" && (len(desc.Inputs) != 1 || !memoryMetadataProducer(result, desc.Inputs[0], "insert")) {
				return 0, false, nil
			}
			if kind == "flush-meta" && (len(desc.Inputs) != 1 || !memoryMetadataProducer(result, desc.Inputs[0], "multi")) {
				return 0, false, nil
			}
			project, scratch := memoryOperatorExpressions(op)
			for _, list := range []struct {
				exprs   []*plan.Expr
				project bool
			}{{scratch, false}, {project, true}} {
				if len(list.exprs) == 0 {
					continue
				}
				if !hashbuild.SupportsBoundedSpillKeys(list.exprs) {
					return 0, false, nil
				}
				result = append(result, MemoryProducerDescriptor{Kind: "expression", Inputs: []uint32{id}, Expressions: list.exprs, Project: list.project})
				id = uint32(len(result) - 1)
			}
			ids[op] = id
			return id, true, nil
		}
		id, ok, err := opProducer(scope.RootOp)
		if ok && err == nil {
			seen[scope] = id
		}
		return id, ok, err
	}
	for _, scope := range c.scopes {
		if _, ok, err := scopeProducer(scope); !ok || err != nil {
			return nil, nil, ok, err
		}
	}
	return result, ids, true, nil
}

func memoryProducerStages(descs []MemoryProducerDescriptor, id uint32, count uint32) []MemoryStageDescriptor {
	var result []MemoryStageDescriptor
	for {
		desc := descs[id]
		if desc.Kind == "pass" && desc.Receivers > 0 || desc.Kind == "expression" || desc.Kind == "scan" || desc.Kind == "apply" || desc.Kind == "spool" || desc.Kind == "preinsert" || desc.Kind == "lock" || desc.Kind == "merge-meta" || desc.Kind == "flush-meta" {
			result = append(result, MemoryStageDescriptor{Role: MemoryStageProducer, Count: count, Producer: id})
		}
		if desc.Kind != "expression" || len(desc.Inputs) != 1 {
			break
		}
		id = desc.Inputs[0]
	}
	return result
}

func memoryMetadataProducer(descs []MemoryProducerDescriptor, id uint32, kind string) bool {
	if int(id) >= len(descs) {
		return false
	}
	desc := descs[id]
	switch desc.Kind {
	case "insert-meta", "merge-meta":
		return kind == "insert"
	case "multi-meta", "flush-meta":
		return kind == "multi"
	case "empty":
		return true
	case "pass", "max", "spool":
		if len(desc.Inputs) == 0 {
			return false
		}
		for _, child := range desc.Inputs {
			if !memoryMetadataProducer(descs, child, kind) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func expressionProducerBound(input memoryProducerBound, exprs []*plan.Expr, project bool) (memoryProducerBound, bool) {
	if !hashbuild.SupportsBoundedSpillKeys(exprs) {
		return memoryProducerBound{}, false
	}
	var validColumns func(*plan.Expr) bool
	validColumns = func(expr *plan.Expr) bool {
		if expr == nil {
			return false
		}
		if col := expr.GetCol(); col != nil {
			// ColumnExpressionExecutor deliberately normalizes RelPos to zero
			// for a single input batch. Preserve that actual consumer contract.
			return col.ColPos >= 0 && int(col.ColPos) < len(input.Columns)
		}
		if f := expr.GetF(); f != nil {
			for _, arg := range f.Args {
				if !validColumns(arg) {
					return false
				}
			}
		}
		return true
	}
	for _, expr := range exprs {
		if !validColumns(expr) {
			return memoryProducerBound{}, false
		}
	}
	columnRows := make([]int64, len(input.Columns))
	columnBatches := make([]int64, len(input.Columns))
	for i, col := range input.Columns {
		columnRows[i], columnBatches[i] = col.RowBytes, col.BatchBytes
	}
	all, ok := hashbuild.SpillKeyMemoryBoundForColumns(exprs, input.Batch, max(1, input.Rows), columnBatches)
	if !ok {
		return memoryProducerBound{}, false
	}
	result := input
	result.Scratch = all.ExpressionBytes
	if !project {
		return result, true
	}
	result.Metadata = false
	result.Columns = nil
	result.Batch, result.Row = 0, 0
	borrowed := make(map[int32]int64)
	var multiplicity int64
	var selectedRow, selectedBatch int64
	for _, expr := range exprs {
		var col table_function.MemoryColumnBound
		if ref := expr.GetCol(); ref != nil {
			if ref.ColPos < 0 || int(ref.ColPos) >= len(input.Columns) {
				return memoryProducerBound{}, false
			}
			col = input.Columns[ref.ColPos]
			borrowed[ref.ColPos]++
			multiplicity = max(multiplicity, borrowed[ref.ColPos])
			selectedRow, ok = memorySum(selectedRow, col.RowBytes)
			if !ok {
				return memoryProducerBound{}, false
			}
			selectedBatch, ok = memorySum(selectedBatch, col.BatchBytes)
			if !ok {
				return memoryProducerBound{}, false
			}
		} else {
			batch, ok := hashbuild.SpillKeyMemoryBoundForColumns([]*plan.Expr{expr}, input.Batch, max(1, input.Rows), columnBatches)
			if !ok {
				return memoryProducerBound{}, false
			}
			row, ok := hashbuild.SpillKeyMemoryBoundForColumns([]*plan.Expr{expr}, input.Row, 1, columnRows)
			if !ok {
				return memoryProducerBound{}, false
			}
			col = table_function.MemoryColumnBound{Type: plan2.MakeTypeByPlan2Type(expr.Typ), RowBytes: row.LogicalResultBytes, BatchBytes: batch.LogicalResultBytes}
			col.Constant = memoryExpressionConstant(expr, input.Columns)
		}
		result.Columns = append(result.Columns, col)
		if expr.GetCol() != nil {
			continue
		}
		result.Row, ok = memorySum(result.Row, col.RowBytes)
		if !ok {
			return memoryProducerBound{}, false
		}
		result.Batch, ok = memorySum(result.Batch, col.BatchBytes)
		if !ok {
			return memoryProducerBound{}, false
		}
	}
	// Distinct source columns share the input's aggregate record/batch bound.
	// Summing their individual worst rows would turn N fields of one legal CSV
	// record into N independent maximum records. Repeated projections use the
	// largest multiplicity, preserving their actual copy amplification.
	for _, pair := range []struct {
		input    int64
		target   *int64
		selected int64
	}{{input.Row, &result.Row, selectedRow}, {input.Batch, &result.Batch, selectedBatch}} {
		bytes, valid := memoryProduct(pair.input, multiplicity)
		if !valid {
			return memoryProducerBound{}, false
		}
		*pair.target, valid = memorySum(*pair.target, min(bytes, pair.selected))
		if !valid {
			return memoryProducerBound{}, false
		}
	}
	return result, true
}

func memoryExpressionConstant(expr *plan.Expr, input []table_function.MemoryColumnBound) bool {
	if expr.GetLit() != nil || expr.GetT() != nil {
		return true
	}
	if col := expr.GetCol(); col != nil {
		return col.ColPos >= 0 && int(col.ColPos) < len(input) && input[col.ColPos].Constant
	}
	if f := expr.GetF(); f != nil {
		for _, arg := range f.Args {
			if !memoryExpressionConstant(arg, input) {
				return false
			}
		}
		return true
	}
	return false
}

func memoryProducerBounds(descs []MemoryProducerDescriptor, unit int64) ([]memoryProducerBound, error) {
	return memoryProducerBoundsWithFrame(descs, unit, max(unit, 4096))
}

func memoryProducerBoundsWithFrame(descs []MemoryProducerDescriptor, unit, frameLimit int64) ([]memoryProducerBound, error) {
	if unit <= 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid producer unit")
	}
	result := make([]memoryProducerBound, len(descs))
	for i, desc := range descs {
		var inputs []memoryProducerBound
		input := memoryProducerBound{Metadata: len(desc.Inputs) > 0}
		for _, id := range desc.Inputs {
			if uint64(id) >= uint64(i) {
				return nil, moerr.NewInternalErrorNoCtx("producer graph is not topologically ordered")
			}
			child := result[id]
			if child.Batch > 0 || child.Rows > 0 {
				input.Metadata = input.Metadata && child.Metadata
				if input.Batch == 0 && input.Rows == 0 {
					input.MetadataKind = child.MetadataKind
				} else if input.MetadataKind != child.MetadataKind {
					input.MetadataKind = "mixed"
				}
			}
			inputs = append(inputs, child)
			input.Rows = max(input.Rows, child.Rows)
			input.Batch = max(input.Batch, child.Batch)
			input.Row = max(input.Row, child.Row)
			for j, col := range child.Columns {
				if j >= len(input.Columns) {
					input.Columns = append(input.Columns, col)
				} else {
					input.Columns[j].RowBytes = max(input.Columns[j].RowBytes, col.RowBytes)
					input.Columns[j].BatchBytes = max(input.Columns[j].BatchBytes, col.BatchBytes)
					input.Columns[j].Constant = input.Columns[j].Constant || col.Constant
				}
			}
		}
		bound := input
		bound.Scratch = 0
		ok := true
		switch desc.Kind {
		case "max", "pass":
		case "empty":
			bound = memoryProducerBound{Metadata: true}
		case "insert-meta":
			protocolLimit := frameLimit
			if desc.InsertOnly {
				var err error
				protocolLimit, err = colexec.InsertMetadataFrameLimit(unit, 1)
				if err != nil {
					return nil, err
				}
			}
			meta, valid := mergeblock.MetadataOutputBound(protocolLimit)
			if !valid {
				ok = false
				break
			}
			bound = memoryProducerBound{Rows: meta.Rows, Batch: meta.BatchBytes, Row: meta.RowBytes, Metadata: true, MetadataKind: "insert"}
			for j := range meta.ColumnBytes {
				bound.Columns = append(bound.Columns, table_function.MemoryColumnBound{Type: types.T_varchar.ToType(), RowBytes: meta.ColumnRowBytes[j], BatchBytes: meta.ColumnBytes[j]})
			}
		case "merge-meta":
			if !input.Metadata || input.MetadataKind != "insert" {
				return nil, moerr.NewInternalErrorNoCtx("unproven merge-block metadata source")
			}
			meta, valid := mergeblock.MetadataOutputBound(input.Batch)
			if !valid {
				ok = false
				break
			}
			bound.Scratch = meta.ScratchBytes
		case "multi-meta", "flush-meta":
			if desc.Kind == "flush-meta" && (!input.Metadata || input.MetadataKind != "multi") {
				return nil, moerr.NewInternalErrorNoCtx("unproven multi-update metadata source")
			}
			protocolLimit := frameLimit
			if desc.Kind == "flush-meta" {
				protocolLimit = input.Batch
			} else if desc.InsertOnly {
				var err error
				protocolLimit, err = colexec.InsertMetadataFrameLimit(unit, desc.TableCount)
				if err != nil {
					return nil, err
				}
			}
			meta, valid := multi_update.MetadataOutputBound(protocolLimit, desc.TableCount, desc.ColumnCount)
			if !valid {
				ok = false
				break
			}
			bound = memoryProducerBound{Rows: meta.Rows, Batch: meta.BatchBytes, Row: meta.RowBytes, Metadata: true, MetadataKind: "multi"}
			if desc.Kind == "flush-meta" {
				bound.Scratch = meta.ScratchBytes
			}
			for j, typ := range []types.Type{types.T_uint8.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_text.ToType()} {
				bound.Columns = append(bound.Columns, table_function.MemoryColumnBound{Type: typ, RowBytes: meta.ColumnRowBytes[j], BatchBytes: meta.ColumnBytes[j]})
			}
		case "lock":
			for _, target := range desc.LockTargets {
				if target.PrimaryColumn < 0 || int(target.PrimaryColumn) >= len(input.Columns) ||
					target.Type.Oid != input.Columns[target.PrimaryColumn].Type.Oid ||
					target.RefreshColumn < -1 || int(target.RefreshColumn) >= len(input.Columns) ||
					target.RefreshColumn >= 0 && input.Columns[target.RefreshColumn].Type.Oid != types.T_TS {
					return nil, moerr.NewInternalErrorNoCtxf("producer %d lock target %+v is incompatible with input columns %+v", i, target, input.Columns)
				}
			}
			columns := make([]lockop.ColumnMemoryBound, len(input.Columns))
			for i, col := range input.Columns {
				columns[i] = lockop.ColumnMemoryBound{RowBytes: col.RowBytes, BatchBytes: col.BatchBytes}
			}
			bound.Scratch, ok = lockop.MemoryBoundForColumns(desc.LockTargets, columns, input.Rows)
		case "spool":
			if desc.Receivers == 0 || desc.Copies != max(uint32(2), desc.Receivers) {
				return nil, moerr.NewInternalErrorNoCtx("invalid spool slot count")
			}
			// Returned cache IDs retain at most unit bytes. During a clone the
			// leftover cache and full old/new vector storage coexist. Aggregate
			// source payload is charged once, with independent vector rounding.
			peak, valid := memoryProduct(input.Batch, 8)
			if !valid {
				ok = false
				break
			}
			peak, ok = memorySum(peak, int64(max(1, len(input.Columns)))*32768)
			if !ok {
				break
			}
			peak, ok = memorySum(peak, unit)
			if !ok {
				break
			}
			bound.Scratch, ok = memoryProduct(peak, int64(desc.Copies))
			if !ok {
				break
			}
			metadata, valid := pSpool.MemoryMetadataBound(desc.Receivers)
			if !valid {
				ok = false
				break
			}
			bound.Scratch, ok = memorySum(bound.Scratch, metadata)
		case "preinsert":
			owned := make(map[int32]bool, len(desc.OwnedCols))
			for _, col := range desc.OwnedCols {
				if col < 0 || int(col) >= len(input.Columns) {
					return nil, moerr.NewInternalErrorNoCtx("invalid preinsert owned column")
				}
				owned[col] = true
			}
			bound.Columns = append([]table_function.MemoryColumnBound(nil), input.Columns...)
			bound.Scratch = int64(len(input.Columns))*1024 + 32768
			for i, col := range input.Columns {
				if !col.Constant && !owned[int32(i)] {
					continue
				}
				peak, valid := table_function.MemoryBatchVectorCapacity(col.Type, col.RowBytes, col.BatchBytes, max(1, input.Rows))
				if !valid {
					ok = false
					break
				}
				bound.Scratch, ok = memorySum(bound.Scratch, peak)
				if !ok {
					break
				}
				bound.Columns[i].Constant = false
			}
		case "csv", "scan":
			if desc.Kind == "scan" && !finiteScanSchema(desc.Schema) {
				return nil, moerr.NewInternalErrorNoCtx("unbounded scan schema")
			}
			bound = memoryProducerBound{Rows: colexec.DefaultBatchSize}
			recordBytes := int64(config.LargestEntryLimit)
			if desc.RecordBytes > 0 {
				recordBytes = min(recordBytes, desc.RecordBytes)
			}
			for _, typ := range desc.Schema {
				row := int64(typ.TypeSize())
				if row <= 0 {
					ok = false
					break
				}
				if typ.IsVarlen() {
					factor := int64(1)
					switch typ.Oid {
					case types.T_array_float64:
						factor = 4
					case types.T_array_float32:
						factor = 2
					case types.T_json:
						factor = 16
					}
					payload, valid := memoryProduct(recordBytes, factor)
					if !valid {
						ok = false
						break
					}
					row, ok = memorySum(row, payload)
					if !ok {
						break
					}
				}
				col := table_function.MemoryColumnBound{Type: typ, RowBytes: row}
				// Schema-evolution/default readers can return constant vectors even
				// for fixed columns; PreInsert must reserve their possible flatten.
				col.Constant = desc.Kind == "scan"
				col.BatchBytes, ok = memoryProduct(row, int64(bound.Rows))
				if !ok {
					break
				}
				bound.Columns = append(bound.Columns, col)
				bound.Row, ok = memorySum(bound.Row, row)
				if !ok {
					break
				}
				bound.Batch, ok = memorySum(bound.Batch, col.BatchBytes)
				if !ok {
					break
				}
			}
			if desc.Kind == "csv" {
				// reader_csv tests typed capacities before appending a second row.
				// Only a singleton may exceed unit; raw limit bounds that singleton.
				row, err := external.MaxRowBytes(desc.Schema, recordBytes)
				if err != nil {
					return nil, err
				}
				bound.Row = min(bound.Row, row)
				bound.Batch = min(bound.Batch, max(unit, bound.Row))
				var minimumRow int64
				for _, col := range bound.Columns {
					minimumRow += int64(col.Type.TypeSize())
				}
				if minimumRow > 0 {
					bound.Rows = int(min(int64(colexec.DefaultBatchSize), max(int64(1), unit/minimumRow)))
				}
				for j := range bound.Columns {
					columnRows, valid := memoryProduct(bound.Columns[j].RowBytes, int64(bound.Rows))
					if !valid {
						ok = false
						break
					}
					// Multirow CSV output fits unit before append; a wider output
					// contains one singleton. An unrelated wide JSON column cannot
					// inflate the batch payload of this flat byte-string column.
					bound.Columns[j].BatchBytes = min(columnRows, bound.Batch, max(unit, bound.Columns[j].RowBytes))
				}
			} else {
				bound.Scratch, ok = memoryProduct(bound.Batch, 8)
				if ok {
					bound.Scratch, ok = memorySum(bound.Scratch, int64(len(bound.Columns))*32768)
				}
			}
		case "expression":
			if len(inputs) != 1 {
				ok = false
				break
			}
			bound, ok = expressionProducerBound(input, desc.Expressions, desc.Project)
		case "join":
			bound = memoryProducerBound{Rows: colexec.DefaultBatchSize}
			if len(inputs) != 2 {
				ok = false
				break
			}
			selected := [2]int64{}
			weights := [2]map[int32]int64{{}, {}}
			multiplicity := [2]int64{}
			for _, pos := range desc.ResultCols {
				if pos.Rel < 0 || pos.Rel > 1 || pos.Pos < 0 || int(pos.Pos) >= len(inputs[pos.Rel].Columns) {
					ok = false
					break
				}
				col := inputs[pos.Rel].Columns[pos.Pos]
				col.Constant = false // Join Union materializes the result vectors.
				col.BatchBytes, ok = memoryProduct(col.RowBytes, int64(bound.Rows))
				if !ok {
					break
				}
				bound.Columns = append(bound.Columns, col)
				bound.Row, ok = memorySum(bound.Row, col.RowBytes)
				if !ok {
					break
				}
				selected[pos.Rel], ok = memorySum(selected[pos.Rel], col.RowBytes)
				if !ok {
					break
				}
				weights[pos.Rel][pos.Pos]++
				multiplicity[pos.Rel] = max(multiplicity[pos.Rel], weights[pos.Rel][pos.Pos])
				bound.Batch, ok = memorySum(bound.Batch, col.BatchBytes)
				if !ok {
					break
				}
			}
			if !ok {
				break
			}
			bound.Row = 0
			for side := range selected {
				correlated, valid := memoryProduct(inputs[side].Row, multiplicity[side])
				if !valid {
					ok = false
					break
				}
				bound.Row, ok = memorySum(bound.Row, min(selected[side], correlated))
				if !ok {
					break
				}
			}
			if !ok {
				break
			}
			// Normal and skew join output stop at the common byte unit, with a
			// legal singleton exception; the final appended row can cross it.
			ceiling, valid := memorySum(unit, bound.Row)
			if !valid {
				ok = false
				break
			}
			bound.Batch = min(bound.Batch, ceiling)
			var minimumRow int64
			for _, col := range bound.Columns {
				minimumRow += int64(col.Type.TypeSize())
			}
			if minimumRow > 0 {
				bound.Rows = int(min(int64(colexec.DefaultBatchSize-1), unit/minimumRow) + 1)
			}
			for j := range bound.Columns {
				bound.Columns[j].BatchBytes = min(bound.Columns[j].BatchBytes, bound.Batch)
			}
		case "apply":
			if len(inputs) != 1 || desc.Function == nil {
				ok = false
				break
			}
			fn := desc.Function
			args := fn.Args
			argBound, valid := expressionProducerBound(input, fn.Args, true)
			if !valid {
				return nil, moerr.NewInternalErrorNoCtxf("apply producer %d argument contract failed: input rows=%d columns=%d expressions=%v", i, input.Rows, len(input.Columns), args)
			}
			tf := table_function.TableFunction{FuncName: fn.Name, Params: fn.Params, Args: args, Rets: fn.Rets}
			f, valid := tf.MemoryBound(argBound.Columns, argBound.Row, unit)
			if !valid {
				return nil, &memoryEnvelopeCapacityError{fmt.Sprintf("apply producer %d tokenizer allocation contract failed: arguments=%v row=%d unit=%d", i, argBound.Columns, argBound.Row, unit)}
			}
			bound = memoryProducerBound{Rows: 2*colexec.DefaultBatchSize - 1}
			bound.Scratch, ok = memorySum(f.Bytes, argBound.Scratch)
			if !ok {
				break
			}
			for _, pos := range desc.ResultCols {
				// Apply's constructor preserves planner binding tags. Its actual
				// consumer selects the input only for Rel==0 and the function
				// result for every other tag, including tags above one.
				cols := f.Columns
				if pos.Rel == 0 {
					cols = input.Columns
				}
				if pos.Pos < 0 || int(pos.Pos) >= len(cols) {
					return nil, moerr.NewInternalErrorNoCtxf("apply producer %d invalid output column: relation=%d position=%d columns=%d", i, pos.Rel, pos.Pos, len(cols))
				}
				col := cols[pos.Pos]
				col.BatchBytes, ok = memoryProduct(col.RowBytes, int64(bound.Rows))
				if !ok {
					break
				}
				bound.Columns = append(bound.Columns, col)
				bound.Row, ok = memorySum(bound.Row, col.RowBytes)
				if !ok {
					break
				}
				bound.Batch, ok = memorySum(bound.Batch, col.BatchBytes)
				if !ok {
					break
				}
			}
			if !ok {
				break
			}
			var minimumRow int64
			for _, col := range bound.Columns {
				minimumRow += int64(col.Type.TypeSize())
			}
			if minimumRow > 0 {
				halfRows := min(int64(colexec.DefaultBatchSize), max(int64(1), unit/minimumRow))
				bound.Rows = int(2*halfRows - 1)
			}
			// A prior partial is below unit; the next TVF chunk obeys the same
			// unit or contains one legal wide row. Both remain live while copied.
			ceiling, valid := memorySum(unit, max(unit, bound.Row))
			if !valid {
				ok = false
				break
			}
			bound.Batch = min(bound.Batch, ceiling)
			for j := range bound.Columns {
				col := &bound.Columns[j]
				rowBytes, valid := memoryProduct(col.RowBytes, int64(bound.Rows))
				if !valid {
					ok = false
					break
				}
				col.BatchBytes = min(rowBytes, bound.Batch)
				peak, valid := table_function.MemoryBatchVectorCapacity(col.Type, col.RowBytes, bound.Batch, bound.Rows)
				if !valid {
					ok = false
					break
				}
				bound.Scratch, ok = memorySum(bound.Scratch, peak)
				if !ok {
					break
				}
			}
			// Apply keeps one int32 selection per tokenizer chunk.
			if !ok {
				break
			}
			bound.Scratch, ok = memorySum(bound.Scratch, 4*4*colexec.DefaultBatchSize+16384)
		default:
			return nil, moerr.NewInternalErrorNoCtx("unknown memory producer")
		}
		if !ok || bound.Batch < 0 || bound.Row < 0 || bound.Scratch < 0 {
			return nil, &memoryEnvelopeCapacityError{fmt.Sprintf("producer %d (%s) allocation contract overflows or has invalid input: rows=%d batch=%d row=%d columns=%d", i, desc.Kind, input.Rows, input.Batch, input.Row, len(input.Columns))}
		}
		result[i] = bound
	}
	return result, nil
}

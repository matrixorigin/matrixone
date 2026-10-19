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

package table_function

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	maxViewDescriptionsPerScan = 65536
	viewColumnsBatchRows       = 1024
	viewColumnsBatchBytes      = 1 << 20
	viewColumnsBatchHardBytes  = 16 << 20
)

// Only catalog/column lookup failures prove that a previously valid View
// definition has become invalid. Other errors may indicate protocol, resource,
// permission, cancellation or compiler failures and must remain fatal.
func skippableViewDescriptionError(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrNoDB) ||
		moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) ||
		moerr.IsMoErrCode(err, moerr.ErrBadFieldError) ||
		moerr.IsMoErrCode(err, moerr.ErrBadDB)
}

type viewColumnsState struct {
	simpleOneBatchState
	described uint64
	derived   bool
	columns   []*planpb.ColDef
	next      int
	result    *plan.ViewSchemaResult
}

func (s *viewColumnsState) releaseColumns() {
	s.columns, s.next = nil, 0
	s.result.Release()
	s.result = nil
}

func (s *viewColumnsState) reset(tf *TableFunction, proc *process.Process) {
	s.releaseColumns()
	s.simpleOneBatchState.reset(tf, proc)
	s.described = 0
	s.derived = false
}

func (s *viewColumnsState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	s.releaseColumns()
	s.simpleOneBatchState.free(tf, proc, pipelineFailed, err)
	s.batch = nil
}

func (s *viewColumnsState) end(_ *TableFunction, _ *process.Process) error {
	s.releaseColumns()
	return nil
}

func viewColumnsPrepare(proc *process.Process, tf *TableFunction) (tvfState, error) {
	state := &viewColumnsState{}
	var err error
	tf.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tf.Args)
	if err != nil {
		return state, err
	}
	tf.ctr.argVecs = make([]*vector.Vector, len(tf.Args))
	return state, nil
}

func (s *viewColumnsState) start(tf *TableFunction, proc *process.Process, nthRow int, _ process.Analyzer) (err error) {
	s.releaseColumns()
	defer func() {
		if err != nil {
			s.releaseColumns()
		}
	}()
	s.startPreamble(tf, proc, nthRow)
	if err := proc.Ctx.Err(); err != nil {
		return err
	}
	s.derived = len(tf.ctr.argVecs) == 3
	if s.derived {
		return s.startDerived(tf, proc, nthRow)
	}
	if plan.HasViewMetadataRequest(proc.Ctx) || (len(tf.ctr.argVecs) != 1 && len(tf.ctr.argVecs) != 2) {
		return moerr.NewInvalidInput(proc.Ctx, "invalid View column metadata arguments")
	}
	idArgs := vector.GenerateFunctionFixedTypeParameter[uint64](tf.ctr.argVecs[len(tf.ctr.argVecs)-1])
	id, isNull := idArgs.GetValue(uint64(nthRow))
	if isNull {
		return nil
	}
	compilerValue := proc.GetSessionInfo().CompilerContext
	if helper := proc.GetSessionInfo().SqlHelper; compilerValue == nil && helper != nil {
		compilerValue = helper.GetCompilerContext()
	}
	provider, ok := compilerValue.(plan.ViewDescriptionContextProvider)
	if !ok || provider == nil {
		return moerr.NewNotSupported(proc.Ctx, "View column description requires an isolated compiler context")
	}
	compiler, closeCompiler, err := provider.NewViewDescriptionCompilerContext(proc.Ctx)
	if err != nil {
		return err
	}
	defer closeCompiler()
	if tf.ScanSnapshot != nil {
		compiler.SetSnapshot(tf.ScanSnapshot)
	}
	var def *planpb.TableDef
	if len(tf.ctr.argVecs) == 2 {
		publisherArgs := vector.GenerateFunctionFixedTypeParameter[uint32](tf.ctr.argVecs[0])
		publisherID, publisherNull := publisherArgs.GetValue(uint64(nthRow))
		if publisherNull {
			return nil
		}
		compiler.SetContext(defines.AttachAccountId(compiler.GetContext(), publisherID))
		sub := &planpb.SubscriptionMeta{AccountId: int32(publisherID), Tables: pubsub.TableAll}
		var obj *planpb.ObjectRef
		obj, def, err = compiler.ResolveSubscriptionTableById(id, sub)
		if err == nil && obj != nil {
			sub.DbName = obj.SchemaName
			compiler.SetQueryingSubscription(sub)
		}
	} else {
		_, def, err = compiler.ResolveById(id, tf.ScanSnapshot)
	}
	if err != nil {
		return err
	}
	if def == nil || def.ViewSql == nil {
		return moerr.NewNoSuchTable(proc.Ctx, "", "View metadata object")
	}
	if s.described >= maxViewDescriptionsPerScan {
		return moerr.NewInternalError(proc.Ctx, "View metadata scan exceeds its description budget")
	}
	s.described++
	columns, err := plan.DescribeViewColumns(compiler, def.ViewSql.View)
	if err != nil {
		// An invalid source makes this View unqueryable, but must not hide
		// other valid rows in a batch I_S metadata scan. Do not downgrade
		// cancellation, rollout gates or unexpected compiler failures.
		if ctxErr := proc.Ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if !skippableViewDescriptionError(err) {
			return err
		}
		process.AppendWarningBatch(proc, 1, []uint16{moerr.ER_VIEW_INVALID}, []string{
			fmt.Sprintf("View '%s.%s' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them",
				def.DbName, def.Name),
		})
		return nil
	}
	if len(columns) > plan.MaxViewMetadataColumns {
		return moerr.NewInternalError(proc.Ctx, "View metadata exceeds its column budget")
	}
	s.columns = columns
	return nil
}

// ApplyPreserveBatch prevents APPLY from coalescing many large metadata pages
// merely because their row count is low. Only the opt-in signature needs it;
// the capability is reconstructed from existing arguments, not a wire flag.
func (tableFunction *TableFunction) ApplyPreserveBatch() bool {
	return (tableFunction.FuncName == plan.ViewColumnsFunctionName || tableFunction.FuncName == plan.SubscriptionViewColumnsFunctionName) && len(tableFunction.Args) == 3
}

func (s *viewColumnsState) startDerived(tf *TableFunction, proc *process.Process, nthRow int) error {
	for _, arg := range tf.ctr.argVecs {
		if arg.IsNull(uint64(nthRow)) {
			return nil
		}
	}
	database := tf.ctr.argVecs[0].GetStringAt(nthRow)
	name := tf.ctr.argVecs[1].GetStringAt(nthRow)
	ids := vector.GenerateFunctionFixedTypeParameter[uint64](tf.ctr.argVecs[2])
	id, _ := ids.GetValue(uint64(nthRow))
	result, invalid, err := plan.DescribeViewMetadataCandidate(proc.Ctx, database, name, tf.ScanSnapshot)
	if err != nil {
		if cause := proc.Ctx.Err(); cause != nil {
			return cause
		}
		if !invalid {
			return err
		}
		process.AppendWarningBatch(proc, 1, []uint16{moerr.ER_VIEW_INVALID}, []string{
			fmt.Sprintf("View '%s.%s' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them", database, name),
		})
		return nil
	}
	s.result = result
	dependencies, err := result.Dependencies()
	if err != nil {
		return err
	}
	matched := false
	for _, dep := range dependencies {
		if dep.RelationID == id && dep.BindingDatabaseName == database && dep.BindingRelationName == name {
			matched = true
			break
		}
	}
	if !matched {
		return plan.ErrViewSchemaChanged
	}
	s.columns, err = result.Columns()
	return err
}

// call emits a bounded page of an already completed description. Derivation is
// once per candidate, never once per output column. No terminal pipeline signal
// is sent here: Reset/Free keep their existing ownership.
func (s *viewColumnsState) call(tf *TableFunction, proc *process.Process) (result vm.CallResult, err error) {
	result = vm.CancelResult
	defer func() {
		if err != nil {
			s.releaseColumns()
			if s.batch != nil {
				s.batch.CleanOnlyData()
			}
		}
	}()
	if err = proc.Ctx.Err(); err != nil {
		return result, err
	}
	if s.next >= len(s.columns) {
		s.releaseColumns()
		return result, nil
	}
	if s.batch == nil {
		s.batch = tf.createResultBatch()
	} else {
		s.batch.CleanOnlyData()
	}
	rows, bytes := 0, 0
	for s.next < len(s.columns) && (!s.derived || rows < viewColumnsBatchRows) {
		if err = proc.Ctx.Err(); err != nil {
			return result, err
		}
		col := s.columns[s.next]
		if col == nil || (s.derived && col.ProtoSize() > viewColumnsBatchHardBytes) {
			return result, plan.ErrViewSchemaLimit
		}
		typ := plan.MakeTypeByPlan2Type(col.Typ)
		typeBytes, encodeErr := types.Encode(&typ)
		if encodeErr != nil {
			return result, encodeErr
		}
		defaultDef := col.Default
		if defaultDef == nil {
			defaultDef = &planpb.Default{NullAbility: !col.Typ.NotNullable}
		}
		defaultBytes, encodeErr := types.Encode(defaultDef)
		if encodeErr != nil {
			return result, encodeErr
		}
		// Include vector slots and all projected variable-width payloads. This
		// is a conservative output-size bound, not a claim about total Go RSS.
		rowBytes := 0
		for _, attr := range tf.Attrs {
			rowBytes += 32
			switch attr {
			case "attname":
				rowBytes += len(col.GetOriginCaseName())
			case "atttyp":
				rowBytes += len(typeBytes)
			case "att_default":
				rowBytes += len(defaultBytes)
			case "attr_enum":
				rowBytes += len(col.Typ.Enumvalues)
			}
		}
		if s.derived && rowBytes > viewColumnsBatchHardBytes {
			return result, plan.ErrViewSchemaLimit
		}
		if s.derived && rows > 0 && rowBytes > viewColumnsBatchBytes-bytes {
			break
		}
		for j, attr := range tf.Attrs {
			v := s.batch.Vecs[j]
			switch attr {
			case "attnum":
				err = vector.AppendFixed(v, int32(s.next+1), false, proc.Mp())
			case "attnotnull":
				var notNull int8
				if col.Typ.NotNullable {
					notNull = 1
				}
				err = vector.AppendFixed(v, notNull, false, proc.Mp())
			case "att_is_auto_increment", "attr_has_generated", "att_is_hidden":
				err = vector.AppendFixed(v, int8(0), false, proc.Mp())
			case "attname":
				err = vector.AppendBytes(v, []byte(col.GetOriginCaseName()), false, proc.Mp())
			case "atttyp":
				err = vector.AppendBytes(v, typeBytes, false, proc.Mp())
			case "att_default":
				err = vector.AppendBytes(v, defaultBytes, false, proc.Mp())
			case "attr_enum":
				err = vector.AppendBytes(v, []byte(col.Typ.Enumvalues), false, proc.Mp())
			case "attr_generated":
				err = vector.AppendBytes(v, nil, true, proc.Mp())
			case "att_constraint_type", "att_comment":
				err = vector.AppendBytes(v, nil, false, proc.Mp())
			default:
				err = moerr.NewInternalErrorf(proc.Ctx, "unknown View column metadata field %s", attr)
			}
			if err != nil {
				return result, err
			}
		}
		bytes += rowBytes
		rows++
		s.next++
	}
	s.batch.SetRowCount(rows)
	if s.next == len(s.columns) {
		s.releaseColumns()
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: s.batch}, nil
}

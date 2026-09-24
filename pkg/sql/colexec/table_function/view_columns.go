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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const maxViewDescriptionsPerScan = 65536

type viewColumnsState struct {
	simpleOneBatchState
	described uint64
}

func (s *viewColumnsState) reset(tf *TableFunction, proc *process.Process) {
	s.simpleOneBatchState.reset(tf, proc)
	s.described = 0
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

func (s *viewColumnsState) start(tf *TableFunction, proc *process.Process, nthRow int, _ process.Analyzer) error {
	s.startPreamble(tf, proc, nthRow)
	if err := proc.Ctx.Err(); err != nil {
		return err
	}
	if len(tf.ctr.argVecs) != 1 && len(tf.ctr.argVecs) != 2 {
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
		return err
	}
	if len(columns) > plan.MaxViewMetadataColumns {
		return moerr.NewInternalError(proc.Ctx, "View metadata exceeds its column budget")
	}
	for i, col := range columns {
		if err := proc.Ctx.Err(); err != nil {
			return err
		}
		typ := plan.MakeTypeByPlan2Type(col.Typ)
		typeBytes, err := types.Encode(&typ)
		if err != nil {
			return err
		}
		defaultDef := col.Default
		if defaultDef == nil {
			defaultDef = &planpb.Default{NullAbility: !col.Typ.NotNullable}
		}
		defaultBytes, err := types.Encode(defaultDef)
		if err != nil {
			return err
		}
		for j, attr := range tf.Attrs {
			v := s.batch.Vecs[j]
			switch attr {
			case "attnum":
				err = vector.AppendFixed(v, int32(i+1), false, proc.Mp())
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
				return moerr.NewInternalErrorf(proc.Ctx, "unknown View column metadata field %s", attr)
			}
			if err != nil {
				return err
			}
		}
	}
	s.batch.SetRowCount(len(columns))
	return nil
}

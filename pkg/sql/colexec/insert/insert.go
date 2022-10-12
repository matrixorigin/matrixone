// Copyright 2021 Matrix Origin
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

package insert

import (
	"bytes"
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/sql/util"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Argument struct {
	Ts            uint64
	TargetTable   engine.Relation
	TargetColDefs []*plan.ColDef
	Affected      uint64
	Engine        engine.Engine
	DB            engine.Database
	TableID       string
	CPkeyColDef   *plan.ColDef
}

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("insert select")
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

func Call(_ int, proc *process.Process, arg any) (bool, error) {
	n := arg.(*Argument)
	bat := proc.Reg.InputBatch
	if bat == nil {
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	defer bat.Clean(proc.Mp())
	{
		// do null value check
		for i := range bat.Vecs {
			if n.TargetColDefs[i].Primary && !n.TargetColDefs[i].AutoIncrement {
				if nulls.Any(bat.Vecs[i].Nsp) {
					return false, moerr.NewConstraintViolation(fmt.Sprintf("Column '%s' cannot be null", n.TargetColDefs[i].GetName()))
				}
			}
		}
	}
	{
		bat.Ro = false
		bat.Attrs = make([]string, len(bat.Vecs))
		// scalar vector's extension
		for i := range bat.Vecs {
			bat.Attrs[i] = n.TargetColDefs[i].GetName()
			bat.Vecs[i] = bat.Vecs[i].ConstExpand(proc.Mp())
		}
	}
	ctx := context.TODO()
	if err := colexec.UpdateInsertBatch(n.Engine, n.DB, ctx, proc, n.TargetColDefs, bat, n.TableID); err != nil {
		return false, err
	}
	if n.CPkeyColDef != nil {
		err := util.FillCompositePKeyBatch(bat, n.CPkeyColDef, proc)
		if err != nil {
			names := util.SplitCompositePrimaryKeyColumnName(n.CPkeyColDef.Name)
			for _, name := range names {
				for i := range bat.Vecs {
					if n.TargetColDefs[i].Name == name {
						if nulls.Any(bat.Vecs[i].Nsp) {
							return false, moerr.NewConstraintViolation(fmt.Sprintf("Column '%s' cannot be null", n.TargetColDefs[i].GetName()))
						}
					}
				}
			}

		}
	}
	// XXX The original logic was buggy and I had to temporarily circumvent it
	if bat.Length() == 0 {
		bat.SetZs(bat.GetVector(0).Length(), proc.Mp())
	}
	err := n.TargetTable.Write(ctx, bat)
	n.Affected += uint64(len(bat.Zs))
	return false, err
}

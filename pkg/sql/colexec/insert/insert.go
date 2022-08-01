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

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Argument struct {
	Ts            uint64
	TargetTable   engine.Relation
	TargetColDefs []*plan.ColDef
	Affected      uint64
}

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("insert select")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(_ int, proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	bat := proc.Reg.InputBatch
	if bat == nil {
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	defer bat.Clean(proc.Mp)
	{
		// do null value check
		for i := range bat.Vecs {
			if n.TargetColDefs[i].Primary {
				if nulls.Any(bat.Vecs[i].Nsp) {
					return false, errors.New(errno.IntegrityConstraintViolation,
						fmt.Sprintf("Column '%s' cannot be null", n.TargetColDefs[i].GetName()))
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
			if bat.Vecs[i].IsScalarNull() {
				if bat.Vecs[i].Typ.Oid == types.T_any {
					bat.Vecs[i].Typ.Oid = types.T(n.TargetColDefs[i].Typ.GetId())
				}
				switch bat.Vecs[i].Typ.Oid {
				case types.T_char, types.T_varchar, types.T_blob, types.T_json:
					bat.Vecs[i].Col = &types.Bytes{
						Data:    nil,
						Offsets: make([]uint32, len(bat.Zs)),
						Lengths: make([]uint32, len(bat.Zs)),
					}
				default:
					vector.PreAlloc(bat.Vecs[i], bat.Vecs[i], bat.Vecs[i].Length, proc.Mp)
				}
				vector.SetVectorLength(bat.Vecs[i], bat.Vecs[i].Length)
			}
			bat.Vecs[i] = bat.Vecs[i].ConstExpand(proc.Mp)
		}
	}
	ctx := context.TODO()
	err := n.TargetTable.Write(ctx, bat)
	n.Affected += uint64(len(bat.Zs))
	return false, err
}

// Copyright 2023 Matrix Origin
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

package lockop

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// NewArgument create new argument
func NewArgument(
	tableID uint64,
	tableName string,
	pkIndx int32,
	pkType types.Type,
	mode lock.LockMode,
	svc lockservice.LockService) *Argument {
	arg := argPool.Get().(*Argument)
	arg.tableID = tableID
	arg.tabaleName = tableName
	arg.pkIdx = pkIndx
	arg.mode = mode
	arg.svc = svc
	return arg
}

var (
	argPool = sync.Pool{
		New: func() any {
			return &Argument{}
		},
	}
)

func (arg *Argument) Free(
	proc *process.Process,
	pipelineFailed bool) {
	arg.reset()
	argPool.Put(arg)
}

func (arg *Argument) reset() {
	arg.packer.Reset()
	arg.packer.FreeMem()
	*arg = Argument{}
}

func String(
	v any,
	buf *bytes.Buffer) {
	arg := v.(*Argument)
	buf.WriteString(fmt.Sprintf("lock-%s(%d)",
		arg.tabaleName,
		arg.tableID))
}

func Prepare(
	proc *process.Process,
	v any) error {
	arg := v.(*Argument)
	arg.fetcher = getFetchRowsFunc(arg.pkType)
	arg.packer = types.NewPacker(proc.Mp())
	return nil
}

// Call add primary key to lockservice, blcoked if conflict encountered.
func Call(
	idx int,
	proc *process.Process,
	v any,
	isFirst bool,
	isLast bool) (bool, error) {
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}

	arg := v.(*Argument)
	s := arg.svc
	vec := bat.GetVector(arg.pkIdx)
	rows, g := arg.fetcher(vec, arg.packer, 0)
	txnOp := proc.TxnOperator
	bind, err := s.Lock(
		proc.Ctx,
		arg.tableID,
		rows,
		txnOp.Txn().ID,
		lock.LockOptions{
			Granularity: g,
			Policy:      lock.WaitPolicy_Wait,
			Mode:        arg.mode,
		})
	if err != nil {
		return false, err
	}
	if err := txnOp.AddLockTable(bind); err != nil {
		return false, err
	}
	return false, nil
}

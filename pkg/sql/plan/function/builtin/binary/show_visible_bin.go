// Copyright 2022 Matrix Origin
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

package binary

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	onUpdateExpr = iota
	defaultExpr
	typNormal
	typWithLen
)

func ShowVisibleBin(vec []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if !vec[1].IsScalar() {
		return nil, moerr.NewInvalidInput("show visible bin, the second argument must be a scalar")
	}
	var err error
	tpSlice := vector.MustTCols[uint8](vec[1])
	srcSlice := vector.MustBytesCols(vec[0])
	tp := tpSlice[0]
	resultVec := vector.New(types.T_varchar.ToType())
	defer func() {
		if err != nil {
			resultVec.Free(proc.Mp())
		}
	}()
	var ret []string
	switch tp {
	case onUpdateExpr:
		ret, err = showExpr(srcSlice, byOnUpdate)
	case defaultExpr:
		ret, err = showExpr(srcSlice, byDefault)
	case typNormal:
		ret, err = showType(srcSlice, false)
	case typWithLen:
		ret, err = showType(srcSlice, true)
	default:
		return nil, moerr.NewNotSupported(fmt.Sprintf("show visible bin, the second argument must be in [0, 3], but got %d", tp))
	}
	if err != nil {
		return nil, err
	}
	for i := range ret {
		err = resultVec.Append([]byte(ret[i]), len(ret[i]) == 0, proc.Mp())
		if err != nil {
			return nil, err
		}
	}
	return resultVec, nil
}

func showType(s [][]byte, showLen bool) ([]string, error) {
	ret := make([]string, len(s))
	for i := range s {
		tp := new(types.Type)
		err := types.Decode(s[i], tp)
		if err != nil {
			return nil, err
		}
		if showLen {
			ret[i] = fmt.Sprintf("%s(%d)", tp.String(), tp.Width)
		} else {
			ret[i] = tp.String()
		}
	}
	return ret, nil
}

func byOnUpdate(s []byte) (string, error) {
	if len(s) == 0 {
		return "", nil
	}
	update := new(plan.OnUpdate)
	err := types.Decode(s, update)
	if err != nil {
		return "", err
	}
	return update.OriginString, nil
}
func byDefault(s []byte) (string, error) {
	if len(s) == 0 {
		return "", nil
	}
	def := new(plan.Default)
	err := types.Decode(s, def)
	if err != nil {
		return "", err
	}
	return def.OriginString, nil
}

func showExpr(s [][]byte, fn func(ss []byte) (string, error)) ([]string, error) {
	ret := make([]string, len(s))
	var err error
	for i := range s {
		ret[i], err = fn(s[i])
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

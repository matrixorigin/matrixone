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
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"io"
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
		ret = showType(srcSlice, false)
	case typWithLen:
		ret = showType(srcSlice, true)
	default:
		return nil, moerr.NewNotSupported(fmt.Sprintf("show visible bin, the second argument must be in [0, 3], but got %d", tp))
	}
	if err != nil {
		return nil, err
	}
	for i := range ret {
		err = resultVec.Append(ret[i], len(ret[i]) == 0, proc.Mp())
		if err != nil {
			return nil, err
		}
	}
	return resultVec, nil
}

func showType(s [][]byte, showLen bool) []string {
	ret := make([]string, len(s))
	for i, v := range s {
		tp := types.DecodeType(v)
		if showLen {
			ret[i] = fmt.Sprintf("%s(%d)", tp.String(), tp.Width)
		} else {
			ret[i] = tp.String()
		}
	}
	return ret
}

func byOnUpdate(r io.Reader) (string, error) {
	tmp := catalog.OnUpdate{}
	_, err := catalog.UnMarshalOnUpdate(r, &tmp)
	if err != nil {
		return "", err
	}
	return tmp.OriginString, nil
}
func byDefault(r io.Reader) (string, error) {
	tmp := catalog.Default{}
	_, err := catalog.UnMarshalDefault(r, &tmp)
	if err != nil {
		return "", err
	}
	return tmp.OriginString, nil
}

func showExpr(s [][]byte, fn func(r io.Reader) (string, error)) ([]string, error) {
	ret := make([]string, len(s))
	r := bytes.NewBuffer(nil)
	var err error
	for i, v := range s {
		r.Reset()
		r.Write(v)
		ret[i], err = fn(r)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

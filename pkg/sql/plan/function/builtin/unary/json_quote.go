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

package unary

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/json_quote"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func JsonQuote(ivecs []*vector.Vector, proc *process.Process) (ret *vector.Vector, err error) {
	vec := ivecs[0]
	defer func() {
		if err != nil && ret != nil {
			ret.Free(proc.Mp())
		}
	}()
	rtyp := types.T_json.ToType()
	if vec.IsConstNull() {
		ret = vector.NewConstNull(rtyp, vec.Length(), proc.Mp())
		return
	}
	vs := vector.MustStrCol(vec)
	if vec.IsConst() {
		var dt []byte
		dt, err = json_quote.Single(vs[0])
		if err != nil {
			return
		}
		ret = vector.NewConstBytes(rtyp, dt, vec.Length(), proc.Mp())
		return
	}
	ret, err = proc.AllocVectorOfRows(rtyp, vec.Length(), vec.GetNulls())
	if err != nil {
		return
	}
	rs := vector.MustBytesCol(ret)
	rs, err = json_quote.Batch(vs, rs, ret.GetNulls())
	if err != nil {
		return
	}
	for i := 0; i < len(rs); i++ {
		err = vector.SetBytesAt(ret, i, rs[i], proc.Mp())
		if err != nil {
			return
		}
	}
	return
}

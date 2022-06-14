// Copyright 2021 - 2022 Matrix Origin
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
package multi

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/unixtimestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func UnixTimestamp(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := lv[0]
	times := inVec.Col.([]types.Datetime)
	size := types.T(types.T_int64).TypeLen()

	vec := proc.AllocScalarVector(types.Type{Oid: types.T_int64, Size: int32(size)})
	rs := make([]int64, len(times))
	for i := 0; i < len(times); i++ {
		if inVec.Nsp.Np == nil {
			inVec.Nsp.Np = &roaring64.Bitmap{}
		}
		if times[i] < 0 {
			inVec.Nsp.Np.AddInt(i)
		}
	}
	nulls.Set(vec.Nsp, inVec.Nsp)
	vector.SetCol(vec, unixtimestamp.UnixTimestamp(times, rs))
	return vec, nil
}

func UnixTimestampVarchar(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := lv[0]
	times_ := inVec.Col.(*types.Bytes)

	times := []types.Datetime{MustDatetimeMe(string(times_.Data))}
	size := types.T(types.T_int64).TypeLen()

	vec := proc.AllocScalarVector(types.Type{Oid: types.T_int64, Size: int32(size)})
	rs := make([]int64, len(times))
	nulls.Set(vec.Nsp, inVec.Nsp)
	vector.SetCol(vec, unixtimestamp.UnixTimestamp(times, rs))
	return vec, nil
}

func MustDatetimeMe(s string) types.Datetime {
	datetime, err := types.ParseDatetime(s)
	if err != nil {
		panic("bad datetime")
	}
	return datetime
}

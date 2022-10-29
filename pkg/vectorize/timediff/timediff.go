// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package timediff

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type DiffT interface {
	types.Time | types.Datetime
}

func TimeDiffWithTimeFn[T DiffT](v1, v2 []T, v1N, v2N *nulls.Nulls, resultVector *vector.Vector, proc *process.Process, vectorLen int) error {
	rs := make([]string, vectorLen)
	if len(v1) == 1 && len(v2) == 1 {
		for i := 0; i < vectorLen; i++ {
			if nulls.Contains(v1N, uint64(0)) || nulls.Contains(v2N, uint64(0)) {
				nulls.Add(resultVector.Nsp, uint64(0))
				continue
			}
			res, err := timeDiff(v1[0], v2[0])
			if err != nil {
				return err
			}
			rs[0] = res
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	} else if len(v1) == 1 {
		for i := 0; i < vectorLen; i++ {
			if nulls.Contains(v1N, uint64(0)) || nulls.Contains(v2N, uint64(i)) {
				nulls.Add(resultVector.Nsp, uint64(0))
				continue
			}
			res, err := timeDiff(v1[0], v2[i])
			if err != nil {
				return err
			}
			rs[i] = res
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	} else if len(v2) == 1 {
		for i := 0; i < vectorLen; i++ {
			if nulls.Contains(v1N, uint64(i)) || nulls.Contains(v2N, uint64(0)) {
				nulls.Add(resultVector.Nsp, uint64(0))
				continue
			}
			res, err := timeDiff(v1[i], v2[0])
			if err != nil {
				return err
			}
			rs[i] = res
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	} else {
		for i := 0; i < vectorLen; i++ {
			if nulls.Contains(v1N, uint64(i)) || nulls.Contains(v2N, uint64(i)) {
				nulls.Add(resultVector.Nsp, uint64(0))
				continue
			}
			res, err := timeDiff(v1[i], v2[i])
			if err != nil {
				return err
			}
			rs[i] = res
		}
		vector.AppendString(resultVector, rs, proc.Mp())
	}
	return nil
}

func timeDiff[T DiffT](v1, v2 T) (string, error) {
	time := types.Time(int(v1) - int(v2))
	hour, _, _, _, isNeg := time.ClockFormat()
	if !types.ValidTime(uint64(hour), 0, 0) {
		if isNeg {
			return "-838:59:59", nil
		} else {
			return "838:59:59", nil
		}
	}

	return time.String(), nil
}

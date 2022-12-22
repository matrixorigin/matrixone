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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math/rand"
)

func BuiltInRand(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := result.(*vector.FunctionResult[float64])
	// IF one parameter, use it as the seed.
	if len(parameters) == 1 {
		var i uint64
		num := vector.GenerateFunctionFixedTypeParameter[int64](parameters[0])
		seedMap := make(map[int64]rand.Source)
		for i = 0; i < uint64(length); i++ {
			seed, null := num.GetValue(i)
			if null {
				seed = 0
			}
			var f float64
			if source, ok := seedMap[seed]; ok {
				f = generateFloat64(source)
				seedMap[seed] = source
			} else {
				source = rand.NewSource(seed)
				f = generateFloat64(source)
				seedMap[seed] = source
			}
			err := rs.Append(f, false)
			if err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < length; i++ {
			err := rs.Append(rand.Float64(), false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func generateFloat64(source rand.Source) float64 {
	for {
		f := float64(source.Int63()) / (1 << 63)
		if f == 1 {
			continue // resample; this branch is taken O(never)
		}
		return f
	}
}

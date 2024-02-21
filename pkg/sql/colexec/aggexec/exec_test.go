// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"testing"
)

type testSingleAggPrivate1 struct{}

func (t *testSingleAggPrivate1) init()                                                             {}
func (t *testSingleAggPrivate1) fill(from int32, getter aggGetter[int64], setter aggSetter[int64]) {}
func (t *testSingleAggPrivate1) fillNull(getter aggGetter[int64], setter aggSetter[int64])         {}
func (t *testSingleAggPrivate1) fills(value int32, isNull bool, count int, getter aggGetter[int64], setter aggSetter[int64]) {
}
func (t *testSingleAggPrivate1) flush(getter aggGetter[int64], setter aggSetter[int64]) {}

func TestSingleAggFuncExec1(t *testing.T) {
	executor := newSingleAggFuncExec1(types.T_int32.ToType(), types.T_int64.ToType()).(*singleAggFuncExec1[int32, int64])
	_ = executor
}

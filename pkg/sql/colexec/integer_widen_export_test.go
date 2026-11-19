// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ObserveIntegerWideningCastForTest lets the external binder test inspect the
// selection at the real cast boundary without a production hook or counter.
func ObserveIntegerWideningCastForTest(executor ExpressionExecutor, observe func(*function.FunctionSelectList)) {
	cast := executor.(*FunctionExpressionExecutor).parameterExecutor[0].(*FunctionExpressionExecutor)
	original := cast.evalFn
	cast.evalFn = func(p []*vector.Vector, r vector.FunctionResultWrapper, proc *process.Process, n int, s *function.FunctionSelectList) error {
		observe(s)
		return original(p, r, proc, n, s)
	}
}

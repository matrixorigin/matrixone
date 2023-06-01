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

package order

//type evalVector struct {
//	vec      *vector.Vector
//	executor colexec.ExpressionExecutor
//}
//
//type container struct {
//	desc      []bool // ds[i] == true: the attrs[i] are in descending order
//	nullsLast []bool
//	vecs      []evalVector // sorted list of attributes
//}
//
//type Argument struct {
//	ctr *container
//	Fs  []*plan.OrderBySpec
//}
//
//func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
//	ctr := arg.ctr
//	if ctr != nil {
//		mp := proc.Mp()
//		ctr.cleanEvalVectors(mp)
//	}
//}
//
//func (ctr *container) cleanEvalVectors(mp *mpool.MPool) {
//	for i := range ctr.vecs {
//		ctr.vecs[i].executor.Free()
//	}
//}

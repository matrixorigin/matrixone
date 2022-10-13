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

package mark

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Build = iota
	Probe
	End
)

type resultType int

const (
	condFalse resultType = iota
	condTrue
	condUnkown
)

type evalVector struct {
	// In MO,for example, select a from t1 where a+1 > 1, col a will be transformed as a vector,
	// when it comes to be accepted by the operator,will firstly calculate the a+1, so it's up to
	// the eval implementor whether he store the a+1 result in the old col a vector or create a
	// new vector to store it.
	needFree bool
	vec      *vector.Vector
}

// note that, different from other joins,the result vector is like below:
// Result[0],Result[1],......,Result[n-1],bool
// we will give more one bool type vector as a marker col
// so if you use mark join result, remember to get the last vector,that's what you want
type container struct {
	// here, we will have three states:
	// Build：we will use the right table to build a hashtable
	// Probe: we will use the left table data to probe the hashtable
	// End: Join working is over
	state int

	// // sels[i] records the table used to build hash table has which row numbers in the i-th group
	// sels [][]int64

	// in the probe stage, when we invoke func find to find rows in the hashtable,it will modify the
	// inBuckets Slice, inBuckets[i] means the i-th row is whether in the bucket
	// 0 means no, 1 means yes
	inBuckets []uint8

	// store the all batch from the build table
	bat *batch.Batch
	// records the eval result of the batch from the probe table
	evecs []evalVector
	// vecs is same as the evecs.vec's union, we need this because
	// when we use the Insert func to build the hashtable, we need
	// vecs not evecs
	vecs []*vector.Vector

	// record those tuple that contain null value in build table
	nullSels []int64
	// record those tuple that contain normal value in build table
	sels []int64
	// the result of eval join condtion for conds[1] and cons[0], those two vectors is used to
	// check equal condition when zval == 0 or condState is False from JoinMap
	buildEqVec   []*vector.Vector
	buildEqEvecs []evalVector

	// a tag means wheretr the build table has null
	hasNull bool

	Nsp       *nulls.Nulls
	joinFlags []bool

	mp *hashmap.JoinMap

	nullWithBatch *batch.Batch

	rewriteCond *plan.Expr
}

// // for join operator, it's a two-ary operator, we will reference to two table
// // so we need this info to determine the columns to output
// // (rel,pos) gives which table and which column
// type ResultPos struct {
// 	Rel int32
// 	Pos int32
// }

// remember that we may use partition stragey, for example, if the origin table has data squence
// like 1,2,3,4. If we use the hash method, after using hash function,assume that we get 13,14,15,16.
// and we divide them into 3 buckets. so 13%3 = 1,so 3 is in the 1-th bucket and so on like this
type Argument struct {
	// container means the local parameters defined by the operator constructor
	ctr *container
	// the five attributes below are passed by the outside

	// Ibucket determines the data partition this operator need to deal with
	Ibucket uint64
	// Nbucket means how many partitions there are
	Nbucket uint64

	// // the input batch's columns' type
	// Typs []types.Type

	// records the result cols' position that the build table needs to return
	Result []int32
	// because we have two tables here,so it's a slice
	// Conditions[i] stands for the table_i's expression
	// we ned this to eval first, if there is a expression
	// like  t.a+1, we will eval it first and then to build
	// hashtable and probe to get the result. note that,though
	// we have two tables, but len(Conditions[0]) == len(Conditions[1])
	// for example, select * from t1 join t2 on t1.a = t2.d+t2.e+t2.f;
	// We will get Condition below:
	// for t1: Expr_Col --> t1.a
	// for t2: Expr_F(arg0,arg1)
	// and the arg0 is Expr_F(t2.d,t2.e)
	// and the arg1 is Expr_Col --> t2.f
	// so from the view of above, the len of two Conditions is the same
	// then you will see I just make the evals with len(Conditions[0]),but
	// I will use evalJoinConditions with parameters Conditions[0] or Conditions[1]
	// they are both ok
	Conditions [][]*plan.Expr

	// tell whether the tuple marked with NULL should be outputed
	OutputNull bool

	// tell whether mark col should be outputed
	OutputMark bool

	Typs []types.Type

	// markMeaning means,if MarkMeaning is true and a tuple is marked as true, output it,if is marked with false,
	// don't output. The same way that if MarkMeaning is false and a tuple is marked as false, output it,if is
	// marked with true, don't output.
	MarkMeaning bool

	// if OutputAnyway is true,we will output all tuples from left table
	OutputAnyway bool

	Cond *plan.Expr

	OnList []*plan.Expr
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	ctr := arg.ctr
	if ctr != nil {
		mp := proc.Mp()
		ctr.cleanBatch(mp)
		ctr.cleanEvalVectors(mp)
		ctr.cleanEqVectors(mp)
		ctr.cleanHashMap()
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
	if ctr.nullWithBatch != nil {
		ctr.nullWithBatch.Clean(mp)
		ctr.nullWithBatch = nil
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) cleanEvalVectors(mp *mpool.MPool) {
	for i := range ctr.evecs {
		if ctr.evecs[i].needFree && ctr.evecs[i].vec != nil {
			ctr.evecs[i].vec.Free(mp)
			ctr.evecs[i].vec = nil
		}
	}
}

func (ctr *container) cleanEqVectors(mp *mpool.MPool) {
	for i := range ctr.buildEqEvecs {
		if ctr.buildEqEvecs[i].needFree && ctr.buildEqEvecs[i].vec != nil {
			ctr.buildEqEvecs[i].vec.Free(mp)
			ctr.buildEqEvecs[i].vec = nil
		}
	}
}

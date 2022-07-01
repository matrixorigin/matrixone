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

package pipeline2

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Pipeline contains the information associated with a pipeline in a query execution plan.
// A query execution plan may contains one or more pipelines.
// As an example:
//
//  CREATE TABLE order
//  (
//        order_id    INT,
//        uid          INT,
//        item_id      INT,
//        year         INT,
//        nation       VARCHAR(100)
//  );
//
//  CREATE TABLE customer
//  (
//        uid          INT,
//        nation       VARCHAR(100),
//        city         VARCHAR(100)
//  );
//
//  CREATE TABLE supplier
//  (
//        item_id      INT,
//        nation       VARCHAR(100),
//        city         VARCHAR(100)
//  );
//
// 	SELECT c.city, s.city, sum(o.revenue) AS revenue
//  FROM customer c, order o, supplier s
//  WHERE o.uid = c.uid
//  AND o.item_id = s.item_id
//  AND c.nation = 'CHINA'
//  AND s.nation = 'CHINA'
//  AND o.year >= 1992 and o.year <= 1997
//  GROUP BY c.city, s.city, o.year
//  ORDER BY o.year asc, revenue desc;
//
//  AST PLAN:
//     order
//       |
//     group
//       |
//     filter
//       |
//     join
//     /  \
//    s   join
//        /  \
//       l   c
//
// In this example, a possible pipeline is as follows:
//
// pipeline:
// o ⨝ c ⨝ s
//  -> σ(c.nation = 'CHINA' ∧  o.year >= 1992 ∧  o.year <= 1997 ∧  s.nation = 'CHINA')
//  -> γ([c.city, s.city, o.year, sum(o.revenue) as revenue], c.city, s.city, o.year)
//  -> τ(o.year asc, revenue desc)
//  -> π(c.city, s.city, revenue)
type Pipeline struct {
	// attrs, column list.
	attrs     []string
	attrTypes []plan.ColDef
	// orders to be executed
	instructions vm.Instructions
	reg          *process.WaitRegister
}

func (p *Pipeline) getValidColLen() int {
	var count int = 0
	for _, attr := range p.attrs {
		if attr != "" {
			count++
		}
	}
	return count
}

func (p *Pipeline) getValidAttrs() []string {
	attrs := make([]string, 0)
	for _, attr := range p.attrs {
		if attr != "" {
			attrs = append(attrs, attr)
		}
	}
	return attrs
}

// Fill in the blanks to bat
func resetBat(bat *batch.Batch, pAttrs []string, pAttrTypes []plan.ColDef, proc *process.Process) *batch.Batch {
	resVectors := make([]*vector.Vector, len(pAttrs))
	var index int = 0
	for i, attr := range pAttrs {
		if attr == "" {
			scalarVector := proc.AllocScalarVector(types.PlanTypeToType(*(pAttrTypes[i].Typ)))
			FillScalarValForVector(scalarVector)
			scalarVector.Length = len(bat.Zs)
			resVectors[i] = scalarVector
		} else {
			resVectors[i] = bat.Vecs[index]
			index++
		}
	}
	bat.Vecs = resVectors
	bat.Attrs = pAttrs
	return bat
}

func FillScalarValForVector(vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_bool:
		vector.SetCol(vec, []bool{true})
	case types.T_int8:
		vector.SetCol(vec, []int8{0})
	case types.T_int16:
		vector.SetCol(vec, []int16{0})
	case types.T_int32:
		vector.SetCol(vec, []int32{0})
	case types.T_int64:
		vector.SetCol(vec, []int64{0})
	case types.T_uint8:
		vector.SetCol(vec, []uint8{0})
	case types.T_uint16:
		vector.SetCol(vec, []uint16{0})
	case types.T_uint32:
		vector.SetCol(vec, []uint32{0})
	case types.T_uint64:
		vector.SetCol(vec, []uint64{0})
	case types.T_float32:
		vector.SetCol(vec, []float32{0})
	case types.T_float64:
		vector.SetCol(vec, []float64{0})
	case types.T_char:
		resultValues := &types.Bytes{
			Data:    make([]byte, 1),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		resultValues.Data = []byte{97}
		resultValues.Offsets = []uint32{0}
		resultValues.Lengths = []uint32{1}
		vector.SetCol(vec, resultValues)
	case types.T_varchar:
		resultValues := &types.Bytes{
			Data:    make([]byte, 1),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		resultValues.Data = []byte{97}
		resultValues.Offsets = []uint32{0}
		resultValues.Lengths = []uint32{1}
		vector.SetCol(vec, resultValues)
	case types.T_date:
		vector.SetCol(vec, []types.Date{731673})
	case types.T_datetime:
		vector.SetCol(vec, []types.Datetime{66287400412774400})
	case types.T_timestamp:
		vector.SetCol(vec, []types.Timestamp{66287370213785600})
	case types.T_sel:

	case types.T_decimal64:
		vector.SetCol(vec, []types.Decimal64{0})
	case types.T_decimal128:
		vector.SetCol(vec, []types.Decimal128{
			{
				Lo: 0,
				Hi: 0,
			},
		})
	}
}

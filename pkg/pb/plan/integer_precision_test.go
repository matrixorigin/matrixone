// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func validIntegerPrecisionQuery() *Query {
	return &Query{Headings: []string{"x"}, IntegerPrecision: &IntegerPrecisionGraph{SourceBudget: 70, Nodes: []*IntegerPrecisionNode{
		{Kind: IntegerPrecisionNode_SQL_LITERAL, Precision: 70},
		{Kind: IntegerPrecisionNode_SQL_LITERAL, Precision: 1},
		{Kind: IntegerPrecisionNode_ADD, Inputs: []uint32{1, 2}, Arithmetic: true},
	}, Outputs: []*IntegerPrecisionOutput{{Node: 3, RootClass: IntegerPrecisionOutput_DIRECT_FUNCTION}}}}
}

func TestIntegerPrecisionAdmission(t *testing.T) {
	q := validIntegerPrecisionQuery()
	require.NoError(t, q.ValidateIntegerPrecision())
	data, err := q.Marshal()
	require.NoError(t, err)
	var decoded Query
	require.NoError(t, decoded.Unmarshal(data))
	for name, change := range map[string]func(*Query){
		"cycle":          func(q *Query) { q.IntegerPrecision.Nodes[2].Inputs[0] = 3 },
		"arity":          func(q *Query) { q.IntegerPrecision.Nodes[2].Inputs = nil },
		"source budget":  func(q *Query) { q.IntegerPrecision.SourceBudget = 65 },
		"output index":   func(q *Query) { q.IntegerPrecision.Outputs[0].Node = 4 },
		"root class":     func(q *Query) { q.IntegerPrecision.Outputs[0].RootClass = IntegerPrecisionOutput_SQL_LITERAL },
		"kind":           func(q *Query) { q.IntegerPrecision.Nodes[2].Kind = 99 },
		"cached formula": func(q *Query) { q.IntegerPrecision.Nodes[2].Precision = 71 },
		"provenance":     func(q *Query) { q.IntegerPrecision.Nodes[2].Arithmetic = false },
		"param index": func(q *Query) {
			q.IntegerPrecision.Nodes[0] = &IntegerPrecisionNode{Kind: IntegerPrecisionNode_PARAMETER, Precision: 20, ParamPos: 2}
			q.IntegerPrecision.ParameterCount = 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			q := validIntegerPrecisionQuery()
			change(q)
			err := q.ValidateIntegerPrecision()
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "unexpected admission error: %v", err)
			require.ErrorContains(t, err, "invalid integer precision graph: ")
			data, err := q.Marshal()
			require.NoError(t, err)
			var decoded Query
			err = decoded.Unmarshal(data)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "RPC admission must reject before result metadata access: %v", err)
			require.ErrorContains(t, err, "invalid integer precision graph: ")
		})
	}
	require.NoError(t, (&Query{}).ValidateIntegerPrecision(), "absent old-plan metadata")
}

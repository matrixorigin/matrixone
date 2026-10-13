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

package shard

import (
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestRequestUnmarshalRejectsUnknownNestedLiteralForm(t *testing.T) {
	invalid := &planpb.Expr{Typ: planpb.Type{Id: 61}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
		Value: &planpb.Literal_Sval{Sval: "x"}, LiteralForm: planpb.StringLiteralForm(99),
	}}}
	for _, request := range []*Request{
		{ShardRead: ShardReadRequest{Param: ReadParam{RangesParam: RangesParam{Exprs: []*planpb.Expr{invalid}}}}},
		{ShardRead: ShardReadRequest{Param: ReadParam{ReaderBuildParam: ReaderBuildParam{Expr: invalid}}}},
	} {
		encoded, err := request.Marshal()
		require.NoError(t, err)
		require.ErrorContains(t, (&Request{}).Unmarshal(encoded), "invalid string literal form 99")
	}
}

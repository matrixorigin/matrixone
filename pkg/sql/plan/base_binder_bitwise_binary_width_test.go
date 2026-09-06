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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestBindBitwiseAggregateSubstringBinaryWidth(t *testing.T) {
	ctx := context.Background()
	for _, source := range []struct {
		name string
		typ  types.Type
	}{
		{name: "varbinary", typ: types.NewWithCharset(types.T_varbinary, 512, 0, types.CharsetBinary)},
		{name: "blob", typ: types.T_blob.ToType()},
	} {
		source := source
		t.Run(source.name, func(t *testing.T) {
			sourceExpr := &planpb.Expr{
				Typ: makePlan2Type(&source.typ),
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
					RelPos: 0,
					ColPos: 0,
				}},
			}

			for _, test := range []struct {
				name      string
				length    int64
				wantWidth int32
				wantError bool
			}{
				{name: "bounded", length: 511, wantWidth: 511},
				{name: "still oversized", length: 512, wantWidth: 512, wantError: true},
			} {
				t.Run(test.name, func(t *testing.T) {
					substring, err := BindFuncExprImplByPlanExpr(ctx, "substring", []*planpb.Expr{
						sourceExpr,
						makePlan2Int64ConstExprWithType(1),
						makePlan2Int64ConstExprWithType(test.length),
					})
					require.NoError(t, err)
					require.Equal(t, int32(types.T_varbinary), substring.Typ.Id)
					require.Equal(t, test.wantWidth, substring.Typ.Width)

					for _, aggregateName := range []string{"bit_and", "bit_or", "bit_xor"} {
						_, err = BindFuncExprImplByPlanExpr(ctx, aggregateName, []*planpb.Expr{substring})
						if test.wantError {
							require.Error(t, err, "%s must reject SUBSTRING(..., %d)", aggregateName, test.length)
							moErr := moerr.DowncastError(err)
							require.Equal(t, moerr.ErrInvalidBitwiseAggregateOperandsSize, moErr.ErrorCode())
						} else {
							require.NoError(t, err, "%s must accept SUBSTRING(..., %d)", aggregateName, test.length)
						}
					}
				})
			}
		})
	}
}

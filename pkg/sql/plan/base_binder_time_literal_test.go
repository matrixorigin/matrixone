// Copyright 2022 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBindTimeDecimalLiteralPreservesLeadingZero(t *testing.T) {
	target := plan.Type{Id: int32(types.T_time), Width: 6, Scale: 3}
	binder := NewDefaultBinder(context.Background(), nil, nil, target, nil)

	for _, tc := range []struct {
		name    string
		literal string
	}{
		{name: "fraction below one", literal: "0.001"},
		{name: "fraction with whole seconds", literal: "34.5"},
		{name: "negative fraction below one", literal: "-0.001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			numVal := tree.NewNumVal(tc.literal, tc.literal, false, tree.P_decimal)
			bound, err := binder.bindNumVal(numVal, target)
			require.NoError(t, err)
			require.Equal(t, int32(types.T_time), bound.Typ.Id)

			cast := bound.GetF()
			require.NotNil(t, cast)
			require.Equal(t, "cast", cast.GetFunc().GetObjName())
			require.Len(t, cast.Args, 2)
			source := cast.Args[0]
			require.Equal(t, int32(types.T_varchar), source.Typ.Id)
			require.Equal(t, tc.literal, source.GetLit().GetSval())
		})
	}
}

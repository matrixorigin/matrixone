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

package function

import (
	"context"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type legacyFloatStringBoundKey struct{}

// WithLegacyFloatStringBounds reconstructs historical implicit DOUBLE string
// casts for catalog-origin proof only. Never use it to author new expressions.
func WithLegacyFloatStringBounds(ctx context.Context) context.Context {
	return context.WithValue(ctx, legacyFloatStringBoundKey{}, true)
}

func LegacyFloatStringBounds(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	legacy, _ := ctx.Value(legacyFloatStringBoundKey{}).(bool)
	return legacy
}

func legacyFloatStringCheck(ctx context.Context, check checkResult, inputs []types.Type) checkResult {
	if !LegacyFloatStringBounds(ctx) || check.status != succeedWithCast {
		return check
	}
	check.finalType = slices.Clone(check.finalType)
	for i := range check.finalType {
		if i < len(inputs) && inputs[i].Oid == types.T_float64 &&
			check.finalType[i].Oid == types.T_varchar && check.finalType[i].Width == 34 {
			check.finalType[i].Width = 32
		}
	}
	return check
}

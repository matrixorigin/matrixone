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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
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

// The formatting era is independent of the implicit width era: repaired live
// casts use current widths while already-folded catalog constants retain bytes.
type legacyFloatStringFormatKey struct{}

// WithLegacyFloatStringFormatting is solely for catalog-origin folding proof.
// It must never be installed on an executing or authoring process.
func WithLegacyFloatStringFormatting(ctx context.Context) context.Context {
	return context.WithValue(ctx, legacyFloatStringFormatKey{}, true)
}

func LegacyFloatStringFormatting(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	legacy, _ := ctx.Value(legacyFloatStringFormatKey{}).(bool)
	return legacy
}

func legacyFloatToBytes(v float64, bitSize int) []byte {
	format := byte('f')
	if v >= 1e15 || v > 0 && v < 1e-13 || v < 0 && v > -1e-13 || v <= -1e15 {
		format = 'E'
	}
	return strconv.AppendFloat(nil, v, format, -1, bitSize)
}

func marshalJSONForFloatStringProof(ctx context.Context, bj bytejson.ByteJson) ([]byte, error) {
	if LegacyFloatStringFormatting(ctx) {
		return bj.MarshalJSONWithLegacyFloat()
	}
	return bj.MarshalJSON()
}
func unquoteJSONForFloatStringProof(ctx context.Context, bj bytejson.ByteJson) (string, error) {
	if LegacyFloatStringFormatting(ctx) {
		return bj.UnquoteWithLegacyFloat()
	}
	return bj.Unquote()
}

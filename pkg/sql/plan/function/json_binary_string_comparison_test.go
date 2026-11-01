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

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// forEachBinaryPeer walks the binary-string family, the opaque byte domains
// that the JSON comparison identities exclude: a character operand joins the
// JSON domain as a string scalar, while a binary peer never does.
func forEachBinaryPeer(yield func(peer types.Type)) {
	for _, oid := range []types.T{types.T_binary, types.T_varbinary, types.T_blob} {
		yield(oid.ToType())
	}
}

// TestJSONBinaryStringComparisonRejected pins the mixed JSON/binary-string
// boundary of issue #31121: every ordering or equality operator rejects the
// pair with the invalid-argument operator error in both operand orders, the
// same rejection the base gate jsonOrderingWithStringNotSupported already
// enforced for the ordering legs and the reversed equality leg. The lone
// pre-change forward-equality admission resolved through the comparison cast
// row and silently compared raw binary bytes as a JSON string scalar.
func TestJSONBinaryStringComparisonRejected(t *testing.T) {
	ctx := context.Background()
	names := []string{"=", "<=>", "!=", ">", ">=", "<", "<="}
	for _, name := range names {
		forEachBinaryPeer(func(peer types.Type) {
			for _, args := range [][]types.Type{
				{types.T_json.ToType(), peer},
				{peer, types.T_json.ToType()},
			} {
				label := "operator " + name + " json/" + peer.Oid.String()
				_, err := GetFunctionByName(ctx, name, args)
				require.Error(t, err, label+" must reject the mixed JSON/binary pair")
			}
		})
	}
	// The character family keeps the direct JSON/string identity overload.
	for _, name := range []string{"=", "<=>", "!=", ">", ">=", "<", "<="} {
		for _, peer := range []types.Type{
			types.T_char.ToType(), types.T_varchar.ToType(), types.T_text.ToType(),
		} {
			for _, args := range [][]types.Type{
				{types.T_json.ToType(), peer},
				{peer, types.T_json.ToType()},
			} {
				got, err := GetFunctionByName(ctx, name, args)
				require.NoError(t, err, "operator %s character control", name)
				_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
				require.Equal(t, int32(1), overloadIndex, "operator %s identity overload", name)
				_, shouldCast := got.ShouldDoImplicitTypeCast()
				require.False(t, shouldCast, "operator %s must not cast", name)
			}
		}
	}
	// Same-domain pairs keep their ordinary identities.
	for _, name := range []string{"=", ">", "<="} {
		_, err := GetFunctionByName(ctx, name, []types.Type{
			types.T_binary.ToType(), types.T_varbinary.ToType(),
		})
		require.NoError(t, err, "operator %s binary same-domain control", name)
		_, err = GetFunctionByName(ctx, name, []types.Type{
			types.T_json.ToType(), types.T_json.ToType(),
		})
		require.NoError(t, err, "operator %s json same-domain control", name)
	}
}

// TestJSONBinaryStringEraRestoresPredecessorForwardCast keeps DUMP/LOAD
// verification of predecessor-authored folded defaults working: the
// predecessor main bound the forward (JSON, binary-string) equality leg
// through the comparison cast row and rejected every other leg, so the
// legacy-era rebinding reproduces exactly that shape.
func TestJSONBinaryStringEraRestoresPredecessorForwardCast(t *testing.T) {
	eraCtx := WithLegacyJSONScalarSemantics(context.Background())
	forEachBinaryPeer(func(peer types.Type) {
		for _, name := range []string{"=", "<=>", "!="} {
			got, err := GetFunctionByName(eraCtx, name, []types.Type{
				types.T_json.ToType(), peer,
			})
			require.NoError(t, err, "era forward %s json/%s binds", name, peer.Oid.String())
			_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
			require.Equal(t, int32(0), overloadIndex,
				"era forward %s keeps the predecessor cast overload", name)
			_, err = GetFunctionByName(eraCtx, name, []types.Type{
				peer, types.T_json.ToType(),
			})
			require.Error(t, err, "era reverse %s stays rejected", name)
		}
		// The predecessor rejected every ordering leg of the pair; the era
		// keeps rejecting them instead of reserving a silent text fold.
		for _, name := range []string{">", ">=", "<", "<="} {
			for _, args := range [][]types.Type{
				{types.T_json.ToType(), peer},
				{peer, types.T_json.ToType()},
			} {
				_, err := GetFunctionByName(eraCtx, name, args)
				require.Error(t, err,
					"era ordering "+name+" json/"+peer.Oid.String()+" stays rejected")
			}
		}
	})
}

// Copyright 2026 Matrix Origin
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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// TestAppendValueToVectorEveryType appends a value of every type a vector
// supports the way window value functions (lag, lead, first_value, ...) store
// their results, and reads it back: no fixed-size type may be appended as
// bytes.
func TestAppendValueToVectorEveryType(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	for _, oid := range types.AllOids() {
		if !vector.SupportsAny(oid) {
			continue
		}
		typ := oid.ToType()
		// a source vector holding one value, whose raw bytes the window keeps
		src := vector.NewVec(typ, mp)
		raw := make([]byte, typ.TypeSize())
		if typ.IsVarlen() {
			raw = []byte("a window value longer than an inline varlena")
		} else {
			for i := range raw {
				raw[i] = byte(i + 1)
			}
			if oid == types.T_bool {
				raw = []byte{1}
			}
		}
		require.NoError(t, vector.MakeAppendBytesFunc(src)(raw, false, mp), oid.OidString())

		dst := vector.NewVec(typ, mp)
		require.NoError(t, appendValueToVector(dst, raw, typ, mp), oid.OidString())
		require.Equal(t, 1, dst.Length(), oid.OidString())
		require.Equal(t, vector.GetAny(src, 0, true), vector.GetAny(dst, 0, true), oid.OidString())
		src.Free(mp)
		dst.Free(mp)
	}
}

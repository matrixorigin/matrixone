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
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestCopyDefaultInto(t *testing.T) {
	require.Nil(t, CopyDefaultInto(&Default{}, nil))
	for _, src := range []*Default{
		{},
		{OriginString: "x", NullAbility: true, XXX_unrecognized: []byte{0x98, 0x06, 0x01}},
		{Expr: &Expr{Typ: Type{Width: 42}}, XXX_unrecognized: []byte{}},
	} {
		var dst Default
		got := CopyDefaultInto(&dst, src)
		require.True(t, proto.Equal(proto.Clone(src), got))
		require.Equal(t, src.XXX_unrecognized == nil, got.XXX_unrecognized == nil)
		if len(got.XXX_unrecognized) > 0 {
			got.XXX_unrecognized[0] = 0
			require.NotEqual(t, got.XXX_unrecognized[0], src.XXX_unrecognized[0])
		}
		if got.Expr != nil {
			got.Expr.Typ.Width++
			require.NotEqual(t, got.Expr.Typ.Width, src.Expr.Typ.Width)
		}
	}
}

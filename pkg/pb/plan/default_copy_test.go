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

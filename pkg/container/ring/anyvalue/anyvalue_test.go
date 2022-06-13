package anyvalue

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSerializationForAnyValueRing1(t *testing.T) {
	tests := []struct {
		info string
		ring AnyVRing1[int64]
	}{
		{
			info: "test1",
			ring: AnyVRing1[int64]{
				Typ: types.Type{Oid: types.T_int64},
				Vs:  []int64{1, 2, 0},
				Ns:  []int64{0, 0, 2},
				Set: []bool{true, true, false},
			},
		},
		{
			info: "test2",
			ring: AnyVRing1[int64]{
				Typ: types.Type{Oid: types.T_int64},
				Vs:  []int64{-1, -2, -3, -5},
				Ns:  []int64{3, 1, 2, 4},
				Set: []bool{false, false, false},
			},
		},
	}
	for _, tt := range tests {
		b := make([]byte, 0, 10000)
		buf := bytes.NewBuffer(b)
		t.Run(tt.info, func(t *testing.T) {
			EncodeAnyValueRing1[int64](tt.ring, buf)
			finalRing, _, _ := DecodeAnyValueRing1[int64](buf.Bytes(), types.Type{Oid: types.T_int64})
			require.Equal(t, tt.ring.Vs, finalRing.Vs)
			require.Equal(t, tt.ring.Ns, finalRing.Ns)
			require.Equal(t, tt.ring.Set, finalRing.Set)
		})
	}
}

func TestSerializationForAnyValueRing2(t *testing.T) {
	tests := []struct {
		info string
		ring AnyVRing2
	}{
		{
			info: "test1",
			ring: AnyVRing2{
				Typ: types.Type{Oid: types.T_varchar},
				Vs:  [][]byte{[]byte("abc"), []byte("a%_123"), []byte(nil)},
				Ns:  []int64{0, 0, 2},
				Set: []bool{true, true, false},
			},
		},
	}
	for _, tt := range tests {
		b := make([]byte, 0, 10000)
		buf := bytes.NewBuffer(b)
		t.Run(tt.info, func(t *testing.T) {
			EncodeAnyRing2(tt.ring, buf)
			println(fmt.Sprintf("buf is %v", buf))
			finalRing, _, _ := DecodeAnyRing2(buf.Bytes())
			require.Equal(t, tt.ring.Vs, finalRing.Vs)
			require.Equal(t, tt.ring.Ns, finalRing.Ns)
			require.Equal(t, tt.ring.Set, finalRing.Set)
		})
	}
}

package types

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBytes_Reset(t *testing.T) {
	myBytes := Bytes{Data: []byte("nihaonihaonihao"), Offsets: []uint32{0, 5, 10}, Lengths: []uint32{5, 5, 5}}
	myBytes.Reset()
	require.Equal(t, Bytes{Data: []byte{}, Offsets: []uint32{}, Lengths: []uint32{}}, myBytes)
}

func TestBytes_Window(t *testing.T) {
	myBytes := Bytes{Data: []byte("nihaohellogutentagkonichiwa"), Offsets: []uint32{0, 5, 10, 18}, Lengths: []uint32{5, 5, 8, 9}}
	require.Equal(t, &Bytes{Data: []byte("nihaohellogutentagkonichiwa"), Offsets: []uint32{5, 10}, Lengths: []uint32{5, 8}}, myBytes.Window(1, 3))
}

func TestBytes_Append(t *testing.T) {
	myBytes := Bytes{Data: []byte("nihaohellogutentagkonichiwa"), Offsets: []uint32{0, 5, 10, 18}, Lengths: []uint32{5, 5, 8, 9}}
	appendBytes := [][]byte{[]byte("festina"), []byte("lente")}
	myBytes.Append(appendBytes)
	require.Equal(t, Bytes{Data: []byte("nihaohellogutentagkonichiwafestinalente"),
		Offsets: []uint32{0, 5, 10, 18, 27, 34}, Lengths: []uint32{5, 5, 8, 9, 7, 5}}, myBytes)
}

func TestBytes_Get(t *testing.T) {
	myBytes := Bytes{Data: []byte("nihaohellogutentagkonichiwa"), Offsets: []uint32{0, 5, 10, 18}, Lengths: []uint32{5, 5, 8, 9}}
	result := myBytes.Get(1)
	require.Equal(t, []byte("hello"), result)
}

func TestBytes_Swap(t *testing.T) {
	myBytes := Bytes{Data: []byte("nihaohellogutentagkonichiwa"), Offsets: []uint32{0, 5, 10, 18}, Lengths: []uint32{5, 5, 8, 9}}
	myBytes.Swap(1, 2)
	require.Equal(t, []uint32{0, 10, 5, 18}, myBytes.Offsets)
	require.Equal(t, []uint32{5, 8, 5, 9}, myBytes.Lengths)
}

func TestBytes_String(t *testing.T) {
	myBytes := Bytes{Data: []byte("nihaohellogutentagkonichiwa"), Offsets: []uint32{0, 5, 10, 18}, Lengths: []uint32{5, 5, 8, 9}}
	require.Equal(t, "[nihao hello gutentag konichiwa]", myBytes.String())
}

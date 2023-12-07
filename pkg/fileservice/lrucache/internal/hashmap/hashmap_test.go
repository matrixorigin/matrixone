package hashmap

import (
	"testing"

	"github.com/dolthub/maphash"
	"github.com/stretchr/testify/require"
)

func TestHashmap(t *testing.T) {
	m := New[int, int](0)
	hasher := maphash.NewHasher[int]()
	for i := 0; i < 1000; i++ {
		v := new(int)
		*v = i
		m.Set(hasher.Hash(i), i, v)
	}
	// test Len
	require.Equal(t, 1000, m.Len())
	// test Get
	for i := 0; i < 1000; i++ {
		v, ok := m.Get(hasher.Hash(i), i)
		require.Equal(t, true, ok)
		require.Equal(t, i, *v)
	}
	// test Delete
	m.Delete(0, 0)
	_, ok := m.Get(0, 0)
	require.Equal(t, false, ok)
}

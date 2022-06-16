package dnservice

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAvailableStores(t *testing.T) {
	dnStores := []*dnStore{
		newDnStore("1", 1, 1), // full
		newDnStore("2", 3, 3), // full
		newDnStore("3", 1, 3), // 2 slots
		newDnStore("4", 4, 4), // full
		newDnStore("5", 1, 4), // 3 slots
	}

	stores := spareStores(dnStores)
	require.Equal(t, 2, len(stores))
}

func TestFilter(t *testing.T) {
	dnStores := []*dnStore{
		newDnStore("0", 0, 10),
		newDnStore("1", 1, 10),
		newDnStore("10", 10, 10),
		newDnStore("5", 5, 5),
	}

	fullFilter := &filterOutFull{}
	candidates := make([]*dnStore, 0, len(dnStores))
	for _, store := range dnStores {
		if fullFilter.Filter(store) {
			continue
		}
		candidates = append(candidates, store)
	}
	require.Equal(t, 2, len(candidates))
}

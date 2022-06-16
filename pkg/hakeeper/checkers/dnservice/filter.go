package dnservice

// Filter filter unexpected DN store
type Filter interface {
	Filter(store *dnStore) bool
}

// filterOutFull filter out full DN store
type filterOutFull struct {
}

func (f *filterOutFull) Filter(store *dnStore) bool {
	return store.length >= store.capacity
}

// filterDnStore filters dn store according to Filters
func filterDnStore(stores []*dnStore, filters []Filter) []*dnStore {
	var candidates []*dnStore
	for _, store := range stores {
		for _, filter := range filters {
			if filter.Filter(store) {
				continue
			}
			candidates = append(candidates, store)
		}
	}
	return candidates
}

// availableStores selects available dn stores.
func spareStores(working []*dnStore) []*dnStore {
	return filterDnStore(working, []Filter{
		&filterOutFull{},
	})
}

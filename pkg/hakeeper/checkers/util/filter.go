package util

// IFilter filter interface
type IFilter interface {
	Filter(store *Store) bool
}

type Filter func(store *Store) bool

func (f Filter) Filter(store *Store) bool {
	return f(store)
}

// FilterStore filters store according to Filters
func FilterStore(stores []*Store, filters []IFilter) (candidates []*Store) {
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

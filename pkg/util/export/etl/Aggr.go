package etl

import (
	"time"
)

type Item interface {
	Key(duration time.Duration) interface{}
}

type Aggregator struct {
	Grouped     map[interface{}]Item
	Ungrouped   []Item
	WindowSize  time.Duration
	NewItemFunc func() Item
	UpdateFunc  func(existing, new Item)
	FilterFunc  func(i Item) bool
}

func NewAggregator(windowSize time.Duration, newItemFunc func() Item, updateFunc func(existing, new Item), filterFunc func(i Item) bool) *Aggregator {
	return &Aggregator{
		Grouped:     make(map[interface{}]Item),
		Ungrouped:   []Item{},
		WindowSize:  windowSize,
		NewItemFunc: newItemFunc,
		UpdateFunc:  updateFunc,
		FilterFunc:  filterFunc,
	}
}

func (a *Aggregator) AddItem(i Item) {
	if a.FilterFunc(i) {
		group, exists := a.Grouped[i.Key(a.WindowSize)]
		if !exists {
			group = a.NewItemFunc()
			a.Grouped[i.Key(a.WindowSize)] = group
		}

		a.UpdateFunc(group, i)
	} else {
		a.Ungrouped = append(a.Ungrouped, i)
	}
}

func (a *Aggregator) GetResults() ([]Item, []Item) {
	var results []Item
	for _, group := range a.Grouped {
		results = append(results, group)
	}
	return results, a.Ungrouped
}

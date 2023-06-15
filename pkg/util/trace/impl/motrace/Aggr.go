package motrace

import (
	"errors"
	"time"
)

type Item interface {
	Key(duration time.Duration) interface{}
}

type Aggregator struct {
	Grouped     map[interface{}]Item
	WindowSize  time.Duration
	NewItemFunc func() Item
	UpdateFunc  func(existing, new Item)
	FilterFunc  func(i Item) bool
}

func NewAggregator(windowSize time.Duration, newItemFunc func() Item, updateFunc func(existing, new Item), filterFunc func(i Item) bool) *Aggregator {
	return &Aggregator{
		Grouped:     make(map[interface{}]Item),
		WindowSize:  windowSize,
		NewItemFunc: newItemFunc,
		UpdateFunc:  updateFunc,
		FilterFunc:  filterFunc,
	}
}

var ErrFilteredOut = errors.New("item filtered out")

func (a *Aggregator) AddItem(i Item) (Item, error) {
	if !a.FilterFunc(i) {
		return i, ErrFilteredOut

	}

	group, exists := a.Grouped[i.Key(a.WindowSize)]
	if !exists {
		group = a.NewItemFunc()
		a.Grouped[i.Key(a.WindowSize)] = group
	}

	a.UpdateFunc(group, i)
	return nil, nil
}

func (a *Aggregator) GetResults() []Item {
	var results []Item
	for _, group := range a.Grouped {
		results = append(results, group)
	}
	return results
}

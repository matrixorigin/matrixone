package etl

import (
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
}

func NewAggregator(windowSize time.Duration, newItemFunc func() Item, updateFunc func(existing, new Item)) *Aggregator {
	return &Aggregator{
		Grouped:     make(map[interface{}]Item),
		WindowSize:  windowSize,
		NewItemFunc: newItemFunc,
		UpdateFunc:  updateFunc,
	}
}

func (a *Aggregator) AddItem(i Item) {
	group, exists := a.Grouped[i.Key(a.WindowSize)]
	if !exists {
		group = a.NewItemFunc()
		a.Grouped[i.Key(a.WindowSize)] = group
	}

	a.UpdateFunc(group, i)
}

func (a *Aggregator) GetResults() []Item {
	var results []Item
	for _, group := range a.Grouped {
		results = append(results, group)
	}
	return results
}

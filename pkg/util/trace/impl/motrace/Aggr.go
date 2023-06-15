package motrace

import (
	"context"
	"errors"
	"time"
)

type Item interface {
	Key(duration time.Duration) interface{}
}

type Aggregator struct {
	ctx         context.Context
	Grouped     map[interface{}]Item
	WindowSize  time.Duration
	NewItemFunc func(i Item, ctx context.Context) Item
	UpdateFunc  func(existing, new Item)
	FilterFunc  func(i Item) bool
}

func NewAggregator(ctx context.Context, windowSize time.Duration, newItemFunc func(i Item, ctx context.Context) Item, updateFunc func(existing, new Item), filterFunc func(i Item) bool) *Aggregator {
	return &Aggregator{
		ctx:         ctx,
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
		group = a.NewItemFunc(i, a.ctx)
		a.Grouped[i.Key(a.WindowSize)] = group
	} else {
		a.UpdateFunc(group, i)
	}
	return nil, nil
}

func (a *Aggregator) GetResults() []Item {
	var results []Item
	for _, group := range a.Grouped {
		results = append(results, group)
	}
	return results
}

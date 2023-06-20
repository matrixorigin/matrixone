// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package motrace

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

var ErrFilteredOut = moerr.NewInternalError(context.Background(), "filtered out")

func (a *Aggregator) Close() {
	// clean up the Grouped map
	a.Grouped = make(map[interface{}]Item)
	// release resources related to the context if necessary
	a.ctx = nil
}

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
	results := make([]Item, 0, len(a.Grouped))
	for _, group := range a.Grouped {
		results = append(results, group)
	}
	return results
}

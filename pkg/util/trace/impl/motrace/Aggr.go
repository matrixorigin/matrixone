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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

type Aggregator struct {
	ctx         context.Context
	mux         sync.Mutex
	Grouped     map[table.WindowKey]table.Item
	WindowSize  time.Duration
	NewItemFunc func(i table.Item, ctx context.Context) table.Item
	UpdateFunc  func(ctx context.Context, existing, new table.Item)
	FilterFunc  func(i table.Item) bool
}

type windowKey int

const (
	DurationKey windowKey = iota
)

func NewAggregator(ctx context.Context, windowSize time.Duration, newItemFunc func(i table.Item, ctx context.Context) table.Item, updateFunc func(ctx context.Context, existing, new table.Item), filterFunc func(i table.Item) bool) *Aggregator {
	ctx = context.WithValue(ctx, DurationKey, windowSize)
	return &Aggregator{
		ctx:         ctx,
		Grouped:     make(map[table.WindowKey]table.Item),
		WindowSize:  windowSize,
		NewItemFunc: newItemFunc,
		UpdateFunc:  updateFunc,
		FilterFunc:  filterFunc,
	}
}

var ErrFilteredOut = moerr.NewInternalError(context.Background(), "filtered out")

func (a *Aggregator) Close() {
	// Free the StatementInfo in the Grouped
	for _, item := range a.Grouped {
		if stmt, ok := item.(*StatementInfo); ok {
			stmt.freeNoLocked()
		}
	}
	// clean up the Grouped map
	a.Grouped = make(map[table.WindowKey]table.Item)
	// release resources related to the context if necessary
	a.ctx = nil
}

func (a *Aggregator) AddItem(i table.Item) (table.Item, error) {
	a.mux.Lock()
	defer a.mux.Unlock()
	if !a.FilterFunc(i) {
		return i, ErrFilteredOut
	}

	groupedItem, exists := a.Grouped[i.Key(a.WindowSize)]
	if !exists {
		groupKey := i.Key(a.WindowSize)
		groupedItem = a.NewItemFunc(i, a.ctx)
		a.Grouped[groupKey] = groupedItem
	} else {
		a.UpdateFunc(a.ctx, groupedItem, i)
	}
	return nil, nil
}

// GetResults return the aggr-ed result in Aggregator, but still keep in Aggregator
// Needs co-operate with Close
func (a *Aggregator) GetResults() []table.Item {
	a.mux.Lock()
	defer a.mux.Unlock()
	results := make([]table.Item, 0, len(a.Grouped))
	for _, group := range a.Grouped {
		results = append(results, group)
	}
	return results
}

func (a *Aggregator) GetWindow() time.Duration { return a.WindowSize }

// PopResultsBeforeWindow implements table.Aggregator.
// return grouped items in Aggregator, and remove them from the Aggregator.
// Those items NEED to be free by caller.
func (a *Aggregator) PopResultsBeforeWindow(end time.Time) []table.Item {
	a.mux.Lock()
	defer a.mux.Unlock()
	results := make([]table.Item, 0, len(a.Grouped))
	for key, group := range a.Grouped {
		if key.Before(end) {
			results = append(results, group)
			delete(a.Grouped, key) // fix mem-leak issue
		}
	}
	return results
}

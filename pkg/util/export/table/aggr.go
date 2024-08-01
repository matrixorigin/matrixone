// Copyright 2024 Matrix Origin
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

package table

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
)

type WindowKey interface {
	Before(end time.Time) bool
}

type Item interface {
	batchpipe.HasName
	Key(duration time.Duration) WindowKey
	Free()
}

type Aggregator interface {
	// AddItem do aggr the input item
	// If aggregated, return nil, nil
	// else return i, err
	AddItem(i Item) (Item, error)
	// GetResults return all grouped items.
	GetResults() []Item
	// PopResultsBeforeWindow return grouped items in Aggregator, and remove them from the Aggregator.
	// Those items NEED to be free by caller.
	PopResultsBeforeWindow(end time.Time) []Item
	GetWindow() time.Duration
	Close()
}

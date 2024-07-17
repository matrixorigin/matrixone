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
	GetResults() []Item
	GetResultsBeforeWindow(end time.Time) []Item
	GetWindow() time.Duration
	Close()
}

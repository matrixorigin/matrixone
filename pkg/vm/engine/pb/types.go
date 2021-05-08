package pb

import "github.com/cockroachdb/pebble"

type pbEngine struct {
	db *pebble.DB
}

type pbBatch struct {
	db  *pebble.DB
	bat *pebble.Batch
}

type pbIterator struct {
	itr *pebble.Iterator
}

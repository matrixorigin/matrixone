package handle

import (
	"io"
	"sync"
)

// TODO: this is not thread-safe
type Iterator interface {
	sync.Locker
	RLock()
	RUnlock()
	io.Closer
	Valid() bool
	Next()
}

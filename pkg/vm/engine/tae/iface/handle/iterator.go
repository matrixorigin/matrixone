package handle

import "io"

// TODO: this is not thread-safe
type Iterator interface {
	io.Closer
	Valid() bool
	Next()
}

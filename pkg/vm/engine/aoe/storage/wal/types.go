package wal

import "io"

type Payload interface {
	Marshal() ([]byte, error)
}

type Wal interface {
	io.Closer
	Log(Payload) (*Entry, error)
	Checkpoint(interface{})
	String() string
}

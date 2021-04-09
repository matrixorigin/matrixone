package client

import (
	"matrixone/pkg/vm/engine/logEngine/meta"
	"net/rpc"
	"time"
)

type Client interface {
	Close() error

	Unlock(string) error
	Lock(string, time.Time) error

	Metadata(string) (meta.Metadata, error)
	UpdateMetadata(string, meta.Metadata) error

	Metadatas(string) ([]meta.Metadata, error)
}

type client struct {
	cli *rpc.Client
}

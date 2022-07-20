package memEngine

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/kv"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/meta"
)

// MemEngine standalone memory engine
type MemEngine struct {
	db *kv.KV
	n  engine.Node
}

type database struct {
	db *kv.KV
	n  engine.Node
}

type relation struct {
	id string
	db *kv.KV
	n  engine.Node
	md meta.Metadata
}

type reader struct {
	zs    []int64
	db    *kv.KV
	segs  []string
	cds   []*bytes.Buffer
	dds   []*bytes.Buffer
	attrs map[string]engine.Attribute
}

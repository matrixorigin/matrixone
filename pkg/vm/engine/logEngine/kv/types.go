package kv

import (
	"matrixone/pkg/vm/engine/logEngine/kv/cache"
	"matrixone/pkg/vm/engine/logEngine/kv/s3"
)

type KV struct {
	name string
	re   *s3.KV       // remote s3 engine
	kc   *cache.Cache // cache
}

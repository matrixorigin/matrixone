package kv

import (
	"matrixbase/pkg/vm/engine/logEngine/kv/cache"
	"matrixbase/pkg/vm/engine/logEngine/kv/s3"
)

type KV struct {
	name string
	re   *s3.KV       // remote s3 engine
	kc   *cache.Cache // cache
}

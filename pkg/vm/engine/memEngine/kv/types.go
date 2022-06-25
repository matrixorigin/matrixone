package kv

import (
	"errors"
	"sync"
)

var (
	ErrNotExist = errors.New("not exist")
)

type KV struct {
	sync.Mutex
	mp map[string][]byte
}

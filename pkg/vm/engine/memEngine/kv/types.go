package kv

import (
	"errors"
	"sync"
)

var (
	NotExist = errors.New("not exist")
)

type KV struct {
	mp map[string][]byte
	rwlock sync.RWMutex
}

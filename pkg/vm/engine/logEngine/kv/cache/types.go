package cache

import (
	"container/list"
	"matrixone/pkg/logger"
	"sync"
)

/*
type Cache interface {
	Del(string) error
	Set(string, int64) error
	Add(string, []byte) error
	Get(string) ([]byte, *aio.AIO, aio.RequestId, bool, error)
}
*/

type entry struct {
	s int64  // size
	k string // file name
}

type Cache struct {
	sync.RWMutex
	size  int64 // current size
	limit int64
	path  string
	lt    *list.List
	log   logger.Log
	mp    map[string]*list.Element
}

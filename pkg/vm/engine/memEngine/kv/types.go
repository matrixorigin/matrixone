package kv

import "errors"

var (
	ErrNotExist = errors.New("not exist")
)

type KV struct {
	mp map[string][]byte
}
